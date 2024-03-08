package wheel

import (
	"container/list"
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"ltstimewheel/slot"
	"ltstimewheel/task"
	"net"
	"os"
	"sync"
	"time"
)

const indexNameFormat = "20060102150405"
const (
	BLOOM_FILTER uint8 = iota
	MAP_FILTER
)

type Wheel struct {
	sync.Once
	intervalSec     uint8
	filterType      uint8
	persistenceCond persistenceCond
	ticker          *time.Ticker
	stopCh          chan struct{}
	//putIndex        map[string]MemIndex
	//delIndex        map[string]MemIndex
	addTaskCh     chan *task.InputTask
	removeTaskCh  chan *task.InputTask
	ctx           context.Context
	ctxCancel     context.CancelFunc
	HttpListener  net.Listener
	httpsListener net.Listener
	memIndex      map[string]*list.List
}
type MemIndex struct {
	createTime   time.Time
	size         uint32
	taskElemList map[string]task.Elem
}

type persistenceCond struct {
	remainSec uint16 //任务执行时间与当前时间的差（秒）
	taskSize  uint32 //任务数量
}

func NewOptions() *Options {
	return &Options{
		IntervalSec: 3,
		FilterType:  MAP_FILTER,
		RemainSec:   2,
		TaskSize:    4,
		HTTPAddress: "0.0.0.0:7891",
	}
}

func NewWheel(opts *Options) (*Wheel, error) {
	var err error

	if _, err = os.Stat(slot.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(slot.DirPath, os.ModePerm); err != nil {
			err = errors.New("数据目录创建失败，" + slot.DirPath)
			return nil, err
		}
	}
	_persistenceCond := persistenceCond{
		remainSec: opts.RemainSec,
		taskSize:  opts.TaskSize,
	}

	t := &Wheel{
		intervalSec:     opts.IntervalSec,
		filterType:      opts.FilterType,
		persistenceCond: _persistenceCond,
		ticker:          time.NewTicker(time.Second),
		stopCh:          make(chan struct{}),
		memIndex:        make(map[string]*list.List),
		addTaskCh:       make(chan *task.InputTask),
		removeTaskCh:    make(chan *task.InputTask),
	}
	t.ctx, t.ctxCancel = context.WithCancel(context.Background())

	t.HttpListener, err = net.Listen("tcp", opts.HTTPAddress)

	return t, err
}

func (w *Wheel) Run() {
	defer func() {
		if err := recover(); err != nil {

		}
	}()

	go startHttpServer(w)

	for {
		select {
		case <-w.stopCh:
			return
		case <-w.ticker.C:
			log.Info("tick")
			w.tick()
		case t := <-w.addTaskCh:
			w.addTask(t)
		case t := <-w.removeTaskCh:
			w.removeTask(t)
		}
	}

}

func (w *Wheel) Stop() {
	w.Do(func() {
		w.ticker.Stop()
		close(w.stopCh)
		w.persistence(false)
		w.ctxCancel()
	})
}

// persistence 将内存中的数据持久化到硬盘
// force 强制检查写入硬盘文件的条件
func (w *Wheel) persistence(force bool) {
	tickTime := time.Now()
	log.Printf("persistence begin,force:%t,memIndexCount:%d", force, len(w.memIndex))
	for indexName, taskList := range w.memIndex {
		if force { //强制检查，符合条件就将缓存内容写入文件
			execTime, _ := time.Parse(indexNameFormat, indexName)
			if tickTime.After(execTime.Add(-time.Duration(w.persistenceCond.remainSec) * time.Second)) { //插槽的执行时间与当前小于阈值，则不向文件写入
				log.Printf("插槽的执行时间与当前小于%ds，则不向文件写入,indexName:%s", w.persistenceCond.remainSec, indexName)
				continue
			}
			if taskList.Len() < int(w.persistenceCond.taskSize) { //插槽内容小于阈值，则不向文件写入
				log.Printf("插槽内容小于%d，则不向文件写入,indexSize:%d,indexName:%s", w.persistenceCond.taskSize, taskList.Len(), indexName)
				continue
			}
		}

		log.Printf("缓存写入文件，indexName:%s,taskCount:%d", indexName, w.memIndex[indexName].Len())
		sf, _ := slot.NewSlotFile(indexName)
		var wg sync.WaitGroup
		inToFile := make(chan task.Elem)
		filterTask := make(chan task.Elem)
		go sf.WriteTaskToFile(&wg, inToFile)
		go w.FilterDuplicateTasks(indexName, filterTask, inToFile)
		for e := taskList.Back(); e != nil; {
			wg.Add(1)
			taskEl, _ := e.Value.(task.Elem)
			log.Printf("filterTask-before,%s", taskEl.TaskKey)
			filterTask <- taskEl

			next := e.Next()
			taskList.Remove(e)
			e = next
		}
		wg.Wait()
		close(filterTask)
		close(inToFile)

		//delete(w.memIndex, indexName)
	}
	log.Printf("persistence end")
}

func (w *Wheel) FilterDuplicateTasks(indexName string, in chan task.Elem, out chan task.Elem) {
	log.Printf("FilterDuplicateTasks-begin,type:%d,indexName:%s", w.filterType, indexName)
	switch w.filterType {
	case BLOOM_FILTER:
		bloom := NewBloomFilter(1 << 16)
		bloom.RegisterHash(hash1, hash2)
		for taskEl := range in {
			taskKey := []byte(taskEl.TaskKey)
			if !bloom.Exist(taskKey) {
				bloom.Add(taskKey)
				out <- taskEl
			}
		}
	case MAP_FILTER:
		allTasks := make(map[string]uint8)
		for taskEl := range in {
			log.Printf("FilterDuplicateTasks,taskKey:%s,indexName:%s", taskEl.TaskKey, indexName)

			if _, ok := allTasks[taskEl.TaskKey]; !ok {
				allTasks[taskEl.TaskKey] = taskEl.Mark
				out <- taskEl
			}
		}
	default:

	}

}

func (w *Wheel) tick() {
	now := time.Now()
	nowSec := now.Second()
	if uint8(nowSec)%w.intervalSec != 0 { //其他插槽的缓存写入文件
		w.persistence(true)
	} else { //执行当前插槽的任务
		indexName := now.Format(indexNameFormat)

		var wg sync.WaitGroup
		execTaskCh := make(chan task.Elem)
		filterTaskCh := make(chan task.Elem)
		go w.FilterDuplicateTasks(indexName, filterTaskCh, execTaskCh)
		go exech(&wg, execTaskCh)

		if taskList, ok := w.memIndex[indexName]; ok { //执行内存中的任务
			for e := taskList.Back(); e != nil; {
				taskEl, _ := e.Value.(*task.Elem)
				wg.Add(1)
				filterTaskCh <- *taskEl

				next := e.Next()
				taskList.Remove(e)
				e = next
			}
		}
		wg.Wait()
		delete(w.memIndex, indexName)

		slot.LoadTasks(indexName, filterTaskCh)
	}

}

// execTask 执行任务
func execTask(from string, t task.Elem) {
	log.Printf("%s_execTask:%s,%s,%s", from, t.TaskKey, t.FuncName, t.FuncParams)
}

func exech(wg *sync.WaitGroup, in chan task.Elem) {
	for taskEl := range in {
		if taskEl.Mark == task.PUT {
			execTask("", taskEl)
		}
		wg.Done()
	}
}

// getActualExecTime 获取真实的执行时间，例如：如果时间轮每隔10s执行一次，任务设定在12:06执行，那么真实的执行时间为12:10。
// 任务归入设定时间的下一个时间槽执行。
func (w *Wheel) getActualExecTime(execTime time.Time) time.Time {
	sec := execTime.Second()
	mod := sec % int(w.intervalSec)
	var actualExecTime time.Time
	if mod == 0 {
		actualExecTime = execTime
	} else {
		actualExecTime = execTime.Add(time.Second * time.Duration(int(w.intervalSec)-mod))
	}

	return actualExecTime
}

func (w *Wheel) AddTask(key, funcName, funcParams string, execTime time.Time) (actualExecTime time.Time, err error) {
	if time.Now().After(execTime) {
		log.Printf("添加任务，执行时间已经超时，execTime:%v", execTime)
		return actualExecTime, errors.New("执行时间已经超时")
	}

	actualExecTime = w.getActualExecTime(execTime)
	w.addTaskCh <- &task.InputTask{
		TaskKey:    key,
		ExecTime:   actualExecTime,
		FuncName:   funcName,
		FuncParams: funcParams,
	}
	return
}

func (w *Wheel) addTask(t *task.InputTask) {
	newElem := task.Elem{
		TaskKey:    t.TaskKey,
		FuncName:   t.FuncName,
		FuncParams: t.FuncParams,
		Mark:       task.PUT,
	}

	w.taskInsertMem(t.ExecTime, newElem)
}

func (w *Wheel) RemoveTask(key string, execTime time.Time) (actualExecTime time.Time, err error) {
	if time.Now().After(execTime) {
		log.Printf("删除任务执行时间已经超过，execTime:%v", execTime)
		return actualExecTime, errors.New("执行时间已经超时")
	}
	actualExecTime = w.getActualExecTime(execTime)
	w.removeTaskCh <- &task.InputTask{
		TaskKey:  key,
		ExecTime: actualExecTime,
	}
	return
}

// removeTask 删除任务
func (w *Wheel) removeTask(t *task.InputTask) {
	newElem := task.Elem{
		TaskKey: t.TaskKey,
		Mark:    task.DEL,
	}
	w.taskInsertMem(t.ExecTime, newElem)
}

func (w *Wheel) Context() context.Context {
	return w.ctx
}

func (w *Wheel) taskInsertMem(execTime time.Time, task task.Elem) {
	indexName := execTime.Format(indexNameFormat)
	memIndexList, ok := w.memIndex[indexName]
	if !ok {
		memIndexList = list.New()
		w.memIndex[indexName] = memIndexList
	}
	memIndexList.PushBack(task)
}
