package slot

import (
	"fmt"
	"log"
	"ltstimewheel/task"
	"os"
	"sync"
)

const DirPath = "/tmp/ltstimewheel"
const entryHeaderSize = 14

// File 插槽文件对象
type File struct {
	file          *os.File
	offset        int64
	headerBufPool *sync.Pool
	mu            sync.RWMutex
}

// distroy 删除当前槽的文件
func (sf *File) distroy() {
	stat, _ := sf.file.Stat()
	sf.FileClose()
	fullPath := fmt.Sprintf("%s/%s", DirPath, stat.Name())
	log.Printf("distory:%s", fullPath)
	err := os.Remove(fullPath)
	if err != nil {
		log.Printf("Remove err:%s", err.Error())
	}
}

// Write 将Entry对象写到文件末尾
func (sf *File) Write(e *Entry) error {
	enc, err := e.Encode()
	_, err = sf.file.WriteAt(enc, sf.offset)
	if err != nil {
		log.Printf("写入文件失败")
		return err
	}
	sf.offset += int64(e.GetSize())
	return nil
}

// Read 从offset位置读取一个Entry
func (sf *File) Read() (e *Entry, err error) {
	buf := sf.headerBufPool.Get().([]byte)
	defer sf.headerBufPool.Put(buf)
	//sf.offset -= entryHeaderSize
	locOffset := sf.offset
	locOffset -= entryHeaderSize
	if _, err = sf.file.ReadAt(buf, locOffset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}
	locOffset -= int64(e.GetSize() - entryHeaderSize)

	if e.KeySize > 0 {
		keyBuf := make([]byte, e.KeySize)
		if _, err = sf.file.ReadAt(keyBuf, locOffset); err != nil {
			return
		}
		e.Key = keyBuf
	}
	locOffset += int64(e.KeySize)

	if e.FuncNameSize > 0 {
		funcNameBuf := make([]byte, e.FuncNameSize)
		if _, err = sf.file.ReadAt(funcNameBuf, locOffset); err != nil {
			return
		}
		e.FuncName = funcNameBuf
	}
	locOffset += int64(e.FuncNameSize)

	if e.FuncParamsSize > 0 {
		funcParamsBuf := make([]byte, e.FuncParamsSize)
		if _, err = sf.file.ReadAt(funcParamsBuf, locOffset); err != nil {
			return
		}
		e.FuncParams = funcParamsBuf
	}
	sf.offset = sf.offset - int64(e.GetSize())
	return
}

// getSlotFileFullPath 获取插槽文件全路径
func getSlotFileFullPath(indexName string) string {
	return fmt.Sprintf("%s/%s_slot.data", DirPath, indexName)
}

// existSlotFile 判断插槽文件是否存在
func existSlotFile(indexName string) bool {
	fullPath := getSlotFileFullPath(indexName)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		return false
	}
	return true
}

// NewSlotFile 初始化slotFile对象，如果不存在槽文件，就创建
func NewSlotFile(indexName string) (sf *File, err error) {
	fullPath := getSlotFileFullPath(indexName)
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("打开文件失败，%s", fullPath)
		return
	}

	stat, err := os.Stat(fullPath)
	if err != nil {
		log.Fatalf("获取文件信息失败，%s", fullPath)
		return
	}
	pool := &sync.Pool{New: func() interface{} { return make([]byte, entryHeaderSize) }}
	return &File{
		file:          file,
		offset:        stat.Size(),
		headerBufPool: pool,
	}, nil

}

func (sf *File) FileClose() {
	sf.file.Close()
}

// WriteTaskToFile 写入文件
func (sf *File) WriteTaskToFile(wg *sync.WaitGroup, in chan task.Elem) {
	log.Printf("WriteTaskToFile-begin")
	sf.mu.Lock()
	defer sf.mu.Unlock()
	defer sf.FileClose()
	for taskEl := range in {
		log.Printf("WriteTaskToFile,taskKey:%s", taskEl.TaskKey)
		entry := newEntry([]byte(taskEl.TaskKey), []byte(taskEl.FuncName), []byte(taskEl.FuncParams), taskEl.Mark)
		wg.Done()
		err := sf.Write(entry)
		if err != nil {
			//return err
		}

	}
}

// LoadTasks 获取当前时间下插槽所有任务，写入通道，并销毁插槽文件
func LoadTasks(indexName string, out chan task.Elem) {
	if !existSlotFile(indexName) {
		log.Printf("当前时间插槽没有任务，%s", indexName)
		return
	}

	sf, err := NewSlotFile(indexName)
	if err != nil {
		log.Printf("获取所有任务失败,%v", err)
		return
	}

	defer func() {
		log.Printf("LoadTasks defer")
		if r := recover(); r != nil {
			log.Println(r)
		}
	}()
	defer sf.distroy()

	for {
		if sf.offset <= 0 {
			break
		}

		e, err := sf.Read()
		if err != nil {
			return
		}
		_elem := task.Elem{
			TaskKey:    string(e.Key),
			FuncName:   string(e.FuncName),
			FuncParams: string(e.FuncParams),
			Mark:       e.Mark,
		}
		out <- _elem
	}
}
