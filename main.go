package main

import (
	"context"
	"github.com/judwhite/go-svc"
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"ltstimewheel/wheel"

	"github.com/pkg/errors"
	"os"
	"path"
	"sync"
	"syscall"
	"time"
)

type program struct {
	once  sync.Once
	wheel *wheel.Wheel
}

func (p *program) Init(env svc.Environment) error {
	ConfigLocalFileSystemLogger("log", "lg", 0, time.Second*10)
	opts := wheel.NewOptions()
	flagSet := wheel.WheelFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	log.Info("init wheel")

	var err error
	if opts.IntervalSec == 0 {
		opts.IntervalSec = 1
	} else if opts.IntervalSec > 60 {
		opts.IntervalSec = 60
	}
	if 60%opts.IntervalSec != 0 {
		err = errors.New("intervalSec 必须要被60整除")
		return err
	}

	w, err := wheel.NewWheel(opts)
	if err != nil {
		return err
	}
	p.wheel = w
	return nil

}

func (p *program) Start() error {
	go p.wheel.Run()
	return nil
}
func (p *program) Stop() error {
	p.once.Do(func() {
		p.wheel.Stop()
	})
	log.Info("app Stop")
	return nil
}
func (p *program) Handle(s os.Signal) error {
	return svc.ErrStop
}
func (p *program) Context() context.Context {
	return p.wheel.Context()
}
func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, os.Interrupt, syscall.SIGTERM); err != nil {
		log.Error(err)
	}
}
func ConfigLocalFileSystemLogger(logPath string, logFileName string, maxAge time.Duration, rotationTime time.Duration) {
	baseLogPaht := path.Join(logPath, logFileName)
	writer, err := rotatelogs.New(
		baseLogPaht+"%Y%m%d_%H%M.log",             // 分割后的文件名称
		rotatelogs.WithLinkName(baseLogPaht),      // 生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(maxAge),             // 设置最大保存时间(7天)
		rotatelogs.WithRotationCount(365),         // 最多存365个文件
		rotatelogs.WithRotationTime(rotationTime), // 设置日志切割时间间隔(1天)
	)
	if err != nil {
		log.Errorf("config local file system logger error. %+v", errors.WithStack(err))
	}
	lfHook := lfshook.NewHook(lfshook.WriterMap{
		log.DebugLevel: writer, // 为不同级别设置不同的输出目的
		log.InfoLevel:  writer,
		log.WarnLevel:  writer,
		log.ErrorLevel: writer,
		log.FatalLevel: writer,
		log.PanicLevel: writer,
	}, &log.TextFormatter{})
	log.AddHook(lfHook)
}
