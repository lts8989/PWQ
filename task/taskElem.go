package task

import "time"

const (
	PUT uint8 = iota
	DEL
)

type Elem struct {
	TaskKey string
	//ExecTime   time.Time
	FuncName   string
	FuncParams string
	Mark       uint8
}

type InputTask struct {
	TaskKey    string
	ExecTime   time.Time
	FuncName   string
	FuncParams string
}
