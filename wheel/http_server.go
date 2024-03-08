package wheel

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strconv"
	"time"
)

type httpHandler struct {
	w      *Wheel
	router http.Handler
}

func (hs *httpHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	hs.router.ServeHTTP(rw, r)
}

func (hs *httpHandler) addTaskHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	err := r.ParseForm()
	if err != nil {
		return
	}
	TaskKey := r.PostFormValue("key")
	strExecTime := r.PostFormValue("exec_time")
	FuncName := r.PostFormValue("func_name")
	FuncParams := r.PostFormValue("func_params")

	intExecTime, err := strconv.ParseInt(strExecTime, 10, 64)
	ExecTime := time.Unix(intExecTime, 0).Local()

	actualExecTime, err := hs.w.AddTask(TaskKey, FuncName, FuncParams, ExecTime)
	if err != nil {
		fmt.Fprintf(rw, "%v", err)
		return
	}
	fmt.Fprintf(rw, "%v", actualExecTime)
}

func (hs *httpHandler) removeTaskHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	err := r.ParseForm()
	if err != nil {
		return
	}
	TaskKey := r.PostFormValue("key")
	strExecTime := r.PostFormValue("exec_time")

	intExecTime, err := strconv.ParseInt(strExecTime, 10, 64)
	ExecTime := time.Unix(intExecTime, 0).Local()

	actualExecTime, err := hs.w.RemoveTask(TaskKey, ExecTime)
	if err != nil {
		fmt.Fprintf(rw, "%v", err)
		return
	}
	fmt.Fprintf(rw, "%v", actualExecTime)
}

func newHttpHandler(a *Wheel) *httpHandler {
	router := httprouter.New()

	hh := &httpHandler{
		w:      a,
		router: router,
	}
	router.POST("/add_task", hh.addTaskHandler)
	router.POST("/remove_task", hh.removeTaskHandler)
	return hh
}

func startHttpServer(w *Wheel) {
	httpServer := newHttpHandler(w)
	server := &http.Server{
		Handler: httpServer,
	}
	err := server.Serve(w.HttpListener)
	if err != nil {
		return
	}
}
