package mq

import (
	"fmt"
	"gmq/logs"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	log *logs.Dispatcher
	gmq *Gmq
)

type Gmq struct {
	running int
	closed  chan struct{}
	notify  chan struct{}
	wg      sync.WaitGroup
}

func NewGmq() *Gmq {
	gmq = &Gmq{
		closed: make(chan struct{}),
		notify: make(chan struct{}),
	}
	return gmq
}

func (gmq *Gmq) Run() {
	if gmq.running == 1 {
		fmt.Println("running.")
		return
	} else {
		gmq.running = 1
	}

	log = logs.NewDispatcher()
	log.SetTarget(logs.TARGET_FILE, `{"filename":"xxx.log","level":2,"max_size":50000000,"rotate":true}`)
	log.SetTarget(logs.TARGET_CONSOLE, "")

	// 无法捕获SIGKILL信号,不要使用kill -9 pid命令来杀死进程
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-sigs
		gmq.running = 0
		close(gmq.notify)        // 通知其他goroutine退出
		gmq.wg.Wait()            // 等待goroutine安全退出
		gmq.closed <- struct{}{} // 关闭整个服务
	}()

	go Dper.Run() // job调度服务
	go Wmor.Run() // web监控服务
	go Serv.Run() // rpc服务或http服务

	<-gmq.closed
	fmt.Println("Closed.")
}
