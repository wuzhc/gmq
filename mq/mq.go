package mq

import (
	"context"
	"fmt"
	"gmq/logs"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"gmq/utils"

	"gopkg.in/ini.v1"
)

var (
	log *logs.Dispatcher
	gmq *Gmq
)

type Gmq struct {
	running    int
	closed     chan struct{}
	cfg        *ini.File
	wg         sync.WaitGroup
	dispatcher *Dispatcher
	webMonitor *WebMonitor
	serv       IServer
}

func NewGmq(cfg string) *Gmq {
	gmq = &Gmq{
		closed: make(chan struct{}),
	}

	if res, err := utils.PathExists(cfg); !res {
		if err != nil {
			fmt.Printf("%s is not exists,errors:%s \n", cfg, err.Error())
		} else {
			fmt.Printf("%s is not exists \n", cfg)
		}
		os.Exit(1)
	}

	var err error
	gmq.cfg, err = ini.Load(cfg)
	if err != nil {
		fmt.Printf("Fail to read file: %v \n", err)
		os.Exit(1)
	}

	gmq.dispatcher = NewDispatcher()
	gmq.webMonitor = NewWebMonitor()
	gmq.serv = NewServ()

	return gmq
}

func (gmq *Gmq) Run() {
	if atomic.LoadInt32(gmq.running) == 1 {
		fmt.Println("running.")
		return
	}

	ctx, cannel := context.WithCancel(context.Background())
	defer cannel()

	atomic.StoreInt32(gmq.running, 1)
	gmq.initLogger()
	gmq.initRedisPool()
	gmq.initSignalHandler(cannel)
	gmq.welcome()

	go gmq.dispatcher.Run(ctx) // job调度服务
	go gmq.webMonitor.Run(ctx) // web监控服务
	go gmq.serv.Run(ctx)       // rpc服务或http服务

	<-gmq.closed
	fmt.Println("Closed.")
}

func (gmq *Gmq) welcome() {
	fmt.Println("Welcome to use gmq, enjoy it")
}

func (gmq *Gmq) initLogger() {
	log = logs.NewDispatcher()
	logConf := gmq.cfg.Section("log")
	filename := logConf.Key("filename").String()
	level, _ := logConf.Key("level").Int()
	rotate, _ := logConf.Key("rotate").Bool()
	max_size, _ := logConf.Key("max_size").Int()
	target_type := logConf.Key("target_type").String()

	targets := strings.Split(target_type, ",")
	for _, t := range targets {
		if t == logs.TARGET_FILE {
			conf := fmt.Sprintf(`{"filename":"%s","level":%d,"max_size":%d,"rotate":%v}`, filename, level, max_size, rotate)
			log.SetTarget(logs.TARGET_FILE, conf)
		} else if t == logs.TARGET_CONSOLE {
			log.SetTarget(logs.TARGET_CONSOLE, "")
		} else {
			fmt.Println("Only support file or console handler")
			os.Exit(1)
		}
	}
}

func (gmq *Gmq) initRedisPool() {
	Redis.InitPool()
}

func (gmq *Gmq) initSignalHandler(cannel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-sigs
		cannel()                 // 通知各个服务退出
		gmq.wg.Wait()            // 等待各个服务退出
		gmq.closed <- struct{}{} // 关闭整个服务
		Redis.Pool.Close()       // 关闭redis连接池
		gmq.running = 0
	}()
}
