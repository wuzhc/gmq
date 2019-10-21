package gregister

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/wuzhc/gmq/configs"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"

	"gopkg.in/ini.v1"
)

type node struct {
	Id       int64  `json:"id"`
	HttpAddr string `json:"http_addr"`
	TcpAddr  string `json:"tcp_addr"`
	JoinTime string `json:"join_time"`
	Weight   int    `json:"weight"`
}

type Gregister struct {
	running  int32
	cfg      *configs.GregisterConfig
	wg       utils.WaitGroupWrapper
	mux      sync.RWMutex
	nodes    []node
	exitChan chan struct{}
}

func New() *Gregister {
	return &Gregister{
		exitChan: make(chan struct{}),
	}
}

// 启动应用
func (gr *Gregister) Run() {
	if atomic.LoadInt32(&gr.running) == 1 {
		log.Fatalln("gregister is running")
	}
	if !atomic.CompareAndSwapInt32(&gr.running, 0, 1) {
		log.Fatalln("gregister start failed")
	}
	if gr.cfg == nil {
		gr.SetDefaultConfig()
	}

	ctx := &Context{
		Gregister: gr,
		Conf:      gr.cfg,
		Logger:    gr.initLogger(),
	}

	gr.wg.Wrap(NewHttpServ(ctx).Run)
	gr.wg.Wrap(gr.heartbeat)

	<-gr.exitChan
}

// 退出应用
func (gr *Gregister) Exit() {
	close(gr.exitChan)
	gr.wg.Wait()
}

// 设置配置
func (gr *Gregister) SetConfig(cfgFile string) {
	if res, err := utils.PathExists(cfgFile); !res {
		if err != nil {
			log.Fatalf("%s is not exists,errors:%s \n", cfgFile, err.Error())
		} else {
			log.Fatalf("%s is not exists \n", cfgFile)
		}
	}

	c, err := ini.Load(cfgFile)
	if err != nil {
		log.Fatalf("Fail to read file: %v \n", err)
	}

	cfg := new(configs.GregisterConfig)

	// log config
	cfg.LogFilename = c.Section("log").Key("filename").String()
	cfg.LogLevel, _ = c.Section("log").Key("level").Int()
	cfg.LogRotate, _ = c.Section("log").Key("rotate").Bool()
	cfg.LogMaxSize, _ = c.Section("log").Key("max_size").Int()
	cfg.LogTargetType = c.Section("log").Key("target_type").String()

	// http server config
	httpServAddr := c.Section("http_server").Key("addr").String()
	cfg.HttpServCertFile = c.Section("http_server").Key("certFile").String()
	cfg.HttpServKeyFile = c.Section("http_server").Key("keyFile").String()
	cfg.HttpServEnableTls, _ = c.Section("http_server").Key("enableTls").Bool()

	// parse flag
	flag.StringVar(&cfg.HttpServAddr, "http_addr", httpServAddr, "http address")
	flag.Parse()

	cfg.SetDefault()
	gr.cfg = cfg
}

// 设置默认配置
func (gr *Gregister) SetDefaultConfig() {
	cfg := new(configs.GregisterConfig)

	flag.StringVar(&cfg.HttpServAddr, "http_addr", "", "http address")
	flag.Parse()

	gr.cfg = cfg
	gr.cfg.SetDefault()
}

// 初始化日志
func (gr *Gregister) initLogger() *logs.Dispatcher {
	logger := logs.NewDispatcher()
	targets := strings.Split(gr.cfg.LogTargetType, ",")
	for _, t := range targets {
		if t == logs.TARGET_FILE {
			conf := fmt.Sprintf(`{"filename":"%s","level":%d,"max_size":%d,"rotate":%v}`, gr.cfg.LogFilename, gr.cfg.LogLevel, gr.cfg.LogMaxSize, gr.cfg.LogRotate)
			logger.SetTarget(logs.TARGET_FILE, conf)
		} else if t == logs.TARGET_CONSOLE {
			logger.SetTarget(logs.TARGET_CONSOLE, "")
		} else {
			log.Fatalln("Only support file or console handler")
		}
	}
	return logger
}

// 安装信号处理器
func (gr *Gregister) installSignalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		<-sigs
		gr.Exit()
	}()
}

// 心跳
func (gr *Gregister) heartbeat() {
	ticker := time.NewTicker(time.Duration(gr.cfg.Heartbeat) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-gr.exitChan:
			return
		case <-ticker.C:
			if len(gr.nodes) == 0 {
				continue
			}

			for i, n := range gr.nodes {
				url := fmt.Sprintf("http://%s/ping", n.HttpAddr)
				resp, err := http.Get(url)
				if err != nil {
					fmt.Println(err)
					gr.removeNode(i)
					continue
				}
				res, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					fmt.Println(err)
					gr.removeNode(i)
					continue
				}
				if !bytes.Equal(res, []byte{'O', 'K'}) {
					fmt.Println(res, string(res), []byte{'O', 'K'})
					gr.removeNode(i)
				}
			}
		}
	}
}

// 移除节点
func (gr *Gregister) removeNode(i int) {
	nlen := len(gr.nodes)
	if nlen == 0 || i >= nlen || i < 0 {
		return
	}

	if i == 0 {
		if nlen == 1 {
			gr.nodes = gr.nodes[0:0]
		} else {
			gr.nodes = gr.nodes[1:]
		}
	} else {
		gr.nodes = append(gr.nodes[0:i], gr.nodes[i+1:]...)
	}
}
