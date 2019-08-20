package gregister

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wuzhc/gmq/configs"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"

	"gopkg.in/ini.v1"
)

type node struct {
	HttpAddr string `json:"http_addr"`
	TcpAddr  string `json:"tcp_addr"`
	JoinTime string `json:"join_time"`
	Weight   int    `json:"weight"`
}

type Gregister struct {
	running int32
	cfgFile string
	wg      utils.WaitGroupWrapper
	closed  chan struct{}
	mux     sync.RWMutex
	nodes   []node
}

func New(cfgFile string) *Gregister {
	return &Gregister{
		cfgFile: cfgFile,
	}
}

func (gr *Gregister) Run() {
	if atomic.LoadInt32(&gr.running) == 1 {
		log.Fatalln("gregister is running")
	}
	if !atomic.CompareAndSwapInt32(&gr.running, 0, 1) {
		log.Fatalln("gregister start failed")
	}

	cfg := initConfig(gr.cfgFile)
	logger := initLogger(cfg)

	ctx := &Context{
		Gregister: gr,
		Conf:      cfg,
		Logger:    logger,
	}

	gr.wg.Wrap(func() {
		NewHttpServ(ctx).Run()
	})

	log.Println("run...")
	<-gr.closed
}

func initConfig(cfgFile string) *configs.GregisterConfig {
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
	return cfg
}

func initLogger(cfg *configs.GregisterConfig) *logs.Dispatcher {
	logger := logs.NewDispatcher()
	targets := strings.Split(cfg.LogTargetType, ",")
	for _, t := range targets {
		if t == logs.TARGET_FILE {
			conf := fmt.Sprintf(`{"filename":"%s","level":%d,"max_size":%d,"rotate":%v}`, cfg.LogFilename, cfg.LogLevel, cfg.LogMaxSize, cfg.LogRotate)
			logger.SetTarget(logs.TARGET_FILE, conf)
		} else if t == logs.TARGET_CONSOLE {
			logger.SetTarget(logs.TARGET_CONSOLE, "")
		} else {
			log.Fatalln("Only support file or console handler")
		}
	}
	return logger
}
