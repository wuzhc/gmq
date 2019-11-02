package gnode

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/wuzhc/gmq/configs"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"

	"gopkg.in/ini.v1"
)

const DATA_DIR = "data"

type Gnode struct {
	version  string
	running  int32
	exitChan chan struct{}
	ctx      *Context
	wg       utils.WaitGroupWrapper
	cfg      *configs.GnodeConfig
}

func New() *Gnode {
	return &Gnode{
		version:  "1.1",
		exitChan: make(chan struct{}),
	}
}

// 启动应用
func (gn *Gnode) Run() {
	defer gn.wg.Wait()

	if atomic.LoadInt32(&gn.running) == 1 {
		log.Fatalln("gnode is running.")
	}
	if !atomic.CompareAndSwapInt32(&gn.running, 0, 1) {
		log.Fatalln("gnode start failed.")
	}

	if gn.cfg == nil {
		gn.SetDefaultConfig()
	}
	if err := gn.cfg.Validate(); err != nil {
		log.Fatalln(err)
	}

	// 创建数据(消息和日志)文件存储位置
	isExist, err := utils.PathExists(gn.cfg.DataSavePath)
	if err != nil {
		log.Fatalln(err)
	}
	if !isExist {
		if err := os.MkdirAll(gn.cfg.DataSavePath, os.ModePerm); err != nil {
			log.Fatalln(err)
		}
	}

	ctx := &Context{
		Gnode:  gn,
		Conf:   gn.cfg,
		Logger: gn.initLogger(),
	}

	gn.ctx = ctx
	gn.wg.Wrap(NewDispatcher(ctx).Run)
	gn.wg.Wrap(NewHttpServ(ctx).Run)
	gn.wg.Wrap(NewTcpServ(ctx).Run)

	gn.installSignalHandler()
	ctx.Logger.Info("gnode is running.")

	if err := gn.register(); err != nil {
		log.Println("register failed, ", err)
	}
}

// 退出应用
func (gn *Gnode) Exit() {
	if err := gn.unregister(); err != nil {
		log.Fatalln("unregister failed")
	}
	close(gn.exitChan)
}

// 设置配置选项
func (gn *Gnode) SetConfig(cfgFile string) {
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

	cfg := new(configs.GnodeConfig)

	// node
	nodeId, _ := c.Section("node").Key("id").Int()
	nodeWeight, _ := c.Section("node").Key("weight").Int()
	msgTTR, _ := c.Section("node").Key("msgTTR").Int()
	msgMaxRetry, _ := c.Section("node").Key("msgMaxRetry").Int()
	reportTcpAddr := c.Section("node").Key("reportTcpaddr").String()
	reportHttpAddr := c.Section("node").Key("reportHttpaddr").String()
	dataSavePath := c.Section("node").Key("dataSavePath").String()

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

	// tcp server config
	tcpServAddr := c.Section("tcp_server").Key("addr").String()
	cfg.TcpServCertFile = c.Section("tcp_server").Key("certFile").String()
	cfg.TcpServKeyFile = c.Section("tcp_server").Key("keyFile").String()
	cfg.TcpServEnableTls, _ = c.Section("tcp_server").Key("enableTls").Bool()

	// register config
	registerAddr := c.Section("gregister").Key("addr").String()

	// parse flag
	flag.StringVar(&cfg.HttpServAddr, "http_addr", httpServAddr, "http address")
	flag.StringVar(&cfg.ReportHttpAddr, "report_http_addr", reportHttpAddr, "report http address")
	flag.StringVar(&cfg.TcpServAddr, "tcp_addr", tcpServAddr, "tcp address")
	flag.StringVar(&cfg.ReportTcpAddr, "report_tcp_addr", reportTcpAddr, "report tcp address")
	flag.StringVar(&cfg.GregisterAddr, "register_addr", registerAddr, "register address")
	flag.IntVar(&cfg.NodeId, "node_id", nodeId, "node unique id")
	flag.IntVar(&cfg.NodeWeight, "node_weight", nodeWeight, "node weight")
	flag.IntVar(&cfg.MsgTTR, "msg_ttr", msgTTR, "msg ttr")
	flag.IntVar(&cfg.MsgMaxRetry, "msg_max_retry", msgMaxRetry, "msg max retry")
	flag.StringVar(&cfg.DataSavePath, "data_save_path", dataSavePath, "data save path")
	flag.Parse()

	gn.cfg = cfg
	gn.cfg.SetDefault()
}

// 设置默认配置选项
func (gn *Gnode) SetDefaultConfig() {
	cfg := new(configs.GnodeConfig)

	flag.StringVar(&cfg.TcpServAddr, "tcp_addr", "", "tcp address")
	flag.StringVar(&cfg.GregisterAddr, "register_addr", "", "register address")
	flag.StringVar(&cfg.HttpServAddr, "http_addr", "", "http address")
	flag.StringVar(&cfg.ReportTcpAddr, "report_tcp_addr", "", "report tcp address")
	flag.StringVar(&cfg.ReportHttpAddr, "report_http_addr", "", "report http address")
	flag.IntVar(&cfg.NodeId, "node_id", 1, "node unique id")
	flag.IntVar(&cfg.NodeWeight, "node_weight", 1, "node weight")
	flag.IntVar(&cfg.MsgTTR, "msg_ttr", 60, "msg ttr")
	flag.IntVar(&cfg.MsgMaxRetry, "msg_max_retry", 5, "msg max retry")
	flag.StringVar(&cfg.DataSavePath, "data_save_path", "", "data save path")
	flag.Parse()

	gn.cfg = cfg
	gn.cfg.SetDefault()
}

// 安装退出信号处理器
func (gn *Gnode) installSignalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		<-sigs
		gn.Exit()
	}()
}

// 初始化日志组件
func (gn *Gnode) initLogger() *logs.Dispatcher {
	logger := logs.NewDispatcher(gn.cfg.LogLevel)
	targets := strings.Split(gn.cfg.LogTargetType, ",")
	for _, t := range targets {
		if t == logs.TARGET_FILE {
			conf := fmt.Sprintf(`{"filename":"%s","max_size":%d,"rotate":%v}`, gn.cfg.DataSavePath+"/"+gn.cfg.LogFilename, gn.cfg.LogMaxSize, gn.cfg.LogRotate)
			logger.SetTarget(logs.TARGET_FILE, conf)
		} else if t == logs.TARGET_CONSOLE {
			logger.SetTarget(logs.TARGET_CONSOLE, "")
		} else {
			log.Fatalln("Only support file or console handler")
		}
	}
	return logger
}

type rs struct {
	Code int         `json:"code"`
	Data interface{} `json:"data"`
	Msg  string      `json:"msg"`
}

func (gn *Gnode) register() error {
	hosts := strings.Split(gn.cfg.GregisterAddr, ",")
	for _, host := range hosts {
		url := fmt.Sprintf("%s/register?tcp_addr=%s&http_addr=%s&weight=%d&node_id=%d", host, gn.cfg.ReportTcpAddr, gn.cfg.ReportHttpAddr, gn.cfg.NodeWeight, gn.cfg.NodeId)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		res, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var r rs
		if err := json.Unmarshal(res, &r); err != nil {
			log.Fatalln(err)
		}
		if r.Code == 1 {
			log.Fatalln(r.Msg)
		}
	}

	return nil
}

func (gn *Gnode) unregister() error {
	ts := strings.Split(gn.cfg.GregisterAddr, ",")
	for _, t := range ts {
		url := t + "/unregister?tcp_addr=" + gn.cfg.TcpServAddr
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		res, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var r rs
		if err := json.Unmarshal(res, &r); err != nil {
			log.Fatalln(err)
		}
		if r.Code == 1 {
			log.Fatalln(r.Msg)
		}
	}

	return nil
}
