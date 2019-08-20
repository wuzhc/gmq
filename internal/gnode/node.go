package gnode

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/wuzhc/gmq/configs"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"

	"gopkg.in/ini.v1"
)

type Gnode struct {
	running  int32
	confPath string
	ctx      context.Context
	wg       utils.WaitGroupWrapper
	closed   chan struct{}
	cfg      *configs.GnodeConfig
}

func New(confPath string) *Gnode {
	gn := &Gnode{
		confPath: confPath,
	}
	gn.ctx = context.Background()
	gn.closed = make(chan struct{})
	return gn
}

func (gn *Gnode) Run() {
	if atomic.LoadInt32(&gn.running) == 1 {
		log.Fatalln("gnode is running")
	}
	if !atomic.CompareAndSwapInt32(&gn.running, 0, 1) {
		log.Fatalln("start failed")
	}

	cfg := initConfig(gn.confPath)
	logger := initLogger(cfg)
	redisDB := initRedisPool(cfg)
	gn.cfg = cfg

	ctx := &Context{
		Gnode:   gn,
		Conf:    cfg,
		Logger:  logger,
		RedisDB: redisDB,
	}

	gn.wg.Wrap(NewDispatcher(ctx).Run)
	gn.wg.Wrap(NewHttpServ(ctx).Run)
	gn.wg.Wrap(NewTcpServ(ctx).Run)

	if err := register(cfg.GregisterAddr, cfg.TcpServAddr, cfg.HttpServAddr, cfg.TcpServWeight); err != nil {
		log.Fatalln(err)
	}

	ctx.Logger.Info("gnode is running.")
	<-gn.closed
}

func (gn *Gnode) Exit() {
	// todo 不能正常注销
	if err := unregister(gn.cfg.GregisterAddr, gn.cfg.TcpServAddr); err != nil {
		log.Fatalln("failed")
	}

	close(gn.closed)
	gn.wg.Wait()
}

func initConfig(cfgFile string) *configs.GnodeConfig {
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

	// log config
	cfg.LogFilename = c.Section("log").Key("filename").String()
	cfg.LogLevel, _ = c.Section("log").Key("level").Int()
	cfg.LogRotate, _ = c.Section("log").Key("rotate").Bool()
	cfg.LogMaxSize, _ = c.Section("log").Key("max_size").Int()
	cfg.LogTargetType = c.Section("log").Key("target_type").String()

	// redis config
	cfg.RedisHost = c.Section("redis").Key("host").String()
	cfg.RedisPwd = c.Section("redis").Key("pwd").String()
	cfg.RedisPort = c.Section("redis").Key("port").String()
	cfg.RedisMaxIdle, _ = c.Section("redis").Key("max_idle").Int()
	cfg.RedisMaxActive, _ = c.Section("redis").Key("max_active").Int()

	// bucket config
	cfg.BucketNum, _ = c.Section("bucket").Key("num").Int()
	cfg.TTRBucketNum, _ = c.Section("TTRBucket").Key("num").Int()

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
	cfg.TcpServWeight, _ = c.Section("tcp_server").Key("weight").Int()

	// register config
	cfg.GregisterAddr = c.Section("gregister").Key("addr").String()

	// parse flag
	flag.StringVar(&cfg.TcpServAddr, "tcp_addr", tcpServAddr, "tcp address")
	flag.StringVar(&cfg.HttpServAddr, "http_addr", httpServAddr, "http address")
	flag.Parse()

	cfg.SetDefault()
	return cfg
}

func initLogger(cfg *configs.GnodeConfig) *logs.Dispatcher {
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

func initRedisPool(cfg *configs.GnodeConfig) *RedisDB {
	return Redis.InitPool(cfg)
}

func register(registerAddr, tcpAddr, httpAddr string, weight int) error {
	hosts := strings.Split(registerAddr, ",")
	for _, host := range hosts {
		url := fmt.Sprintf("%s/register?tcp_addr=%s&http_addr=%s&weight=%d", host, tcpAddr, httpAddr, weight)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
	}

	return nil
}

func unregister(registerAddr, tcpAddr string) error {
	ts := strings.Split(registerAddr, ",")
	for _, t := range ts {
		url := t + "/unregister?tcp_addr=" + tcpAddr
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
	}

	return nil
}
