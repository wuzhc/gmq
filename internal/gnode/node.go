package gnode

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/etcd-io/etcd/clientv3"
	"github.com/wuzhc/gmq/configs"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"

	"gopkg.in/ini.v1"
)

type Gnode struct {
	version  string
	running  int32
	exitChan chan struct{}
	wg       utils.WaitGroupWrapper
	cfg      *configs.GnodeConfig
	ctx      *Context
	etcd     etcd
}

type etcd struct {
	cli     *clientv3.Client
	leaseId clientv3.LeaseID
}

func New(cfg *configs.GnodeConfig) *Gnode {
	return &Gnode{
		cfg:      cfg,
		version:  "3.0",
		exitChan: make(chan struct{}),
	}
}

// begin run
func (gn *Gnode) Run() {
	if atomic.LoadInt32(&gn.running) == 1 {
		log.Fatalln("gnode is running.")
	}
	if !atomic.CompareAndSwapInt32(&gn.running, 0, 1) {
		log.Fatalln("gnode start failed.")
	}

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
	gn.wg.Wrap(NewBroker(ctx).Start)

	// whether to enable cluster, if true,
	// etcd must be started and the node will registers information to etcd
	if gn.cfg.EnableCluster {
		if err := gn.register(); err != nil {
			log.Fatalln(err)
		}
	}

	ctx.Logger.Info("gnode is running.")
}

// the node will registers information to etcd
func (gn *Gnode) register() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   gn.cfg.EtcdEndPoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("create etcd client failed, %s\n", err)
	}

	gn.etcd.cli = cli
	ch, err := gn.keepAlive()
	if err != nil {
		return err
	}

	gn.wg.Wrap(func() {
		gn.recvLeaseResponse(ch)
	})

	return nil
}

func (gn *Gnode) recvLeaseResponse(ch <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		select {
		case <-gn.exitChan:
			gn.revoke()
			return
		case <-gn.etcd.cli.Ctx().Done():
			return
		case ka, ok := <-ch:
			if !ok {
				gn.ctx.Logger.Info("keep alive channel closed")
				gn.revoke()
				return
			} else {
				gn.ctx.Logger.Debug(fmt.Sprintf("etcd lease keep alive, ttl:%d", ka.TTL))
			}
		}
	}
}

// revoke the lease
func (gn *Gnode) revoke() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := gn.etcd.cli.Revoke(ctx, gn.etcd.leaseId)
	cancel()
	if err != nil {
		gn.ctx.Logger.Info(fmt.Sprintf("etcd lease revoke failed, %s\n", err))
	}

	gn.ctx.Logger.Info("etcd lease has revoke.")
}

// keep the lease alive to ensure that the node is alive
func (gn *Gnode) keepAlive() (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := gn.etcd.cli.Grant(ctx, 30)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("grant etcd.leaseId failed, %s", err)
	}
	gn.etcd.leaseId = resp.ID

	key := fmt.Sprintf("/gmq/node-%d", gn.cfg.NodeId)
	info := make(map[string]string)
	info["server_addr"] = gn.cfg.RpcServAddr
	info["weight"] = strconv.Itoa(gn.cfg.NodeWeight)
	info["node_id"] = strconv.Itoa(gn.cfg.NodeId)
	info["join_time"] = time.Now().Format("2006-01-02 15:04:05")
	value, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("json marshal failed, %s", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = gn.etcd.cli.Put(ctx, key, string(value), clientv3.WithLease(resp.ID))
	if err != nil {
		return nil, fmt.Errorf("put key to etcd failed, %s", err)
	}

	return gn.etcd.cli.KeepAlive(context.TODO(), resp.ID)
}

func (gn *Gnode) Exit() {
	close(gn.exitChan)
	gn.wg.Wait()
}

func NewGnodeConfig() *configs.GnodeConfig {
	var err error
	var cfg *configs.GnodeConfig

	// specify config file
	cfgFile := flag.String("config_file", "", "config file")
	if len(*cfgFile) > 0 {
		cfg, err = LoadConfigFromFile(*cfgFile)
		if err != nil {
			log.Fatalf("load config file %v error, %v\n", *cfgFile, err)
		}
	} else {
		cfg = new(configs.GnodeConfig)
	}

	// command options
	var endpoints string
	flag.StringVar(&endpoints, "etcd_endpoints", strings.Join(cfg.EtcdEndPoints, ","), "etcd endpoints")
	flag.StringVar(&cfg.RpcServAddr, "rpc_addr", cfg.RpcServAddr, "rpc address")
	flag.IntVar(&cfg.NodeId, "node_id", cfg.NodeId, "node unique id")
	flag.IntVar(&cfg.NodeWeight, "node_weight", cfg.NodeWeight, "node weight")
	flag.IntVar(&cfg.MsgTTR, "msg_ttr", cfg.MsgTTR, "msg ttr")
	flag.IntVar(&cfg.MsgMaxRetry, "msg_max_retry", cfg.MsgMaxRetry, "msg max retry")
	flag.StringVar(&cfg.DataSavePath, "data_save_path", cfg.DataSavePath, "data save path")
	flag.IntVar(&cfg.LogLevel, "log_level", cfg.LogLevel, "log level,such as: 0,error 1,warn 2,info 3,trace 4,debug")
	flag.Parse()

	// parse etcd endpoints
	if len(endpoints) > 0 {
		cfg.EtcdEndPoints = strings.Split(endpoints, ",")
	}

	cfg.SetDefault()

	if err := cfg.Validate(); err != nil {
		log.Fatalf("config file %v error, %v\n", *cfgFile, err)
	}

	return cfg
}

func LoadConfigFromFile(cfgFile string) (*configs.GnodeConfig, error) {
	if res, err := utils.PathExists(cfgFile); !res {
		if err != nil {
			return nil, fmt.Errorf("config file %s is error, %s \n", cfgFile, err)
		} else {
			return nil, fmt.Errorf("config file %s is not exists \n", cfgFile)
		}
	}

	c, err := ini.Load(cfgFile)
	if err != nil {
		return nil, fmt.Errorf("load config file %v failed, %v \n", cfgFile, err)
	}

	cfg := new(configs.GnodeConfig)

	// node
	cfg.NodeId, _ = c.Section("node").Key("id").Int()
	cfg.NodeWeight, _ = c.Section("node").Key("weight").Int()
	cfg.MsgTTR, _ = c.Section("node").Key("msgTTR").Int()
	cfg.MsgMaxRetry, _ = c.Section("node").Key("msgMaxRetry").Int()
	cfg.DataSavePath = c.Section("node").Key("dataSavePath").String()

	// log config
	cfg.LogFilename = c.Section("log").Key("filename").String()
	cfg.LogLevel, _ = c.Section("log").Key("level").Int()
	cfg.LogRotate, _ = c.Section("log").Key("rotate").Bool()
	cfg.LogMaxSize, _ = c.Section("log").Key("max_size").Int()
	cfg.LogTargetType = c.Section("log").Key("target_type").String()

	// http server config
	cfg.RpcServAddr = c.Section("rpc_server").Key("addr").String()
	cfg.RpcServCertFile = c.Section("rpc_server").Key("certFile").String()
	cfg.RpcServKeyFile = c.Section("rpc_server").Key("keyFile").String()
	cfg.RpcServEnableTls, _ = c.Section("rpc_server").Key("enableTls").Bool()

	return cfg, nil
}

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
