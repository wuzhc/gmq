package configs

import (
	"errors"
)

type GnodeConfig struct {
	// node
	NodeId            int
	NodeWeight        int
	MsgTTR            int
	MsgMaxRetry       int
	MsgMaxPushNum     int
	DataSavePath      string
	EnableCluster     bool
	HeartbeatInterval int

	// gresiter
	GregisterAddr string

	// etcd
	EtcdEndPoints []string

	// http server
	HttpServAddr      string
	HttpServEnableTls bool
	HttpServCertFile  string
	HttpServKeyFile   string

	// tcp server
	TcpServAddr      string
	TcpServEnableTls bool
	TcpServCertFile  string
	TcpServKeyFile   string

	// report addr
	ReportHttpAddr string
	ReportTcpAddr  string

	// log
	LogTargetType string
	LogFilename   string
	LogLevel      int
	LogMaxSize    int
	LogRotate     bool
}

func (c *GnodeConfig) Validate() error {
	if c.MsgTTR > 60 {
		return errors.New("msgTTR can't greater than 60.")
	}
	if c.NodeId > 1024 || c.NodeId < 0 {
		return errors.New("nodeId must be between 1 and 1024.")
	}
	if c.LogLevel > 4 || c.LogLevel < 0 {
		return errors.New("logLevel must be between 0 and 4.")
	}
	if c.MsgMaxPushNum > 1000 || c.MsgMaxPushNum <= 0 {
		return errors.New("MsgMaxPushNum must be between 1 and 1000.")
	}
	return nil
}

func (c *GnodeConfig) SetDefault() {
	if c.NodeId == 0 {
		c.NodeId = 1
	}
	if c.NodeWeight == 0 {
		c.NodeWeight = 1
	}
	if c.MsgTTR == 0 {
		c.MsgTTR = 30
	}
	if c.MsgMaxRetry == 0 {
		c.MsgMaxRetry = 5
	}
	if c.MsgMaxPushNum == 0 {
		c.MsgMaxPushNum = 1000
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 5
	}

	// etcd
	if len(c.EtcdEndPoints) == 0 {
		c.EtcdEndPoints = append(c.EtcdEndPoints, "127.0.0.1:2379")
	}

	// 数据存储目录,相对于命令执行所在目录,例如在/home执行启动命令,将会生成/home/data目录
	if len(c.DataSavePath) == 0 {
		c.DataSavePath = "data/gnode"
	}

	// log default config
	if len(c.LogFilename) == 0 {
		c.LogFilename = "gnode.log"
	}
	if c.LogLevel <= 0 {
		c.LogLevel = 4
	}
	if c.LogMaxSize <= 5000000 {
		c.LogMaxSize = 5000000
	}
	if len(c.LogTargetType) == 0 {
		c.LogTargetType = "console"
	}

	// server default config
	if len(c.HttpServAddr) == 0 {
		c.HttpServAddr = "127.0.0.1:9504"
	}
	if len(c.TcpServAddr) == 0 {
		c.TcpServAddr = "127.0.0.1:9503"
	}

	// gresiger default config
	if len(c.GregisterAddr) == 0 {
		c.GregisterAddr = "http://127.0.0.1:9595"
	}

	if len(c.ReportHttpAddr) == 0 {
		c.ReportHttpAddr = c.HttpServAddr
	}
	if len(c.ReportTcpAddr) == 0 {
		c.ReportTcpAddr = c.TcpServAddr
	}
}
