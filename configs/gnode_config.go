package configs

import (
	"errors"
)

type GnodeConfig struct {
	// node
	NodeId       int
	NodeWeight   int
	MsgTTR       int
	MsgMaxRetry  int
	DataSavePath string

	// gresiter
	GregisterAddr string

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
	if c.MsgTTR > 300 {
		return errors.New("msgTTR can't greater than 300.")
	}
	if c.NodeId > 1024 || c.NodeId < 0 {
		return errors.New("nodeId must be between 1 and 1024.")
	}
	if c.LogLevel > 4 || c.LogLevel < 0 {
		return errors.New("log.level must be between 1 and 5.")
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
		c.MsgTTR = 60
	}
	if c.MsgMaxRetry == 0 {
		c.MsgMaxRetry = 5
	}
	// 数据存储目录,相对于命令执行所在目录,例如在/home执行启动命令,将会生成/home/data目录
	if len(c.DataSavePath) == 0 {
		c.DataSavePath = "data"
	}

	// log default config
	if len(c.LogFilename) == 0 {
		c.LogFilename = "gnode.log"
	}
	if c.LogLevel <= 0 {
		c.LogLevel = 2
	}
	if c.LogMaxSize <= 5000000 {
		c.LogMaxSize = 5000000
	}
	if len(c.LogTargetType) == 0 {
		c.LogTargetType = "file,console"
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
