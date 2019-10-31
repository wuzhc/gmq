package configs

import (
	"errors"
)

type GregisterConfig struct {
	Heartbeat    int
	DataSavePath string

	// http server
	HttpServAddr      string
	HttpServEnableTls bool
	HttpServCertFile  string
	HttpServKeyFile   string

	// log
	LogTargetType string
	LogFilename   string
	LogLevel      int
	LogMaxSize    int
	LogRotate     bool
}

func (c *GregisterConfig) Validate() error {
	if c.LogLevel > 4 || c.LogLevel < 0 {
		return errors.New("log.level must be between 1 and 5.")
	}
	return nil
}

func (c *GregisterConfig) SetDefault() {
	if c.Heartbeat == 0 {
		c.Heartbeat = 2
	}
	// 数据存储目录,相对于命令执行所在目录,例如在/home执行启动命令,将会生成/home/data目录
	if len(c.DataSavePath) == 0 {
		c.DataSavePath = "data"
	}

	// log default config
	if len(c.LogFilename) == 0 {
		c.LogFilename = "gregister.log"
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
		c.HttpServAddr = ":9595"
	}
}
