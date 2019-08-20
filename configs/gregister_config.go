package configs

type GregisterConfig struct {
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

func (c *GregisterConfig) Validate() {
	// to do
}

func (c *GregisterConfig) SetDefault() {
	// log default config
	if len(c.LogFilename) == 0 {
		c.LogFilename = "gregister.log"
	}
	if c.LogLevel <= 0 {
		c.LogLevel = 3
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
