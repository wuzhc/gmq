package configs

type GnodeConfig struct {
	// node
	NodeId     int64
	NodeWeight int64

	// redis
	RedisMaxIdle     int    `gmq:"redis_max_idle" def:"3"`
	RedisMaxActive   int    `gmq:"redis_max_active" def:"3000"`
	RedisPort        string `gmq:"redis_port" def:"6379"`
	RedisHost        string
	RedisPwd         string
	RedisPopInterVal int

	// gresiter
	GregisterAddr string

	// grpc server
	GrpcGatewayAddr string
	GrpcAddr        string

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

	// log
	LogTargetType string
	LogFilename   string
	LogLevel      int
	LogMaxSize    int
	LogRotate     bool

	// bucket
	BucketNum    int
	TTRBucketNum int
}

func (c *GnodeConfig) Validate() {
	// to do
}

func (c *GnodeConfig) SetDefault() {
	if c.NodeId == 0 {
		c.NodeId = 1
	}
	if c.NodeWeight == 0 {
		c.NodeWeight = 1
	}

	// bucket default config
	if c.BucketNum <= 0 {
		c.BucketNum = 3
	}
	if c.TTRBucketNum <= 0 {
		c.TTRBucketNum = 3
	}

	// log default config
	if len(c.LogFilename) == 0 {
		c.LogFilename = "gnode.log"
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

	// redis default config
	if len(c.RedisHost) == 0 {
		c.RedisHost = "127.0.0.1"
	}
	if c.RedisMaxActive <= 0 {
		c.RedisMaxActive = 3000
	}
	if c.RedisMaxIdle <= 0 {
		c.RedisMaxIdle = 3
	}
	if len(c.RedisPort) == 0 {
		c.RedisPort = "6379"
	}
	if c.RedisPopInterVal == 0 {
		c.RedisPopInterVal = 3
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
}
