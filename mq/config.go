package mq

import (
	"reflect"
)

type Config struct {
	// redis
	RedisMaxIdle   int `gmq:"redis_max_idle" def:"3"`
	RedisMaxActive int `gmq:"redis_max_active" def:"3000"`
	RedisPort      int `gmq:"redis_port" def:"6379"`
	RedisHost      string
	RedisPwd       string

	// grpc server
	GrpcGatewayAddr string
	GrpcAddr        string

	// log
	LogTargetType string
	LogFilename   string
	LogLevel      int
	LogMaxSize    int
	LogRotate     bool

	// bucket
	BucketNum int
	TTRBucket int
}

func (c *Config) SetInt(k string, v int) {

}

func (c *Config) SetString(k, v string) {

}

func (c *Config) GetInt(key string) int {
	// 如果是string,需要转为int,或者报错
	return c.key
}

func (c *Config) GetString(key string) string {
	// 如果是int,需要转为string,或者报错
	return c.key
}

func (c *Config) Validate() {

}
