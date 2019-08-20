package gnode

import (
	"github.com/wuzhc/gmq/configs"
	"github.com/wuzhc/gmq/pkg/logs"
)

type Context struct {
	Gnode      *Gnode
	Dispatcher *Dispatcher
	Conf       *configs.GnodeConfig
	Logger     *logs.Dispatcher
	RedisDB    *RedisDB
}
