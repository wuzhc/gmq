package gnode

import (
	"github.com/wuzhc/gmq/configs"
	"github.com/wuzhc/gmq/pkg/logs"
)

type Context struct {
	Gnode      *Gnode
	Conf       *configs.GnodeConfig
	Logger     *logs.Dispatcher
}
