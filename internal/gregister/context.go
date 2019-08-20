package gregister

import (
	"github.com/wuzhc/gmq/configs"
	"github.com/wuzhc/gmq/pkg/logs"
)

type Context struct {
	Gregister *Gregister
	Conf      *configs.GregisterConfig
	Logger    *logs.Dispatcher
}
