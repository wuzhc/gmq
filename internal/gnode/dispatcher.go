package gnode

import (
	"errors"
	"sync"

	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

var (
	ErrBucketNum      = errors.New("The number of buckets must be greater then 0")
	ErrTTRBucketNum   = errors.New("The number of TTRBuckets must be greater then 0")
	ErrDispacherNoRun = errors.New("Dispacher is not running")
)

type Dispatcher struct {
	ctx       *Context
	topics    map[string]*Topic
	snowflake *utils.Snowflake
	sync.Mutex
}

func NewDispatcher(ctx *Context) *Dispatcher {
	sn, err := utils.NewSnowflake(ctx.Conf.NodeId)
	if err != nil {
		panic(err)
	}

	dispatcher := &Dispatcher{
		ctx:       ctx,
		topics:    make(map[string]*Topic),
		snowflake: sn,
	}

	ctx.Dispatcher = dispatcher
	return dispatcher
}

func (d *Dispatcher) Run() {
	defer d.LogInfo("Dispatcher exit.")

	for {
		select {
		case <-d.ctx.Gnode.exitChan:
			for _, t := range d.topics {
				t.exit()
			}
			return
		}
	}
}

func (d *Dispatcher) GetTopic(name string) *Topic {
	d.Lock()
	defer d.Unlock()

	if t, ok := d.topics[name]; ok {
		return t
	}

	t := NewTopic(name, d.ctx)
	d.topics[name] = t
	return t
}

func (d *Dispatcher) LogError(msg interface{}) {
	d.ctx.Logger.Error(logs.LogCategory("Dispatcher"), msg)
}

func (d *Dispatcher) LogWarn(msg interface{}) {
	d.ctx.Logger.Warn(logs.LogCategory("Dispatcher"), msg)
}

func (d *Dispatcher) LogInfo(msg interface{}) {
	d.ctx.Logger.Info(logs.LogCategory("Dispatcher"), msg)
}
