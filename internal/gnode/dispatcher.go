package gnode

import (
	"errors"
	"sync"
	"time"

	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

var (
	ErrBucketNum      = errors.New("The number of buckets must be greater then 0")
	ErrTTRBucketNum   = errors.New("The number of TTRBuckets must be greater then 0")
	ErrDispacherNoRun = errors.New("Dispacher is not running")
)

type Dispatcher struct {
	snowflake     *utils.Snowflake
	exitChan      chan struct{}
	ctx           *Context
	mux           sync.RWMutex
	wg            utils.WaitGroupWrapper
	memoryJobChan chan *Job
	delayMQ       *skiplist
	waitAckMQ     *skiplist
	topics        map[string]*Topic
}

func NewDispatcher(ctx *Context) *Dispatcher {
	sn, err := utils.NewSnowflake(ctx.Conf.NodeId)
	if err != nil {
		panic(err)
	}

	dispatcher := &Dispatcher{
		snowflake:     sn,
		exitChan:      make(chan struct{}),
		ctx:           ctx,
		memoryJobChan: make(chan *Job, 2),
		delayMQ:       NewSkiplist(ctx, "delay-mq"),
		waitAckMQ:     NewSkiplist(ctx, "wait-ack-mq"),
		topics:        make(map[string]*Topic),
	}

	ctx.Dispatcher = dispatcher
	return dispatcher
}

func (d *Dispatcher) Run() {
	defer func() {
		d.wg.Wait()
		d.LogInfo("Dispatcher exit.")
	}()

	for {
		select {
		case <-d.ctx.Gnode.exitChan:
			close(d.exitChan)
			for _, t := range d.topics {
				t.exit()
			}
			return
		case j := <-d.memoryJobChan:
			if j.Delay > 0 {
				score := int(time.Now().Unix()) + j.Delay
				d.delayMQ.Insert(j, score)
			} else {
				topic := d.GetTopic(j.Topic)
				topic.Push(j)
			}
		case j := <-d.delayMQ.ch:
			topic := d.GetTopic(j.Topic)
			topic.Push(j)
		case j := <-d.waitAckMQ.ch:
			topic := d.GetTopic(j.Topic)
			topic.Push(j)
		}
	}
}

func (d *Dispatcher) GetTopic(name string) *Topic {
	d.mux.Lock()
	defer d.mux.Unlock()

	if t, ok := d.topics[name]; ok {
		return t
	}

	t := NewTopic(name, d.ctx)
	d.topics[name] = t
	return t
}

func (d *Dispatcher) AddToJobPool(j *Job) error {
	if j.Id == 0 {
		j.Id = d.snowflake.Generate()
	}
	if err := j.Validate(); err != nil {
		return err
	}

	d.memoryJobChan <- j
	return nil
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
