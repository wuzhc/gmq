// job调度器
// 功能:
// 	- 负责分配延时job到bucket
//	- 负责添加job到job pool
//	- 管理所有bucket

package gnode

import (
	"errors"
	"sort"
	"strconv"
	"sync"

	// "sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

var (
	ErrBucketNum      = errors.New("The number of buckets must be greater then 0")
	ErrTTRBucketNum   = errors.New("The number of TTRBuckets must be greater then 0")
	ErrDispacherNoRun = errors.New("Dispacher is not running")
)

type Dispatcher struct {
	snowflake      *utils.Snowflake
	addToBucket    chan *JobCard
	addToTTRBucket chan *JobCard
	exitChan       chan struct{}
	TTRBuckets     []*Bucket
	buckets        []*Bucket
	ctx            *Context
	mux            sync.RWMutex
	wg             utils.WaitGroupWrapper
	memoryJobChan  chan *Job
	num            int64
	conn           redis.Conn
	queue          *ItemQueue
}

func NewDispatcher(ctx *Context) *Dispatcher {
	sn, err := utils.NewSnowflake(ctx.Conf.NodeId)
	if err != nil {
		panic(err)
	}

	dispatcher := &Dispatcher{
		snowflake:      sn,
		addToBucket:    make(chan *JobCard),
		addToTTRBucket: make(chan *JobCard),
		exitChan:       make(chan struct{}),
		ctx:            ctx,
		memoryJobChan:  make(chan *Job, 2),
		conn:           Redis.Pool.Get(),
		queue:          NewItemQueue(),
	}

	ctx.Dispatcher = dispatcher
	go dispatcher.flushJob()
	go dispatcher.flushJobResult()
	return dispatcher
}

func (d *Dispatcher) Run() {
	defer func() {
		d.wg.Wait()
		d.LogInfo("Dispatcher exit.")
	}()

	if err := d.initBucket(); err != nil {
		panic(err)
	}

	for {
		select {
		case <-d.ctx.Gnode.exitChan:
			d.LogInfo("Dispather waiting for bucket exit.")
			close(d.exitChan)
			return
		case card := <-d.addToBucket:
			if card.delay > 0 {
				sort.Sort(ByNum(d.buckets))
				d.buckets[0].recvJob <- card
			} else {
				// 延迟时间job.delay <= 0,直接添加到topic队列
				if err := AddToReadyQueue(card.id); err != nil {
					d.LogError(err)
				}
			}
		case card := <-d.addToTTRBucket:
			sort.Sort(ByNum(d.TTRBuckets))
			d.TTRBuckets[0].recvJob <- card
		}
	}
}

func (d *Dispatcher) initBucket() error {
	if d.ctx.Conf.BucketNum == 0 {
		return ErrBucketNum
	}
	if d.ctx.Conf.TTRBucketNum == 0 {
		return ErrTTRBucketNum
	}

	for i := 0; i < d.ctx.Conf.BucketNum; i++ {
		id := strconv.Itoa(i)
		b := &Bucket{
			Id:              id,
			recvJob:         make(chan *JobCard),
			addToReadyQueue: make(chan int64),
			resetTimerChan:  make(chan struct{}),
			exitChan:        make(chan struct{}),
			JobNum:          GetBucketJobNum(id),
			dispatcher:      d,
			ctx:             d.ctx,
		}

		d.buckets = append(d.buckets, b)
		d.wg.Wrap(b.run)
	}

	for i := 0; i < d.ctx.Conf.TTRBucketNum; i++ {
		id := "TTR:" + string(i+65)
		b := &Bucket{
			Id:              id,
			recvJob:         make(chan *JobCard),
			addToReadyQueue: make(chan int64),
			resetTimerChan:  make(chan struct{}),
			exitChan:        make(chan struct{}),
			JobNum:          GetBucketJobNum(id),
			dispatcher:      d,
			ctx:             d.ctx,
		}

		d.TTRBuckets = append(d.TTRBuckets, b)
		d.wg.Wrap(b.run)
	}

	return nil
}

func (d *Dispatcher) AddToJobPool(j *Job) error {
	if j.Id == 0 {
		j.Id = d.snowflake.Generate()
	}
	if err := j.Validate(); err != nil {
		return err
	}

	select {
	case d.memoryJobChan <- j:
	default:
		if err := AddToJobPool(j); err != nil {
			return err
		}
	}

	return nil
}

func (d *Dispatcher) flushJob() {
	defer func() {
		d.LogError("lookup exit..")
	}()
	for {
		select {
		case <-d.ctx.Gnode.exitChan:
			d.LogInfo("Dispather lookup exit.")
			return
		case <-time.After(1 * time.Second):
			d.conn.Flush()
		case j := <-d.memoryJobChan: // 高并发情况下可能会写满4096字节或者1秒后刷盘,当两个事件都准备好时,随机执行其中一个io
			d.conn.Send("HMSET", redis.Args{}.Add(j.Key()).AddFlat(j)...)
			d.queue.Push(j.Card())
		}
	}
}

func (d *Dispatcher) flushJobResult() {
	for {
		if d.conn.Err() != nil {
			return
		}
		rs, err := redis.String(d.conn.Receive())
		if err != nil {
			d.LogError(err)
			continue
		}
		if rs != "OK" {
			d.LogError("Push job pool failed")
			continue
		}
		if card := d.queue.Pop(); card != nil {
			d.addToBucket <- card
		}
	}
}

func (d *Dispatcher) GetBuckets() []*Bucket {
	return d.buckets
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
