// job调度器
// 功能:
// 		- 负责分配延时job到bucket
//		- 负责添加job到job pool
//		- 管理所有bucket

package gnode

import (
	"errors"
	"sort"
	"strconv"
	"sync"

	"github.com/wuzhc/gmq/pkg/utils"
)

var (
	ErrBucketNum      = errors.New("The number of buckets must be greater then 0")
	ErrTTRBucketNum   = errors.New("The number of TTRBuckets must be greater then 0")
	ErrDispacherNoRun = errors.New("Dispacher is not running")
)

type Dispatcher struct {
	addToBucket    chan *JobCard
	addToTTRBucket chan *JobCard
	closed         chan struct{}
	TTRBuckets     []*Bucket
	bucket         []*Bucket
	ctx            *Context
	mux            sync.RWMutex
	wg             utils.WaitGroupWrapper
}

func NewDispatcher(ctx *Context) *Dispatcher {
	dispatcher := &Dispatcher{
		addToBucket:    make(chan *JobCard),
		addToTTRBucket: make(chan *JobCard),
		closed:         make(chan struct{}),
		ctx:            ctx,
	}
	ctx.Dispatcher = dispatcher
	return dispatcher
}

func (d *Dispatcher) Run() {
	if err := d.initBucket(); err != nil {
		panic(err)
	}

	for {
		select {
		case card := <-d.addToBucket:
			if card.delay > 0 {
				sort.Sort(ByNum(d.bucket))
				d.bucket[0].recvJob <- card
			} else {
				// 延迟时间<=0,直接添加到队列(作为普通队列使用)
				if err := AddToReadyQueue(card.id); err != nil {
					// 添加ready queue失败了,要怎么处理
					d.ctx.Logger.Error(err)
				}
			}
		case card := <-d.addToTTRBucket:
			sort.Sort(ByNum(d.TTRBuckets))
			d.TTRBuckets[0].recvJob <- card
		case <-d.ctx.Gnode.ctx.Done():
			d.ctx.Logger.Info("dispatcher notifies all bucket to close.")
			close(d.closed)
			d.wg.Wait()
			return
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
		b := &Bucket{
			Id:              strconv.Itoa(i),
			recvJob:         make(chan *JobCard),
			addToReadyQueue: make(chan string),
			resetTimerChan:  make(chan struct{}),
			closed:          make(chan struct{}),
			ctx:             d.ctx,
		}

		b.JobNum = GetBucketJobNum(b)
		d.wg.Wrap(func() {
			b.run()
		})
		d.bucket = append(d.bucket, b)
	}

	for i := 0; i < d.ctx.Conf.TTRBucketNum; i++ {
		b := &Bucket{
			Id:              "TTR:" + string(i+65),
			recvJob:         make(chan *JobCard),
			addToReadyQueue: make(chan string),
			resetTimerChan:  make(chan struct{}),
			closed:          make(chan struct{}),
			ctx:             d.ctx,
		}

		b.JobNum = GetBucketJobNum(b)
		d.wg.Wrap(func() {
			b.run()
		})
		d.TTRBuckets = append(d.TTRBuckets, b)
	}

	return nil
}

func (d *Dispatcher) AddToJobPool(j *Job) error {
	if err := j.Validate(); err != nil {
		return err
	}
	if err := AddToJobPool(j); err != nil {
		return err
	}

	d.addToBucket <- j.Card()
	return nil
}

func (d *Dispatcher) GetBuckets() []*Bucket {
	return d.bucket
}
