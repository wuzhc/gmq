package mq

import (
	"errors"
	"sort"
	"strconv"
)

var (
	ErrBucketNum      = errors.New("The number of buckets must be greater then 0")
	ErrTTRBucketNum   = errors.New("The number of TTRBuckets must be greater then 0")
	ErrDispacherNoRun = errors.New("Dispacher is not running")
)

// 添加Job到Job Pool
// 调度Job分配到bucket
// 管理bucket
type Dispatcher struct {
	addToBucket    chan *JobCard
	addToTTRBucket chan *JobCard
	bucket         []*Bucket
	TTRBuckets     []*Bucket
	closed         chan struct{}
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		addToBucket:    make(chan *JobCard),
		addToTTRBucket: make(chan *JobCard),
		closed:         make(chan struct{}),
	}
}

// job调度器,负责bucket分配
func (d *Dispatcher) Run() {
	defer gmq.wg.Done()
	gmq.wg.Add(1)

	if gmq.running == 0 {
		return
	}
	if err := d.initBucket(); err != nil {
		panic(err)
	}

	for {
		select {
		case card := <-d.addToBucket:
			if card.delay > 0 {
				// bucket.job_number可能会有差误,比如手动删除队列元素(这种情况需要重启服务,才能复位)
				sort.Sort(ByNum(d.bucket))
				d.bucket[0].recvJob <- card
			} else {
				// 延迟时间<=0,直接添加到队列(作为普通队列使用)
				if err := AddToReadyQueue(card.id); err != nil {
					// 添加ready queue失败了,要怎么处理
					log.Error(err)
				}
			}
		case card := <-d.addToTTRBucket:
			sort.Sort(ByNum(d.TTRBuckets))
			d.TTRBuckets[0].recvJob <- card
		case <-gmq.notify:
			log.Info("dispatcher notifies all bucket to close.")
			close(d.closed)
			return
		}
	}
}

// 初始化bucket
func (d *Dispatcher) initBucket() error {
	bucket_num, _ := gmq.cfg.Section("bucket").Key("num").Int()
	if bucket_num == 0 {
		return ErrBucketNum
	}
	TTRBucket_num, _ := gmq.cfg.Section("TTRBucket").Key("num").Int()
	if TTRBucket_num == 0 {
		return ErrTTRBucketNum
	}

	for i := 0; i < bucket_num; i++ {
		b := &Bucket{
			Id:              strconv.Itoa(i),
			JobNum:          0,
			recvJob:         make(chan *JobCard),
			addToReadyQueue: make(chan string),
			resetTimerChan:  make(chan struct{}),
			closed:          make(chan struct{}),
		}

		b.JobNum = GetBucketJobNum(b)
		go b.run()
		d.bucket = append(d.bucket, b)
	}

	for i := 0; i < TTRBucket_num; i++ {
		b := &Bucket{
			Id:              "TTR:" + string(i+65),
			JobNum:          0,
			recvJob:         make(chan *JobCard),
			addToReadyQueue: make(chan string),
			resetTimerChan:  make(chan struct{}),
			closed:          make(chan struct{}),
		}

		b.JobNum = GetBucketJobNum(b)
		go b.run()
		d.TTRBuckets = append(d.TTRBuckets, b)
	}

	return nil
}

// 添加任务到对象池
func (d *Dispatcher) AddToJobPool(j *Job) error {
	if err := j.CheckJobData(); err != nil {
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
