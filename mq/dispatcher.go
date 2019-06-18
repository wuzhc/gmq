package mq

import (
	"errors"
	"sort"
	"strconv"

	"goer/logs"

	"gopkg.in/ini.v1"
)

var (
	log               *logs.Dispatcher
	ErrBucketNum      = errors.New("The number of buckets must be greater then 0")
	ErrDispacherNoRun = errors.New("Dispacher is not running")
)

func init() {
	log = logs.NewDispatcher()
	log.SetTarget(logs.TARGET_FILE, `{"filename":"xxx.log","level":2,"max_size":50000000,"rotate":true}`)
	log.SetTarget(logs.TARGET_CONSOLE, "")
}

// 添加Job到Job Pool
// 调度Job分配到bucket
// 管理bucket
type Dispatcher struct {
	conf        *ini.File
	addToBucket chan *JobCard
	bucket      []*Bucket
	running     int
	closeChan   chan struct{}
}

func NewDispatcher() *Dispatcher {
	// cfg, err := ini.Load("conf.ini")
	// if err != nil {
	// panic("Road config.ini failed, " + err.Error())
	// }

	return &Dispatcher{
		running:     0,
		addToBucket: make(chan *JobCard),
		closeChan:   make(chan struct{}),
	}
}

// 执行调度器
func (d *Dispatcher) Run() {
	if d.running == 1 {
		return
	}
	if err := d.initBucket(); err != nil {
		panic(err)
	}

	d.running = 1
	for {
		select {
		case card := <-d.addToBucket:
			// 分配到数量少的bucket
			sort.Sort(ByNum(d.bucket))
			d.bucket[0].recvJob <- card
		case <-d.closeChan:
			d.running = 0
		}
	}
}

// 初始化bucket
func (d *Dispatcher) initBucket() error {
	n := 30
	if n <= 0 {
		return ErrBucketNum
	}
	for i := 0; i < n; i++ {
		b := &Bucket{
			Id:              strconv.Itoa(i),
			JobNum:          0,
			recvJob:         make(chan *JobCard),
			addToReadyQueue: make(chan string),
			resetTimerChan:  make(chan struct{}),
		}

		// 初始化job数量,可能上次执行到一半就终止了
		b.JobNum = GetBucketJobNum(b)
		go b.run()
		d.bucket = append(d.bucket, b)
	}
	return nil
}

// 添加任务到对象池
func (d *Dispatcher) AddToJobPool(j *Job) error {
	if d.running == 0 {
		return ErrDispacherNoRun
	}
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
