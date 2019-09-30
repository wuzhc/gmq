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
	ctx       *Context
	topics    map[string]*Topic
	snowflake *utils.Snowflake
	poolSize  int
	wg        utils.WaitGroupWrapper
	exitChan  chan struct{}
	sync.RWMutex
}

func NewDispatcher(ctx *Context) *Dispatcher {
	sn, err := utils.NewSnowflake(ctx.Conf.NodeId)
	if err != nil {
		panic(err)
	}

	dispatcher := &Dispatcher{
		ctx:       ctx,
		snowflake: sn,
		topics:    make(map[string]*Topic),
		exitChan:  make(chan struct{}),
	}

	ctx.Dispatcher = dispatcher
	return dispatcher
}

func (d *Dispatcher) Run() {
	defer d.LogInfo("Dispatcher exit.")

	d.wg.Wrap(d.scanLoop)

	select {
	case <-d.ctx.Gnode.exitChan:
		d.exit()
	}
}

func (d *Dispatcher) exit() {
	close(d.exitChan)
	for _, t := range d.topics {
		t.exit()
	}

	d.wg.Wait()
}

func (d *Dispatcher) GetTopic(name string) *Topic {
	d.RLock()
	defer d.RUnlock()

	if t, ok := d.topics[name]; ok {
		return t
	}

	t := NewTopic(name, d.ctx)
	d.topics[name] = t
	return t
}

func (d *Dispatcher) GetTopics() []*Topic {
	d.RLock()
	defer d.RUnlock()

	var topics []*Topic
	for _, t := range d.topics {
		topics = append(topics, t)
	}
	return topics
}

// timing polling scan
// from redis algorithm
// pool -> worker -> workCh
//				  -> closeCh
// 				  -> responseCh
func (d *Dispatcher) scanLoop() {
	selectNum := 20

	workCh := make(chan *Topic, selectNum)   // distribute topic to worker
	responseCh := make(chan bool, selectNum) // worker done and response
	closeCh := make(chan int)                // notify workers to exit
	scanTicker := time.NewTicker(1 * time.Second)
	freshTicker := time.NewTicker(10 * time.Second)

	topics := d.GetTopics()
	d.resizePool(len(topics), workCh, closeCh, responseCh) // init pool and worker

	for {
		select {
		case <-d.exitChan:
			goto exit
		case <-scanTicker.C:
			if len(topics) == 0 {
				continue
			}
		case <-freshTicker.C:
			topics = d.GetTopics()
			// d.LogInfo("start fresh-ticker", len(topics))
			d.resizePool(len(topics), workCh, closeCh, responseCh)
			continue
		}

		// rand topics
		if selectNum > len(topics) {
			selectNum = len(topics)
		}
	loop:
		for _, i := range utils.UniqRands(selectNum, len(topics)) {
			workCh <- topics[i]
		}

		hasMsgNum := 0
		for i := 0; i < selectNum; i++ {
			if <-responseCh {
				hasMsgNum++
			}
		}

		if float64(hasMsgNum)/float64(selectNum) > 0.25 {
			goto loop
		}
	}

exit:
	close(closeCh) // notify all worker exit
	scanTicker.Stop()
	freshTicker.Stop()
}

func (d *Dispatcher) scanWorker(workCh chan *Topic, closeCh chan int, responseCh chan bool) {
	for {
		select {
		case <-closeCh:
			d.poolSize--
			return
		case t := <-workCh:
			var flag bool
			var j *Job
			now := int(time.Now().Unix())
			j, _ = t.delayMQ.Arrival(now)
			if j != nil {
				flag = true
				j.Delay = 0
				t.Push(j)
			}

			j, _ = t.waitAckMQ.Arrival(now)
			if j != nil {
				flag = true
				j.Delay = 0
				t.Push(j)
			}

			// d.LogInfo("topic-name:", t.Name, "delay-mq-size:", t.delayMQ.size, "wait-ack-mq-size:", t.waitAckMQ.size)
			responseCh <- flag
		}
	}
}

// resize pool
func (d *Dispatcher) resizePool(topicNum int, workCh chan *Topic, closeCh chan int, responseCh chan bool) {
	workerNum := int(float64(topicNum) * 0.25)
	if workerNum < 1 {
		workerNum = 1
	}

	// add worker
	if workerNum > d.poolSize {
		d.LogInfo("add scan-Worker for pool", workerNum, d.poolSize)
		d.wg.Wrap(func() {
			d.scanWorker(workCh, closeCh, responseCh)
		})
		d.poolSize++
		return
	}

	// reduce worker
	if workerNum < d.poolSize {
		d.LogInfo("reduce scan-Worker for pool", workerNum, d.poolSize)
		for i := d.poolSize - workerNum; i > 0; i-- {
			closeCh <- 1
		}
	}
}

func (d *Dispatcher) LogError(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Dispatcher"))
	v = append(v, msg...)
	d.ctx.Logger.Error(v...)
}

func (d *Dispatcher) LogWarn(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Dispatcher"))
	v = append(v, msg...)
	d.ctx.Logger.Warn(v...)
}

func (d *Dispatcher) LogInfo(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Dispatcher"))
	v = append(v, msg...)
	d.ctx.Logger.Info(v...)
}
