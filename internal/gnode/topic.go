package gnode

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wuzhc/gmq/pkg/logs"
)

type Topic struct {
	Name           string
	MessageChan    chan *Job
	LikedList      *LikedList
	ctx            *Context
	pushNum        int32
	isPersist      bool
	isAutoAck      bool
	exitChan       chan struct{}
	dec            *gob.Decoder
	enc            *gob.Encoder
	reader, writer *os.File
	sync.Mutex
}

func NewTopic(name string, ctx *Context) *Topic {
	t := &Topic{
		ctx:         ctx,
		Name:        name,
		MessageChan: make(chan *Job),
		LikedList:   NewLikedList(ctx),
		isPersist:   true,
		isAutoAck:   true,
		exitChan:    make(chan struct{}),
	}

	// load data from disk if it is not empty
	if t.isPersist {
		reader, err := os.OpenFile(t.backupName(), os.O_CREATE|os.O_RDONLY, 0644)
		if err != nil {
			t.LogError(err)
			return t
		}
		writer, err := os.OpenFile(t.backupName(), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			t.LogError(err)
			return t
		}

		t.dec = gob.NewDecoder(reader)
		t.enc = gob.NewEncoder(writer)

		var jobs []*Job
		err = t.dec.Decode(&jobs)
		if err != nil {
			t.LogError(err)
			return t
		}

		for _, j := range jobs {
			t.LikedList.Push(j)
		}

		go t.persist()
	}

	return t
}

func (t *Topic) exit() {
	close(t.exitChan)
	t.LikedList.exit()
	t.writer.Close()
	t.reader.Close()
}

func (t *Topic) Push(j *Job) {
	t.LikedList.Push(j)
	atomic.AddInt32(&t.pushNum, 1)
}

func (t *Topic) Pop() (*Job, error) {
	select {
	case j := <-t.LikedList.ReadyChan:
		return j, nil
	case <-t.exitChan:
		return nil, errors.New("exit.")
	}
}

// save 900 1
// save 300 10
// save 60 10000
func (t *Topic) persist() {
	bgsaveCfg := t.ctx.Gnode.cfg.BGSave
	if len(bgsaveCfg) == 0 {
		return
	}

	var secs, nums []int
	bcfgs := strings.Split(bgsaveCfg, ",")
	for _, v := range bcfgs {
		vv := strings.Split(v, "-")
		s, _ := strconv.Atoi(vv[0])
		n, _ := strconv.Atoi(vv[1])
		secs = append(secs, s)
		nums = append(nums, n)
	}

	j := 0         // timing
	i := 0         // the number of thresholds
	l := len(secs) // thresholds level
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			j++
			if atomic.LoadInt32(&t.pushNum) >= int32(nums[i]) {
				t.doPersist()
				j = 0
				i = 0
				atomic.StoreInt32(&t.pushNum, 0)
				continue
			}
			if j > secs[i] && i < l-1 {
				i++ // next threshold
			}
		}
	}
}

func (t *Topic) doPersist() {
	jobs := make([]*Job, 0)
	node := t.LikedList.Front.Next
	for node != nil {
		jobs = append(jobs, node.Data)
		node = node.Next
	}
	t.enc.Encode(jobs)

	fmt.Println(t.Name, " persist success, total is ", len(jobs))
}

func (t *Topic) backupName() string {
	return fmt.Sprintf("%s.dat", t.Name)
}

func (t *Topic) LogError(msg interface{}) {
	t.ctx.Logger.Error(logs.LogCategory(fmt.Sprintf("Topic.%s", t.Name)), msg)
}

func (t *Topic) LogWarn(msg interface{}) {
	t.ctx.Logger.Warn(logs.LogCategory(fmt.Sprintf("Topic.%s", t.Name)), msg)
}

func (t *Topic) LogInfo(msg interface{}) {
	t.ctx.Logger.Info(logs.LogCategory(fmt.Sprintf("Topic.%s", t.Name)), msg)
}
