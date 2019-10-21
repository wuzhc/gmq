package gnode

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"

	bolt "github.com/wuzhc/bbolt"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

type Topic struct {
	name       string
	pushNum    int64
	popNum     int64
	startTime  time.Time
	ctx        *Context
	queue      *queue
	closed     bool
	wg         utils.WaitGroupWrapper
	isAutoAck  bool
	dispatcher *Dispatcher
	exitChan   chan struct{}
	waitAckMap map[uint64]int64
	waitAckMux sync.Mutex
	sync.Mutex
}

type TopicMeta struct {
	PopNum      int64       `json:"pop_num"`
	PushNum     int64       `json:"push_num"`
	WriteFid    int         `json:"write_fid"`
	ReadFid     int         `json:"read_fid"`
	WriteOffset int         `json:"write_offset"`
	ReadOffset  int         `json:"read_offset"`
	WriteFMap   map[int]int `json:"write_file_map"`
}

func NewTopic(name string, ctx *Context) *Topic {
	t := &Topic{
		ctx:        ctx,
		name:       name,
		isAutoAck:  true,
		exitChan:   make(chan struct{}),
		queue:      NewQueue(name),
		waitAckMap: make(map[uint64]int64),
		dispatcher: ctx.Dispatcher,
		startTime:  time.Now(),
	}

	t.init()
	return t
}

// 初始化
func (t *Topic) init() {
	// 初始化bucket
	err := t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(t.name))
		if err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		} else {
			return nil
		}
	})
	if err != nil {
		panic(err)
	}

	t.LogInfo(fmt.Sprintf("loading topic.%s metadata.", t.name))

	// 初始化队列读写偏移量
	fd, err := os.OpenFile(fmt.Sprintf("%s.meta", t.name), os.O_RDONLY, 0600)
	if err != nil {
		if !os.IsNotExist(err) {
			t.LogError(fmt.Sprintf("load %s.meta failed, %v", t.name, err))
		}
		return
	}
	defer fd.Close()

	data, err := ioutil.ReadAll(fd)
	if err != nil {
		t.LogError(fmt.Sprintf("load %s.meta failed, %v", t.name, err))
		return
	}
	meta := &TopicMeta{}
	if err := json.Unmarshal(data, meta); err != nil {
		t.LogError(fmt.Sprintf("load %s.meta failed, %v", t.name, err))
		return
	}

	t.popNum = meta.PopNum
	t.pushNum = meta.PushNum
	err = t.queue.setByMetaData(meta.ReadFid, meta.ReadOffset, meta.WriteFid, meta.WriteOffset, meta.WriteFMap)
	if err != nil {
		t.LogError(fmt.Sprintf("init %s.queue failed, %v", t.name, err))
	}
}

// 退出topic
func (t *Topic) exit() {
	defer t.LogInfo(fmt.Sprintf("topic.%s has exit.", t.name))

	t.closed = true
	close(t.exitChan)
	t.wg.Wait()

	t.LogInfo(fmt.Sprintf("writing topic.%s metadata.", t.name))
	fd, err := os.OpenFile(fmt.Sprintf("%s.meta", t.name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.LogError(fmt.Sprintf("write %s.meta failed, %v", t.name, err))
	}
	defer fd.Close()

	meta := TopicMeta{
		PopNum:      t.popNum,
		PushNum:     t.pushNum,
		WriteFid:    t.queue.w.fid,
		ReadFid:     t.queue.r.fid,
		WriteOffset: t.queue.w.offset,
		ReadOffset:  t.queue.r.offset,
		WriteFMap:   t.queue.w.wmap,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		t.LogError(fmt.Sprintf("write %s.meta failed, %v", t.name, err))
	}

	_, err = fd.Write(data)
	if err != nil {
		t.LogError(fmt.Sprintf("write %s.meta failed, %v", t.name, err))
	}
}

// 消息推送
func (t *Topic) push(msgId uint64, msg []byte, delay int) error {
	if delay > 0 {
		return t.pushMsgToBucket(msgId, msg, delay)
	}

	t.queue.write(msgId, msg)
	atomic.AddInt64(&t.pushNum, 1)
	return nil
}

// 消息批量推送
func (t *Topic) mpush(msgIds []uint64, msgs [][]byte, delays []int) error {
	defer func() {
		msgs = nil
		msgIds = nil
		delays = nil
	}()

	for i := 0; i < len(msgIds); i++ {
		if delays[i] == 0 {
			atomic.AddInt64(&t.pushNum, 1)
			t.queue.write(msgIds[i], msgs[i])
			msgs = append(msgs[:i], msgs[i+1:]...)
			msgIds = append(msgIds[:i], msgIds[i+1:]...)
			delays = append(delays[:i], delays[i+1:]...)
		}
	}

	if len(msgIds) > 0 {
		return t.mpushMsgToBucket(msgIds, msgs, delays)
	}

	return nil
}

// bucket.key : delay - msgId
func creatBucketKey(msgId uint64, delay int64) []byte {
	var buf = make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(delay))
	binary.BigEndian.PutUint64(buf[8:], msgId)
	return buf
}

// 解析bucket.key
func parseBucketKey(key []byte) (uint64, uint64) {
	return binary.BigEndian.Uint64(key[:8]), binary.BigEndian.Uint64(key[8:])
}

// 延迟消息保存到bucket
// topic.delayMQ: {key:delayTime,value:msg.Id}
// db.topic: {key:delayTime-msg.Id,value:msg}
func (t *Topic) pushMsgToBucket(msgId uint64, msg []byte, delay int) error {
	return t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(t.name))
		if err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}

		bucket := tx.Bucket([]byte(t.name))
		key := creatBucketKey(msgId, int64(delay)+time.Now().Unix())
		// t.LogInfo(fmt.Sprintf("%v-%v-%v write in bucket", delayTime, msgId, key))
		if err := bucket.Put(key, msg); err != nil {
			return err
		}

		t.waitAckMux.Lock()
		t.waitAckMap[msgId] = int64(delay) + time.Now().Unix()
		t.waitAckMux.Unlock()

		return nil
	})
}

// 批量添加延迟消息到bucket
func (t *Topic) mpushMsgToBucket(msgIds []uint64, msgs [][]byte, delays []int) error {
	defer func() {
		msgs = nil
		msgIds = nil
		delays = nil
	}()

	return t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(t.name))
		if err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}

		now := time.Now().Unix()
		bucket := tx.Bucket([]byte(t.name))

		for i := 0; i < len(msgIds); i++ {
			key := creatBucketKey(msgIds[i], now+int64(delays[i]))
			if err := bucket.Put(key, msgs[i]); err != nil {
				return err
			}
		}

		return nil
	})
}

// 检索bucket中到期消息写入到队列
func (t *Topic) retrievalBucketExpireMsg() error {
	if t.closed {
		err := errors.New(fmt.Sprintf("topic.%s has exit."))
		t.LogWarn(err)
		return err
	}

	return t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		if bucket.Stats().KeyN == 0 {
			return nil
		}

		now := time.Now().Unix()
		c := bucket.Cursor()
		for key, msg := c.First(); key != nil; key, msg = c.Next() {
			delayTime, msgId := parseBucketKey(key)
			if now >= int64(delayTime) {
				if err := t.push(msgId, msg, 0); err != nil {
					t.LogError(err)
				}
				if err := bucket.Delete(key); err != nil {
					t.LogError(err)
				}
			} else {
				break
			}
		}

		return nil
	})
}

// 消息消费
func (t *Topic) pop() (msgId uint64, msg []byte, err error) {
	msgId, msg, err = t.queue.read()
	if err == nil && msgId > 0 {
		atomic.AddInt64(&t.popNum, 1)
	}
	return
}

// 消息确认
func (t *Topic) ack(msgId uint64) error {
	delay, ok := t.waitAckMap[msgId] // 有先后顺序,并且msgId是唯一的,所以不需要加锁
	if !ok {
		return errors.New(fmt.Sprintf("msgId:%v is not exist", msgId))
	}

	return t.dispatcher.db.View(func(tx *bolt.Tx) error {
		key := creatBucketKey(msgId, delay)
		bucket := tx.Bucket([]byte(t.name))
		if err := bucket.Delete(key); err != nil {
			delete(t.waitAckMap, msgId)
			return err
		} else {
			return nil
		}
	})
}

// 获取bucket堆积数量
func (t *Topic) getBucketNum() int {
	var num int
	t.dispatcher.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		num = bucket.Stats().KeyN
		return nil
	})
	return num
}

func (t *Topic) LogError(msg interface{}) {
	t.ctx.Logger.Error(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}

func (t *Topic) LogWarn(msg interface{}) {
	t.ctx.Logger.Warn(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}

func (t *Topic) LogInfo(msg interface{}) {
	t.ctx.Logger.Info(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}
