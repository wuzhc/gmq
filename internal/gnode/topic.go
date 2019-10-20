package gnode

import (
	"encoding/binary"
	"errors"
	"fmt"
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
	wg         utils.WaitGroupWrapper
	isAutoAck  bool
	dispatcher *Dispatcher
	exitChan   chan struct{}
	waitAckMap map[int64]int64
	waitAckMux sync.Mutex
	sync.Mutex
}

func NewTopic(name string, ctx *Context) *Topic {
	t := &Topic{
		ctx:        ctx,
		name:       name,
		isAutoAck:  true,
		exitChan:   make(chan struct{}),
		queue:      NewQueue(name),
		waitAckMap: make(map[int64]int64),
		dispatcher: ctx.Dispatcher,
		startTime:  time.Now(),
	}

	t.init()
	return t
}

// 初始化
// 初始化bucket
func (t *Topic) init() {
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
}

// 退出topic
func (t *Topic) exit() {
	close(t.exitChan)
	t.wg.Wait()
}

// 消息推送
func (t *Topic) push(msgId int64, msg []byte, delay int) error {
	if delay > 0 {
		start := time.Now()
		err := t.pushMsgToBucket(msgId, msg, delay)
		t.LogWarn(time.Now().Sub(start))
		return err
	}

	t.queue.write(msgId, msg)
	atomic.AddInt64(&t.pushNum, 1)
	return nil
}

// 消息批量推送
func (t *Topic) mpush(msgIds []int64, msgs [][]byte, delays []int) error {
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
		start := time.Now()
		return t.mpushMsgToBucket(msgIds, msgs, delays)
		t.LogWarn(time.Now().Sub(start))
	}

	return nil
}

// bucket.key : delay - msgId
func creatBucketKey(msgId int64, delay int64) []byte {
	var buf = make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], uint64(delay))
	binary.BigEndian.PutUint64(buf[8:], uint64(msgId))
	return buf
}

// 解析bucket.key
func parseBucketKey(key []byte) (int64, int64) {
	return int64(binary.BigEndian.Uint64(key[:8])), int64(binary.BigEndian.Uint64(key[8:]))
}

// 延迟消息保存到bucket
// topic.delayMQ: {key:delayTime,value:msg.Id}
// db.topic: {key:delayTime-msg.Id,value:msg}
func (t *Topic) pushMsgToBucket(msgId int64, msg []byte, delay int) error {
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
func (t *Topic) mpushMsgToBucket(msgIds []int64, msgs [][]byte, delays []int) error {
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
				// t.LogWarn(fmt.Sprintf("retreval from bucket with %v", msgId))
			} else {
				break
			}
		}

		return nil
	})
}

// 消息消费
func (t *Topic) pop() (msgId int64, msg []byte, err error) {
	msgId, msg, err = t.queue.read()
	if err == nil && msgId > 0 {
		atomic.AddInt64(&t.popNum, 1)
	}
	return
}

// 消息确认
func (t *Topic) ack(msgId int64) error {
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
