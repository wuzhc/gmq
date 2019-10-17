package gnode

import (
	"bytes"
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
	ctx        *Context
	pushNum    int32
	queue      *queue
	delayMQ    *skiplist
	wg         utils.WaitGroupWrapper
	isAutoAck  bool
	dispatcher *Dispatcher
	exitChan   chan struct{}
	delayMap   map[int64]string
	sync.Mutex
}

func NewTopic(name string, ctx *Context) *Topic {
	t := &Topic{
		ctx:        ctx,
		name:       name,
		isAutoAck:  true,
		exitChan:   make(chan struct{}),
		delayMQ:    NewSkiplist(32),
		queue:      NewQueue(name),
		delayMap:   make(map[int64]string),
		dispatcher: ctx.Dispatcher,
	}

	t.init()
	return t
}

// 初始化
// 导入磁盘上延迟消息
func (t *Topic) init() {
	t.LogInfo("init")
	if err := t.importBucketMsg(); err != nil {
		panic(err)
	}
}

// 退出topic
func (t *Topic) exit() {
	close(t.exitChan)
	t.wg.Wait()
}

// 导入延迟消息
func (t *Topic) importBucketMsg() error {
	return t.dispatcher.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		if bucket.Stats().KeyN == 0 {
			return nil
		}

		now := time.Now().Unix()
		c := bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			buf := bytes.Split(k, []byte{'-'})
			delayTimeBuf, msgIdBuf := buf[0], buf[1]
			delayTime := binary.BigEndian.Uint64(delayTimeBuf)

			// 1个小时内并且delayMQ长度不超过1000
			if int64(delayTime)-now < 3600 && t.delayMQ.Size() < 1000 {
				t.delayMQ.Insert(int64(binary.BigEndian.Uint64(msgIdBuf)), delayTime)
			}
		}

		return nil
	})
}

// 消息推送
func (t *Topic) push(msgId int64, msg []byte, delay int) error {
	if delay > 0 {
		start := time.Now()
		err := t.pushDelayMsg(msgId, msg, delay)
		t.LogWarn(time.Now().Sub(start))
		return err
	}

	t.queue.write(msgId, msg)
	atomic.AddInt32(&t.pushNum, 1)
	return nil
}

// 延迟消息推送
// topic.delayMQ: {key:delayTime,value:msg.Id}
// db.topic: {key:delayTime-msg.Id,value:msg}
func (t *Topic) pushDelayMsg(msgId int64, msg []byte, delay int) error {
	return t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(t.name))
		if err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}

		var buf [][]byte
		bucket := tx.Bucket([]byte(t.name))
		delayTime := time.Now().Unix() + int64(delay)

		delayBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(delayBuf, uint64(delayTime))
		buf = append(buf, delayBuf)

		msgIdBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(msgIdBuf, uint64(msgId))
		buf = append(buf, msgIdBuf)

		key := bytes.Join(buf, []byte{'-'})
		// t.LogInfo(fmt.Sprintf("%v-%v-%v write in bucket", delayTime, msgId, key))
		if err := bucket.Put(key, msg); err != nil {
			return err
		}

		// 1个小时内,并且长度不超过1000
		if delay < 3600 && t.delayMQ.Size() < 1000 {
			t.delayMQ.Insert(msgId, uint64(delayTime))
		}

		buf = nil
		delayBuf = nil
		msgIdBuf = nil
		return nil
	})
}

// 处理到期消息
func (t *Topic) handleExpireMsg() error {
	delayTime, msgIds, err := t.delayMQ.Exipre(uint64(time.Now().Unix()))
	if err != nil {
		if t.delayMQ.Size() == 0 {
			// 队列已为空,再导入磁盘延迟消息到队列
			t.importBucketMsg()
		} else {
			// 如果磁盘中第一个消息比延迟队列第一个大
			dtime := t.getBucketFirstMsgTime()
			if dtime != 0 && int64(dtime) < time.Now().Unix() {
				t.delayMQ.Clear()
				t.importBucketMsg()
			}
		}

		return err
	}

	return t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		for _, v := range msgIds {
			if v == nil {
				t.LogInfo("msgId is nil")
				continue
			}

			var buf [][]byte
			msgId := v.(int64)

			delayBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(delayBuf, uint64(delayTime))
			buf = append(buf, delayBuf)

			msgIdBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(msgIdBuf, uint64(msgId))
			buf = append(buf, msgIdBuf)

			key := bytes.Join(buf, []byte{'-'})
			msg := bucket.Get(key)
			// t.LogInfo(fmt.Sprintf("%v-%v-%v find in bucket", delayTime, msgId, key))
			if len(msg) == 0 {
				continue
			}

			fmt.Println("get msg from delayMQ", msg)
			if err := t.push(msgId, msg, 0); err != nil {
				// t.LogInfo("msg has been comfired")
				continue
			}
			if err := bucket.Delete(key); err != nil {
				t.LogError(err)
				continue
			}

			buf = nil
			msgIdBuf = nil
			delayBuf = nil
		}

		return nil
	})
}

// 检索bucket中到期消息写入到队列
func (t *Topic) retrievalBucketExpireMsg() error {
	return t.dispatcher.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		if bucket.Stats().KeyN == 0 {
			return nil
		}

		now := time.Now().Unix()
		c := bucket.Cursor()
		for key, msg := c.First(); key != nil; key, msg = c.Next() {
			buf := bytes.Split(key, []byte{'-'})
			delayTimeBuf, msgIdBuf := buf[0], buf[1]
			delayTime := binary.BigEndian.Uint64(delayTimeBuf)

			if now >= int64(delayTime) {
				t.queue.write(int64(binary.BigEndian.Uint64(msgIdBuf)), msg)
				if err := bucket.Delete(key); err != nil {
					t.LogError(err)
				}
			}
		}

		return nil
	})
}

// 获取bucket存储中第一个消息延迟时间
func (t *Topic) getBucketFirstMsgTime() uint64 {
	var delayTime uint64

	t.dispatcher.db.View(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(t.name))
		if err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}

		bucket := tx.Bucket([]byte(t.name))

		// 本地磁盘没有延迟消息,
		if bucket.Stats().KeyN == 0 {
			return nil
		}

		c := bucket.Cursor()
		k, _ := c.First()

		buf := bytes.Split(k, []byte{'-'})
		delayTimeBuf := buf[0]
		delayTime = binary.BigEndian.Uint64(delayTimeBuf)
		return nil
	})

	return delayTime
}

// 消息消费
func (t *Topic) pop() (int64, []byte, error) {
	return t.queue.read()
}

// 消息确认
func (t *Topic) ack(msgId int64) error {
	return t.dispatcher.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		key := make([]byte, 64)
		binary.BigEndian.PutUint64(key, uint64(msgId))
		if err := bucket.Delete(key); err != nil {
			return err
		} else {
			return nil
		}
	})
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
