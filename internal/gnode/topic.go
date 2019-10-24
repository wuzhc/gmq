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

const (
	DEAD_FLAG = "_failure"
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
	IsAutoAck   bool        `json:"is_auto_ack"`
}

func NewTopic(name string, ctx *Context) *Topic {
	t := &Topic{
		ctx:        ctx,
		name:       name,
		isAutoAck:  false,
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
		if _, err := tx.CreateBucketIfNotExists([]byte(t.name)); err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(t.name + DEAD_FLAG)); err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}
		return nil
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
	t.isAutoAck = meta.IsAutoAck
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
		IsAutoAck:   t.isAutoAck,
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
func (t *Topic) push(msg *Msg) error {
	defer func() {
		msg = nil
	}()

	if msg.Delay > 0 {
		msg.Expire = int64(msg.Delay) + time.Now().Unix()
		return t.pushMsgToBucket(msg)
	}
	if err := t.queue.write(Encode(msg)); err != nil {
		return err
	}

	atomic.AddInt64(&t.pushNum, 1)
	return nil
}

// 消息批量推送
func (t *Topic) mpush(msgIds []uint64, msgs [][]byte, delays []int) error {
	var (
		dmsgIDs []uint64
		dmsgs   [][]byte
		ddelays []int
	)

	total := len(msgIds)
	for i := 0; i < total; i++ {
		if delays[i] == 0 {
			msg := &Msg{
				Id:   msgIds[i],
				Body: msgs[i],
			}
			atomic.AddInt64(&t.pushNum, 1)
			t.queue.write(Encode(msg))
			msg = nil
		} else {
			dmsgIDs = append(dmsgIDs, msgIds[i])
			dmsgs = append(dmsgs, msgs[i])
			ddelays = append(ddelays, delays[i])
		}
	}

	msgs = nil
	msgIds = nil
	delays = nil

	if len(dmsgIDs) > 0 {
		return t.mpushMsgToBucket(dmsgIDs, dmsgs, ddelays)
	}

	dmsgIDs = nil
	dmsgs = nil
	ddelays = nil

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
// db.topic: {key:expire+msg.Id,value:msg}
func (t *Topic) pushMsgToBucket(msg *Msg) error {
	err := t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(t.name))
		if err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}

		bucket := tx.Bucket([]byte(t.name))
		key := creatBucketKey(msg.Id, msg.Expire)
		// t.LogInfo(fmt.Sprintf("%v-%v-%v write in bucket", delayTime, msgId, key))
		if err := bucket.Put(key, Encode(msg)); err != nil {
			return err
		}

		return nil
	})

	msg = nil
	return err
}

// 死信队列
// 规定:如果一条消息6次被消费后都未收到客户端确认,则会被添加到死信队列
func (t *Topic) pushMsgToDeadBucket(msg *Msg) error {
	return t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		name := t.name + DEAD_FLAG
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}

		bucket := tx.Bucket([]byte(name))
		key := creatBucketKey(msg.Id, time.Now().Unix())
		// t.LogInfo(fmt.Sprintf("%v-%v-%v write in bucket", delayTime, msgId, key))
		if err := bucket.Put(key, Encode(msg)); err != nil {
			return err
		}

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
		err := errors.New(fmt.Sprintf("topic.%s has exit.", t.name))
		t.LogWarn(err)
		return err
	}

	var num int
	var err error
	err = t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name))
		if bucket.Stats().KeyN == 0 {
			return nil
		}

		now := time.Now().Unix()
		c := bucket.Cursor()
		for key, data := c.First(); key != nil; key, data = c.Next() {
			delayTime, _ := parseBucketKey(key)
			msg := Decode(data)
			if msg.Id == 0 {
				t.LogError(errors.New("decode message failed."))
				continue
			}

			// 因为消息是有序的,当一个消息的到期时间比当前时间大,说明之后信息都还未到期
			// 此时可以退出检索了
			if now < int64(delayTime) {
				break
			}

			if err := t.queue.write(data); err != nil {
				t.LogError(err)
				continue
			}
			if err := bucket.Delete(key); err != nil {
				t.LogError(err)
				continue
			}

			// waitAckMap存储待确认的消息索引信息
			delete(t.waitAckMap, msg.Id)
			atomic.AddInt64(&t.pushNum, 1)
			num++
		}

		return nil
	})

	if err != nil {
		return err
	}
	if num == 0 {
		return ErrMessageNotExist
	}

	return nil
}

// 消息消费
func (t *Topic) pop() (*Msg, error) {
	data, err := t.queue.read()
	if err != nil {
		return nil, err
	}

	msg := Decode(data)
	if msg.Id > 0 {
		atomic.AddInt64(&t.popNum, 1)
		return msg, nil
	} else {
		msg = nil
	}

	return nil, errors.New("message decode failed.")
}

// 私信队列
func (t *Topic) dead(num int) (msgs []*Msg, err error) {
	err = t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		name := t.name + DEAD_FLAG
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return errors.New(fmt.Sprintf("create bucket: %s", err))
		}

		bucket := tx.Bucket([]byte(name))
		if bucket.Stats().KeyN == 0 {
			return nil
		}

		c := bucket.Cursor()
		for key, msg := c.First(); key != nil; key, msg = c.Next() {
			if err := bucket.Delete(key); err != nil {
				return err
			}
			msgs = append(msgs, Decode(msg))
			if len(msgs) >= num {
				break
			}
		}

		return nil
	})

	return
}

// 消息确认
func (t *Topic) ack(msgId uint64) error {
	delay, ok := t.waitAckMap[msgId]
	if !ok {
		return errors.New(fmt.Sprintf("msgId:%v is not exist", msgId))
	}

	return t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		key := creatBucketKey(msgId, delay)
		bucket := tx.Bucket([]byte(t.name))
		if err := bucket.Delete(key); err != nil {
			return err
		} else {
			delete(t.waitAckMap, msgId)
			return nil
		}
	})
}

// 设置topic信息
func (t *Topic) set(isAutoAck int) error {
	t.Lock()
	defer t.Unlock()

	if isAutoAck == 1 {
		t.isAutoAck = true
	} else {
		t.isAutoAck = false
	}

	return nil
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

// 获取死信数量
func (t *Topic) getDeadNum() int {
	var num int
	t.dispatcher.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.name + DEAD_FLAG))
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
