package gnode

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	bolt "github.com/wuzhc/bbolt"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

const (
	DEAD_FLAG             = "_failure"
	ROUTE_KEY_MATCH_FULL  = 1
	ROUTE_KEY_MATCH_FUZZY = 2
)

type Topic struct {
	name       string
	mode       int
	msgTTR     int
	msgRetry   int
	isAutoAck  bool
	pushNum    int64
	popNum     int64
	startTime  time.Time
	ctx        *Context
	closed     bool
	wg         utils.WaitGroupWrapper
	dispatcher *Dispatcher
	exitChan   chan struct{}
	queues     map[string]*queue
	waitAckMux sync.Mutex
	queueMux   sync.Mutex
	sync.Mutex
}

type TopicMeta struct {
	Mode      int         `json:"mode"`
	PopNum    int64       `json:"pop_num"`
	PushNum   int64       `json:"push_num"`
	IsAutoAck bool        `json:"is_auto_ack"`
	Queues    []QueueMeta `json:"queues"`
}

type QueueMeta struct {
	Num         int64  `json:"queue_num"`
	Name        string `json:"queue_name"`
	BindKey     string `json:"bind_key"`
	WriteOffset int64  `json:"write_offset"`
	ReadOffset  int64  `json:"read_offset"`
	ScanOffset  int64  `json:"scan_offset"`
}

func NewTopic(name string, ctx *Context) *Topic {
	t := &Topic{
		ctx:        ctx,
		name:       name,
		msgTTR:     MSG_MAX_TTR,
		msgRetry:   MSG_MAX_RETRY,
		mode:       ROUTE_KEY_MATCH_FUZZY,
		isAutoAck:  true,
		exitChan:   make(chan struct{}),
		dispatcher: ctx.Dispatcher,
		startTime:  time.Now(),
		queues:     make(map[string]*queue),
	}

	t.init()
	return t
}

// 初始化
func (t *Topic) init() {
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

	fd, err := os.OpenFile(fmt.Sprintf("%s/%s.meta", t.ctx.Conf.DataSavePath, t.name), os.O_RDONLY, 0600)
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

	// retore topic meta data
	t.mode = meta.Mode
	t.popNum = meta.PopNum
	t.pushNum = meta.PushNum
	t.isAutoAck = meta.IsAutoAck

	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	// restore queue meta data
	for _, q := range meta.Queues {
		queue := NewQueue(q.Name, q.BindKey, t.ctx, t)
		queue.woffset = q.WriteOffset
		queue.roffset = q.ReadOffset
		queue.soffset = q.ScanOffset
		queue.num = q.Num
		queue.name = q.Name
		t.queues[q.BindKey] = queue
	}
}

// 退出topic
func (t *Topic) exit() {
	defer t.LogInfo(fmt.Sprintf("topic.%s has exit.", t.name))

	t.closed = true
	close(t.exitChan)
	t.wg.Wait()

	t.LogInfo(fmt.Sprintf("writing topic.%s metadata.", t.name))
	fd, err := os.OpenFile(fmt.Sprintf("%s/%s.meta", t.ctx.Conf.DataSavePath, t.name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.LogError(fmt.Sprintf("write %s.meta failed, %v", t.name, err))
	}
	defer fd.Close()

	// save all queue meta
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	var queues []QueueMeta
	for k, q := range t.queues {
		queues = append(queues, QueueMeta{
			Num:         q.num,
			Name:        q.name,
			BindKey:     k,
			WriteOffset: q.woffset,
			ReadOffset:  q.roffset,
			ScanOffset:  q.soffset,
		})
	}

	meta := TopicMeta{
		PopNum:    t.popNum,
		PushNum:   t.pushNum,
		IsAutoAck: t.isAutoAck,
		Queues:    queues,
		Mode:      t.mode,
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
func (t *Topic) push(msg *Msg, routeKey string) error {
	queues := t.getQueuesByRouteKey(routeKey)
	if len(queues) == 0 {
		return fmt.Errorf("routeKey:%s is not match with queue, the mode is %d", routeKey, t.mode)
	}

	if msg.Delay > 0 {
		// 记录延迟消息需要被发送的queue
		bindKeys := make([]string, len(queues))
		for i, q := range queues {
			bindKeys[i] = q.bindKey
		}

		msg.Expire = uint64(msg.Delay) + uint64(time.Now().Unix())
		return t.pushMsgToBucket(&DelayMsg{msg, bindKeys})
	}

	for _, q := range queues {
		if err := q.write(Encode(msg)); err != nil {
			return err
		}
		atomic.AddInt64(&t.pushNum, 1)
	}

	return nil
}

// bucket.key : delay + msgId
func creatBucketKey(msgId uint64, expire uint64) []byte {
	var buf = make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], expire)
	binary.BigEndian.PutUint64(buf[8:], msgId)
	return buf
}

// 解析bucket.key
func parseBucketKey(key []byte) (uint64, uint64) {
	return binary.BigEndian.Uint64(key[:8]), binary.BigEndian.Uint64(key[8:])
}

// 延迟消息保存到bucket
func (t *Topic) pushMsgToBucket(dg *DelayMsg) error {
	err := t.dispatcher.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(t.name))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		bucket := tx.Bucket([]byte(t.name))
		key := creatBucketKey(dg.Msg.Id, dg.Msg.Expire)
		// t.LogInfo(fmt.Sprintf("%v-%v-%v write in bucket", delayTime, msgId, key))

		value, err := json.Marshal(dg)
		if err != nil {
			return err
		}
		if err := bucket.Put(key, value); err != nil {
			return err
		}

		return nil
	})

	dg.Msg = nil
	dg = nil
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
		key := creatBucketKey(msg.Id, 0)
		// t.LogInfo(fmt.Sprintf("%v-%v-%v write in bucket", delayTime, msgId, key))
		if err := bucket.Put(key, Encode(msg)); err != nil {
			return err
		}

		return nil
	})
}

// 检索延迟消息
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
			// 因为消息是有序的,当一个消息的到期时间比当前时间大,
			// 说明之后信息都还未到期,此时可以退出检索了
			delayTime, _ := parseBucketKey(key)
			if now < int64(delayTime) {
				break
			}

			var dg DelayMsg
			if err := json.Unmarshal(data, &dg); err != nil {
				t.LogError(fmt.Errorf("decode delay message failed, %s", err))
				goto deleteBucketElem
			}
			if dg.Msg.Id == 0 {
				t.LogError(fmt.Errorf("invalid delay message."))
				goto deleteBucketElem
			}

			for _, bindKey := range dg.BindKeys {
				queue := t.getQueueByBindKey(bindKey)
				if queue == nil {
					t.LogError(fmt.Sprintf("bindkey:%s is not associated with queue", bindKey))
					continue
				}
				if err := queue.write(Encode(dg.Msg)); err != nil {
					t.LogError(err)
					continue
				}
				atomic.AddInt64(&t.pushNum, 1)
				num++
			}

		deleteBucketElem:
			if err := bucket.Delete(key); err != nil {
				t.LogError(err)
			}
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

// 检测超时消息
func (t *Topic) retrievalQueueExipreMsg() error {
	if t.closed {
		err := fmt.Errorf("topic.%s has exit.", t.name)
		t.LogWarn(err)
		return err
	}

	num := 0
	for _, queue := range t.queues {
		for {
			data, err := queue.scan()
			if err != nil {
				if err != ErrMessageNotExist && err != ErrMessageNotExpire {
					t.LogError(err)
				}
				break
			}

			msg := Decode(data)
			if msg.Id == 0 {
				msg = nil
				break
			}

			// 移除消息等待状态
			if err := queue.removeWait(msg.Id); err != nil {
				t.LogError(err)
				break
			}

			// 消息重新消费次数超过阀值，则将消息添加到死信队列
			msg.Retry = msg.Retry + 1 // incr retry number
			if msg.Retry > uint16(t.msgRetry) {
				t.LogDebug(fmt.Sprintf("msg.Id %v has been added to dead queue.", msg.Id))
				if err := t.pushMsgToDeadBucket(msg); err != nil {
					t.LogError(err)
					break
				} else {
					continue
				}
			}

			// 消息到期,重新添加到队列等待再次被消费
			if err := queue.write(Encode(msg)); err != nil {
				t.LogError(err)
				break
			} else {
				t.LogDebug(fmt.Sprintf("msg.Id %v has expired and will be consumed again.", msg.Id))
				atomic.AddInt64(&t.pushNum, 1)
				num++
			}
		}
	}

	if num > 0 {
		return nil
	} else {
		return ErrMessageNotExist
	}
}

// 消息消费
func (t *Topic) pop(bindKey string) (*Msg, error) {
	queue := t.getQueueByBindKey(bindKey)
	if queue == nil {
		return nil, fmt.Errorf("bindKey:%s can't match queue", bindKey)
	}

	data, err := queue.read(t.isAutoAck)
	if err != nil {
		return nil, err
	}

	msg := Decode(data)
	if msg.Id == 0 {
		msg = nil
		return nil, errors.New("message decode failed.")
	}

	atomic.AddInt64(&t.popNum, 1)
	return msg, nil
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
func (t *Topic) ack(msgId uint64, bindKey string) error {
	queue := t.getQueueByBindKey(bindKey)
	if queue == nil {
		return fmt.Errorf("bindkey:%s is not associated with queue", bindKey)
	}

	return queue.ack(msgId)
}

// 设置topic信息
func (t *Topic) set(configure *topicConfigure) error {
	t.Lock()
	defer t.Unlock()

	// 是否自动确认消息
	if configure.isAutoAck == 1 {
		t.isAutoAck = true
	} else {
		t.isAutoAck = false
	}

	// 消息路由模式
	if configure.mode == ROUTE_KEY_MATCH_FULL {
		t.mode = ROUTE_KEY_MATCH_FULL
	} else if configure.mode == ROUTE_KEY_MATCH_FUZZY {
		t.mode = ROUTE_KEY_MATCH_FUZZY
	}

	// 执行过期时间
	if configure.msgTTR > 0 && configure.msgTTR < MSG_MAX_TTR {
		t.msgTTR = configure.msgTTR
	}

	// 消息重试次数阀值
	if configure.msgRetry > 0 && configure.msgRetry < MSG_MAX_RETRY {
		t.msgRetry = configure.msgRetry
	}

	return nil
}

// 声明队列，绑定key必须是唯一值
// 队列名称为<topic_name>_<bind_key>
func (t *Topic) delcareQueue(bindKey string) error {
	queue := t.getQueueByBindKey(bindKey)
	if queue != nil {
		return fmt.Errorf("bindKey %s has exist.", bindKey)
	}

	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	queueName := fmt.Sprintf("%s_%s", t.name, bindKey)
	t.queues[bindKey] = NewQueue(queueName, bindKey, t.ctx, t)
	return nil
}

// 根据key获取队列，key匹配支持全匹配和模糊匹配
func (t *Topic) getQueuesByRouteKey(key string) []*queue {
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	var queues []*queue
	for k, v := range t.queues {
		if t.mode == ROUTE_KEY_MATCH_FULL {
			if k == key {
				queues = append(queues, v)
			}
		} else {
			if ok, _ := regexp.MatchString(key, k); ok {
				queues = append(queues, v)
			}
		}
	}

	return queues
}

func (t *Topic) getQueueByBindKey(key string) *queue {
	t.queueMux.Lock()
	defer t.queueMux.Unlock()

	if q, ok := t.queues[key]; ok {
		return q
	} else {
		return nil
	}
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

func (t *Topic) LogDebug(msg interface{}) {
	t.ctx.Logger.Debug(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}

func (t *Topic) LogTrace(msg interface{}) {
	t.ctx.Logger.Trace(logs.LogCategory(fmt.Sprintf("Topic.%s", t.name)), msg)
}
