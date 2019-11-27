// 初始化:
// 	- wfid为0,woffset为0
//	- rfid为0,roffset为0
// 	- 初始化一个totalSize大小的文件,内容为0,totalSzie大小为pageSize的整数倍

// 写(生产消息):
// 	- wfid为写文件编号,woffset为当前写偏移量,两个字段表示当前写到哪个文件哪个位置
// 	- 维护一个wfid和offset的map表
//	- wfid为0,初始化一个totalSize大小的文件,内容为0,totalSzie大小为pageSize的整数倍,执行映射,wfid加1
//	- 根据woffset,写入内容,更新woffset

// 读(消费消息):
//  - rfid为读文件编号,roffset为当前读偏移量,两个字段表示当前读到哪个文件哪个位置
// 	- rfid为0,rfid加1,查看文件是否存在,存在则映射
// 	- 根据roffset和woffset读取内容,更新roffset
//	- 读取完毕,删除写步骤的map表记录

// 扫(确认消息):
//  - sfid为扫文件编号,soffset为当前扫偏移量,两个字段表示当前扫描到哪个文件哪个位置
// 	- sfid为0,sfid加1,查看文件是否存在,存在则映射
// 	- 根据soffset和roffset读取内容,更新soffset
//	- 扫描完毕,删除数据文件,删除读步骤的map表记录
package gnode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/wuzhc/gmq/pkg/logs"
)

// flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
const MSG_FIX_LENGTH = 1 + 2 + 4
const GROW_SIZE = 10 * 1024 * 1024
const REWRITE_SIZE = 100 * 1024 * 1024

var queueSavePath string

type queue struct {
	woffset  int64
	roffset  int64
	soffset  int64
	filesize int64
	num      int64
	name     string
	data     []byte
	topic    *Topic
	file     *os.File
	ctx      *Context
	bindKey  string
	waitAck  map[uint64]int64
	sync.RWMutex
}

func NewQueue(name, bindKey string, ctx *Context, topic *Topic) *queue {
	queue := &queue{
		name:    name,
		ctx:     ctx,
		topic:   topic,
		bindKey: bindKey,
		waitAck: make(map[uint64]int64),
	}

	path := fmt.Sprintf("%s/%s.queue", ctx.Conf.DataSavePath, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Fatalf("open %s.queue failed, %v\n", queue.name, err)
	} else {
		queue.file = f
	}

	stat, err := f.Stat()
	if err != nil {
		log.Fatalf("get %s.queue stat failed, %v\n", queue.name, err)
	}

	initMmapSize := int(stat.Size())

	// 初始文件为一个页大小
	if initMmapSize == 0 {
		if _, err := f.WriteAt([]byte{'0'}, int64(os.Getpagesize())-1); err != nil {
			log.Fatalf("extend %v.queue failed, %v\n", queue.name, err)
		}

		if runtime.GOOS != "windows" {
			if err := syscall.Fdatasync(int(f.Fd())); err != nil {
				log.Fatalf("sync %v.queue failed, %v\n", queue.name, err)
			}
		} else {
			if err := f.Sync(); err != nil {
				log.Fatalf("sync %v.queue failed, %v\n", queue.name, err)
			}
		}

		initMmapSize = os.Getpagesize()
	}

	if err := queue.mmap(initMmapSize); err != nil {
		log.Fatalln(err)
	}

	return queue
}

// 队列扫描未确认消息
func (q *queue) scan() ([]byte, error) {
	q.Lock()
	// q.LogDebug(fmt.Sprintf("scan.offset:%v read.offset:%v write.offset:%v", q.soffset, q.roffset, q.woffset))

	if q.soffset > REWRITE_SIZE {
		q.rewrite()
	}
	if q.soffset == q.roffset {
		q.Unlock()
		return nil, ErrMessageNotExist
	}

	// 消息结构 flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
	if flag := q.data[q.soffset]; flag != 'v' {
		q.Unlock()
		return nil, errors.New("unkown msg flag")
	}

	status := binary.BigEndian.Uint16(q.data[q.soffset+1 : q.soffset+3])
	msgLen := int64(binary.BigEndian.Uint32(q.data[q.soffset+3 : q.soffset+7]))

	// 当前的消息已被确认了,继续扫描下一条消息
	if status == MSG_STATUS_FIN {
		q.soffset += MSG_FIX_LENGTH + msgLen
		q.Unlock()
		return q.scan()
	}

	expireTime := binary.BigEndian.Uint64(q.data[q.soffset+7 : q.soffset+15])
	q.LogDebug(fmt.Sprintf("msg.expire:%v now:%v", expireTime, time.Now().Unix()))

	// 队列中未有消息到期
	if expireTime > uint64(time.Now().Unix()) {
		q.Unlock()
		return nil, errors.New("no message expire.")
	}

	// 已过期消息状态设置为已到期,已过期消息将重新添加到队列,等待再次被消费
	binary.BigEndian.PutUint16(q.data[q.soffset+1:q.soffset+3], uint16(MSG_STATUS_EXPIRE))
	msg := make([]byte, msgLen)
	copy(msg, q.data[q.soffset+7:q.soffset+7+msgLen])
	q.soffset += MSG_FIX_LENGTH + msgLen

	q.Unlock()
	return msg, nil
}

// 重写文件大小
func (q *queue) rewrite() error {
	q.LogInfo(fmt.Sprintf("begin rewrite %v.queue, filesize:%v.", q.name, q.filesize))
	defer func() {
		q.LogInfo(fmt.Sprintf("after rewrite %v.queue, filesize:%v.", q.name, q.filesize))
	}()

	tempPath := fmt.Sprintf("%s/%s.temp.queue", q.ctx.Conf.DataSavePath, q.name)
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Fatalln(err)
	}

	pageSize := os.Getpagesize()
	size := q.filesize - int64(q.soffset)
	q.LogDebug(fmt.Sprintf("fileszie:%v,queue.soffset:%v,size:%v", q.filesize, q.soffset, size))

	// 确保mmap大小是页面大小的倍数
	sz := int(size)
	if (sz % pageSize) != 0 {
		remainSize := int64(sz - sz/pageSize*pageSize)
		if (q.filesize-q.woffset)-remainSize-int64(pageSize) > 0 {
			// 文件剩余空间满足页的大小的倍数,继续缩小文件大小
			size = size - remainSize
		} else {
			// 不满足,需要增加文件大小
			size = int64(sz/pageSize+1) * int64(pageSize)
		}
	}

	// 扩展文件
	_, err = f.WriteAt([]byte{'0'}, size-1)
	if err != nil {
		return err
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	// 将旧文件剩余数据迁移到新文件上
	copy(data, q.data[q.soffset:])

	q.file.Close()
	if err := q.unmap(); err != nil {
		return err
	}

	q.LogDebug(fmt.Sprintf("before rewrite, scan-offset:%v, read-offset:%v, write-offset:%v", q.soffset, q.roffset, q.woffset))
	q.data = data
	q.file = f
	q.roffset -= q.soffset
	q.woffset -= q.soffset
	q.filesize = size
	q.LogDebug(fmt.Sprintf("after rewrite, scan-offset:%v, read-offset:%v, write-offset:%v", q.soffset, q.roffset, q.woffset))
	// data = nil

	for k, v := range q.waitAck {
		q.waitAck[k] = v - q.soffset
	}

	q.soffset = 0
	path := fmt.Sprintf("%s/%s.queue", q.ctx.Conf.DataSavePath, q.name)
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	return nil
}

// 消息设置为已确认消费
func (q *queue) ack(msgId uint64) error {
	q.Lock()
	defer q.Unlock()

	offset, ok := q.waitAck[msgId]
	if !ok {
		return fmt.Errorf("msgId:%v is not exist.", msgId)
	}

	if offset > int64(len(q.data))-1 {
		return fmt.Errorf("ack.offset greather than queue.length.")
	}
	if q.data[offset] != 'v' {
		return fmt.Errorf("ack.offset error.")
	}

	binary.BigEndian.PutUint16(q.data[offset+1:offset+3], MSG_STATUS_FIN)
	delete(q.waitAck, msgId)
	return nil
}

// 移除消息等待状态
func (q *queue) removeWait(msgId uint64) error {
	q.Lock()
	defer q.Unlock()

	_, ok := q.waitAck[msgId]
	if !ok {
		return fmt.Errorf("msgId:%v is not exist.", msgId)
	}

	delete(q.waitAck, msgId)
	return nil
}

// 映射文件
func (q *queue) mmap(size int) error {
	stat, err := q.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap %v.queue failed, %v.\n", q.name, err)
	}
	if stat.Size() == 0 {
		return fmt.Errorf("mmap %v.queue failed, file is empty.\n", q.name)
	}

	// 解除上一次映射,如果有的话
	if len(q.data) > 0 {
		if err := q.unmap(); nil != err {
			return err
		}
	}

	if err := mmap(q, size); err != nil {
		return err
	}

	q.filesize = stat.Size()
	return nil
}

// 解除映射
func (q *queue) unmap() error {
	return unmap(q)
}

// 队列读取消息
func (q *queue) read(isAutoAck bool) ([]byte, error) {
	q.Lock()
	defer q.Unlock()

	msgOffset := q.roffset
	if q.roffset == q.woffset {
		return nil, errors.New("no message")
	}

	// 消息结构 flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
	// msg又包括expire(8-bytes) + id(8-bytes) + retry(2-bytes) + body(n-bytes)
	if flag := q.data[q.roffset]; flag != 'v' {
		return nil, errors.New("unkown msg flag")
	}

	msgLen := int64(binary.BigEndian.Uint32(q.data[q.roffset+3 : q.roffset+7]))
	msg := make([]byte, msgLen)
	copy(msg, q.data[q.roffset+7:q.roffset+7+msgLen])
	atomic.AddInt64(&q.num, -1)

	if isAutoAck {
		binary.BigEndian.PutUint16(q.data[q.roffset+1:q.roffset+3], uint16(MSG_STATUS_FIN))
	} else {
		binary.BigEndian.PutUint16(q.data[q.roffset+1:q.roffset+3], uint16(MSG_STATUS_WAIT))
		binary.BigEndian.PutUint64(q.data[q.roffset+7:q.roffset+15], uint64(time.Now().Unix())+uint64(q.topic.msgTTR))
		msgId := binary.BigEndian.Uint64(q.data[q.roffset+15 : q.roffset+23])
		q.waitAck[msgId] = msgOffset
		// q.LogDebug(fmt.Sprintf("msg had been readed. exipire time is %v", uint64(time.Now().Unix())+uint64(q.topic.msgTTR)))
	}

	// 移动读偏移位置
	q.roffset += MSG_FIX_LENGTH + msgLen
	return msg, nil
}

// 新写入信息的长度不能超过文件大小,超过则新建文件
func (q *queue) write(msg []byte) error {
	q.Lock()
	defer q.Unlock()

	msgLen := int64(len(msg))
	if q.woffset+MSG_FIX_LENGTH+msgLen > q.filesize {
		// 文件大小不够,需要扩展文件
		if err := q.grow(); err != nil {
			return err
		}
	}

	// package = flag(1-byte) + status(2-bytes) + msgLen(4-bytes) + msg(n-bytes)
	copy(q.data[q.woffset:q.woffset+1], []byte{'v'})
	binary.BigEndian.PutUint16(q.data[q.woffset+1:q.woffset+3], uint16(MSG_STATUS_DEFAULT))
	binary.BigEndian.PutUint32(q.data[q.woffset+3:q.woffset+7], uint32(msgLen))
	copy(q.data[q.woffset+7:q.woffset+7+msgLen], msg)

	q.woffset += MSG_FIX_LENGTH + msgLen
	atomic.AddInt64(&q.num, 1)

	return nil
}

// 扩展文件大小,每次为GROW_SIZE
func (q *queue) grow() error {
	fz := q.filesize + GROW_SIZE
	if err := q.mmap(int(fz)); err != nil {
		return err
	}

	if runtime.GOOS != "windows" {
		if err := q.file.Truncate(fz); err != nil {
			return errors.New(fmt.Sprintf("file resize error: %s\n", err))
		}
	}
	if err := q.file.Sync(); err != nil {
		return errors.New(fmt.Sprintf("file sync error: %s\n", err))
	}

	q.LogInfo(fmt.Sprintf("grow %v.queue size to %v, and old is %v, default to %v", q.name, fz, q.filesize, GROW_SIZE))
	q.filesize = fz
	return nil
}

func (q *queue) LogError(msg interface{}) {
	q.ctx.Logger.Error(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}

func (q *queue) LogWarn(msg interface{}) {
	q.ctx.Logger.Warn(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}

func (q *queue) LogInfo(msg interface{}) {
	q.ctx.Logger.Info(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}

func (q *queue) LogDebug(msg interface{}) {
	q.ctx.Logger.Debug(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}

func (q *queue) LogTrace(msg interface{}) {
	q.ctx.Logger.Trace(logs.LogCategory(fmt.Sprintf("Queue.%s", q.name)), msg)
}
