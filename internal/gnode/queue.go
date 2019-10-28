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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/wuzhc/gmq/pkg/logs"
)

// const FILE_SIZE = 2 << 32 // 4G
// const FILE_SIZE = 209715200
const FILE_SIZE = 209715200

type queue struct {
	w    *writer
	r    *reader
	s    *scanner
	name string
	ctx  *Context
	num  int64
	sync.RWMutex
}

type writer struct {
	fid    int         // 文件编号,每次映射都会自增
	offset int         // 文件内容写入偏移量(即当前写入的位置)
	data   []byte      // 内存映射文件数据
	flag   bool        // 是否已映射
	wmap   map[int]int // 文件编号和偏移量关系表
}

type reader struct {
	fid    int         // 文件编号,每次映射都会自增
	offset int         // 文件内容读取偏移量(即当前读取的位置)
	data   []byte      // 内存映射文件数据
	flag   bool        // 是否已映射
	rmap   map[int]int // 文件编号和偏移量关系表
}

type scanner struct {
	fid    int    // 文件编号,每次映射都会自增
	offset int    // 文件内容读取偏移量(即当前读取的位置)
	data   []byte // 内存映射文件数据
	flag   bool   // 是否已映射
}

func NewQueue(name string, ctx *Context) *queue {
	return &queue{
		name: name,
		ctx:  ctx,
		w:    &writer{wmap: make(map[int]int)},
		r:    &reader{rmap: make(map[int]int)},
		s:    &scanner{},
	}
}

// 上次中断后,记录元数据,当topic.queue重启启动时,根据元数据还原上次执行环境
func (q *queue) setByMetaData(readFid, readOffset, writeFid, writeOffset int, wmap map[int]int) error {
	if readFid > 0 {
		q.r.fid = readFid
		q.r.offset = readOffset
		if err := q.r.mmap(q.name); err != nil {
			return err
		}
	}

	if writeFid > 0 {
		q.w.fid = writeFid
		q.w.offset = writeOffset
		if err := q.w.mmap(q.name); err != nil {
			return err
		}
		q.w.wmap = wmap
	}

	return nil
}

func (w *writer) mmap(queueName string) error {
	fname := fmt.Sprintf("%s_%d.queue", queueName, w.fid)

	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	// 扩展文件内容
	if _, err := f.WriteAt([]byte{'0'}, FILE_SIZE-1); nil != err {
		log.Fatalln(err)
	} else {
		f.Close()
	}

	f, err = os.OpenFile(fname, os.O_RDWR, 0600)
	if err != nil {
		return err
	} else {
		defer f.Close()
	}

	w.data, err = syscall.Mmap(int(f.Fd()), 0, FILE_SIZE, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	w.flag = true
	return nil
}

// 偏移位置重置为0
func (w *writer) unmap() error {
	if err := syscall.Munmap(w.data); nil != err {
		return err
	}
	w.flag = false
	w.offset = 0
	return nil
}

func (s *scanner) mmap(queueName string) error {
	fname := fmt.Sprintf("%s_%d.queue", queueName, s.fid)

	f, err := os.OpenFile(fname, os.O_RDWR, 0600)
	if err != nil {
		return err
	} else {
		defer f.Close()
	}

	s.data, err = syscall.Mmap(int(f.Fd()), 0, FILE_SIZE, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	s.flag = true
	return nil
}

func (s *scanner) unmap(queueName string) error {
	fname := fmt.Sprintf("%s_%d.queue", queueName, s.fid)
	if err := syscall.Munmap(s.data); nil != err {
		return err
	}
	if err := os.Remove(fname); err != nil {
		return err
	}
	s.flag = false
	s.offset = 0
	return nil
}

// 队列扫描未确认消息
func (q *queue) scan() ([]byte, error) {
	q.Lock()
	q.LogDebug(fmt.Sprintf("scan.offset:%v read.offset:%v write.offset:%v", q.s.offset, q.r.offset, q.w.offset))

	if !q.s.flag {
		q.s.fid++
		if err := q.s.mmap(q.name); err != nil {
			q.s.fid--
			if os.IsNotExist(err) {
				q.Unlock()
				return nil, errors.New("write has not started.")
			} else {
				q.Unlock()
				return nil, err
			}
		}
	}

	soffset := q.s.offset
	roffset, ok := q.r.rmap[q.s.fid]
	if !ok {
		q.Unlock()
		return nil, errors.New("read has not started.")
	}

	// 当scan.offset == read.offset时,有两种情况
	// 1.已经到文件末尾,可能已有下个文件,则继续下个文件
	// 2.未到文件末尾,说明目前位置读过的数据已经全部得到扫描
	if soffset == roffset {
		if _, ok := q.r.rmap[q.s.fid+1]; ok {
			if err := q.s.unmap(q.name); err != nil {
				q.Unlock()
				return nil, err
			} else {
				delete(q.r.rmap, q.s.fid)
				q.Unlock()
				return q.scan()
			}
		} else {
			q.Unlock()
			return nil, errors.New("no message")
		}
	}

	// 读一条消息
	// 消息结构 flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
	if flag := q.s.data[soffset]; flag != 'v' {
		q.Unlock()
		return nil, errors.New("unkown msg flag")
	}

	status := binary.BigEndian.Uint16(q.s.data[soffset+1 : soffset+3])
	msgLen := int(binary.BigEndian.Uint32(q.s.data[soffset+3 : soffset+7]))

	// 当前的消息已被确认了,需要跳过到下一条消息
	if status == MSG_STATUS_FIN {
		q.s.offset += 1 + 2 + 4 + msgLen
		q.Unlock()
		return q.scan() // 递归调用,注意死锁问题
	}

	// 消息未超时
	expireTime := binary.BigEndian.Uint64(q.s.data[soffset+7 : soffset+15])
	q.LogDebug(fmt.Sprintf("msg.expire:%v now:%v", expireTime, time.Now().Unix()))
	if expireTime > uint64(time.Now().Unix()) {
		q.Unlock()
		return nil, errors.New("no message expire.")
	}

	// 设置消息已超时
	binary.BigEndian.PutUint16(q.s.data[soffset+1:soffset+3], uint16(MSG_STATUS_EXPIRE))
	msg := make([]byte, msgLen)
	copy(msg, q.s.data[soffset+7:soffset+7+msgLen])
	q.s.offset += 1 + 2 + 4 + msgLen

	// 当扫描到文件末尾时,说明文件内消息已被全部得到确认,可解除映射并移除数据文件
	if q.s.offset == roffset {
		if _, ok := q.r.rmap[q.r.fid+1]; ok {
			if err := q.s.unmap(q.name); err != nil {
				q.Unlock()
				return nil, err
			} else {
				delete(q.r.rmap, q.s.fid)
			}
		}
	}

	q.Unlock()
	return msg, nil
}

// 如果要确认消息刚好在映射文件中,则直接操作文件,否则调用file.writeAt写入对应文件
// 几种情况
// 1.消费后立马得到确认(此时应该在读文件,有可能读比确认的快)
// 2.消费后一直没有得到确认(此时应该在扫描文件)
// 3.topic设置为自动确认(不需要处理)
func (q *queue) ack(fid, offset int) error {
	if fid == q.r.fid {
		binary.BigEndian.PutUint16(q.r.data[offset+1:offset+3], MSG_STATUS_FIN)
		return nil
	}
	if fid == q.s.fid {
		binary.BigEndian.PutUint16(q.s.data[offset+1:offset+3], MSG_STATUS_FIN)
		return nil
	}

	fname := fmt.Sprintf("%s_%d.queue", q.name, fid)
	f, err := os.OpenFile(fname, os.O_RDONLY, 0600)
	if err != nil {
		return err
	} else {
		defer f.Close()
	}

	var status []byte
	binary.BigEndian.PutUint16(status, uint16(MSG_STATUS_FIN))
	if _, err := f.WriteAt(status, int64(offset)); err != nil {
		return err
	} else {
		return nil
	}
}

func (r *reader) mmap(queueName string) error {
	fname := fmt.Sprintf("%s_%d.queue", queueName, r.fid)

	f, err := os.OpenFile(fname, os.O_RDWR, 0600)
	if err != nil {
		return err
	} else {
		defer f.Close()
	}

	r.data, err = syscall.Mmap(int(f.Fd()), 0, FILE_SIZE, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	r.flag = true
	return nil
}

// 偏移位置重置为0
func (r *reader) unmap() error {
	if err := syscall.Munmap(r.data); nil != err {
		return err
	}
	r.flag = false
	r.offset = 0
	return nil
}

// 队列读取消息
func (q *queue) read(isAutoAck bool) ([]byte, *MsgIndex, error) {
	q.Lock()
	defer q.Unlock()

	if !q.r.flag {
		q.r.fid++
		if err := q.r.mmap(q.name); err != nil {
			q.r.fid--
			if os.IsNotExist(err) {
				return nil, nil, errors.New("no message")
			} else {
				return nil, nil, err
			}
		}
	}

	fid := q.r.fid
	roffset := q.r.offset
	woffset, ok := q.w.wmap[q.r.fid]
	if !ok {
		return nil, nil, errors.New("no write offset")
	}

	if roffset == woffset {
		_, ok := q.w.wmap[q.r.fid+1]
		// 当woffset等于文件大小,说明woffset已经是文件的末尾
		// 当已存在下一个写文件,说明woffset已经是文件的末尾
		if woffset == FILE_SIZE || ok {
			if err := q.r.unmap(); err != nil {
				return nil, nil, err
			} else {
				delete(q.w.wmap, q.r.fid)
				return q.read(isAutoAck)
			}
		} else {
			return nil, nil, errors.New("no message")
		}
	}

	// 读一条消息
	// 消息结构 flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
	if flag := q.r.data[roffset]; flag != 'v' {
		return nil, nil, errors.New("unkown msg flag")
	}

	// 如果所属的topic是自动确认,则status设置为MSG_STATUS_FIN
	if isAutoAck {
		binary.BigEndian.PutUint16(q.r.data[roffset+1:roffset+3], uint16(MSG_STATUS_FIN))
	} else {
		binary.BigEndian.PutUint16(q.r.data[roffset+1:roffset+3], uint16(MSG_STATUS_WAIT))
		binary.BigEndian.PutUint64(q.r.data[roffset+7:roffset+15], uint64(time.Now().Unix())+uint64(q.ctx.Conf.MsgTTR))
		q.LogDebug(fmt.Sprintf("msg had been readed. exipire time is %v", uint64(time.Now().Unix())+uint64(q.ctx.Conf.MsgTTR)))
	}

	msgLen := int(binary.BigEndian.Uint32(q.r.data[roffset+3 : roffset+7]))
	msg := make([]byte, msgLen)
	copy(msg, q.r.data[roffset+7:roffset+7+msgLen])
	q.r.offset += 1 + 2 + 4 + msgLen
	q.r.rmap[q.r.fid] = q.r.offset
	atomic.AddInt64(&q.num, -1)

	// 当读到文件末尾时,说明文件内消息已被全部读取,可解除映射并移除数据文件
	if q.r.offset == woffset {
		_, ok := q.w.wmap[q.r.fid+1]
		// 当woffset等于文件大小,说明woffset已经是文件的末尾
		// 当已存在下一个写文件,说明woffset已经是文件的末尾
		if woffset == FILE_SIZE || ok {
			if err := q.r.unmap(); err != nil {
				return nil, nil, err
			} else {
				delete(q.w.wmap, q.r.fid)
			}
		}
	}

	return msg, NewMsgIndex(fid, roffset), nil
}

// 新写入信息的长度不能超过文件大小,超过则新建文件
func (q *queue) write(msg []byte) error {
	q.Lock()
	defer q.Unlock()

	woffset := q.w.offset

	if !q.w.flag {
		q.w.fid++
		if err := q.w.mmap(q.name); err != nil {
			q.w.fid--
			return err
		}
	}

	msgLen := len(msg)
	if woffset+1+2+4+msgLen > FILE_SIZE {
		if err := q.w.unmap(); err != nil {
			return err
		}
		q.w.fid++
		if err := q.w.mmap(q.name); err != nil {
			q.w.fid--
			return err
		}
		woffset = q.w.offset
	}

	// package = flag(1-byte) + status(2-bytes) + msgLen(4-bytes) + msg(n-bytes)
	copy(q.w.data[woffset:woffset+1], []byte{'v'})
	binary.BigEndian.PutUint16(q.w.data[woffset+1:woffset+3], uint16(MSG_STATUS_DEFAULT))
	binary.BigEndian.PutUint32(q.w.data[woffset+3:woffset+7], uint32(msgLen))
	copy(q.w.data[woffset+7:woffset+7+msgLen], msg)

	q.w.offset += 1 + 2 + 4 + msgLen
	q.w.wmap[q.w.fid] = q.w.offset
	atomic.AddInt64(&q.num, 1)

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
