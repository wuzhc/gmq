// 初始化:
// 	- wfid为0,woffset为0
//	- rfid为0,roffset为0
// 	- 初始化一个totalSize大小的文件,内容为0,totalSzie大小为pageSize的整数倍

// 写入:
// 	- 维护一个wfid和offset的map表
//	- wfid为0,初始化一个totalSize大小的文件,内容为0,totalSzie大小为pageSize的整数倍,执行映射,wfid加1
//	- 根据woffset,写入内容,更新woffset

// 读取:
// 	- rfid为0,rfid加1,查看文件是否存在,存在则映射
// 	- 根据roffset和woffset读取内容,更新roffset
//	- 读取完毕,删除数据文件,删除写入的map表记录
package gnode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
)

// const FILE_SIZE = 2 << 32 // 4G
// const FILE_SIZE = 209715200
const FILE_SIZE = 209715200

type queue struct {
	w    *writer
	r    *reader
	name string
	sync.RWMutex
}

type writer struct {
	fid    int
	offset int
	data   []byte
	wmap   map[int]int
	flag   bool
}

type reader struct {
	fid    int
	offset int
	data   []byte
	flag   bool
}

func NewQueue(name string) *queue {
	return &queue{
		name: name,
		w:    &writer{wmap: make(map[int]int)},
		r:    &reader{},
	}
}

func (w *writer) mmap(queueName string) error {
	w.fid += 1
	fname := fmt.Sprintf("%s_%d.log", queueName, w.fid)

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

func (r *reader) mmap(queueName string) error {
	r.fid += 1
	fname := fmt.Sprintf("%s_%d.log", queueName, r.fid)

	f, err := os.OpenFile(fname, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}

	r.data, err = syscall.Mmap(int(f.Fd()), 0, FILE_SIZE, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	r.flag = true
	return nil
}

// 偏移位置重置为0
func (r *reader) unmap(queueName string) error {
	fname := fmt.Sprintf("%s_%d.log", queueName, r.fid)
	if err := syscall.Munmap(r.data); nil != err {
		return err
	}
	if err := os.Remove(fname); err != nil {
		return err
	}
	r.flag = false
	r.offset = 0
	return nil
}

func (q *queue) read() (int64, []byte, error) {
	q.Lock()
	defer q.Unlock()

	if !q.r.flag {
		if err := q.r.mmap(q.name); err != nil {
			if os.IsNotExist(err) {
				return 0, nil, errors.New("no message")
			} else {
				return 0, nil, err
			}
		}
	}

	roffset := q.r.offset
	woffset, ok := q.w.wmap[q.r.fid]
	if !ok {
		return 0, nil, errors.New("no write offset")
	}

	fmt.Println("read ---:", "woffset:", woffset, "roffset:", q.r.offset)
	if roffset == woffset {
		_, ok := q.w.wmap[q.r.fid+1]
		// 当woffset等于文件大小,说明woffset已经是文件的末尾
		// 当已存在下一个写文件,说明woffset已经是文件的末尾
		if woffset == FILE_SIZE || ok {
			if err := q.r.unmap(q.name); err != nil {
				return 0, nil, err
			} else {
				delete(q.w.wmap, q.r.fid)
				return q.read()
			}
		} else {
			return 0, nil, errors.New("no message")
		}
	}

	// 读一条消息
	// 消息结构 flag+msgId+msg_len+msg
	if flag := q.r.data[roffset]; flag != 'v' {
		return 0, nil, errors.New("unkown msg flag")
	}

	msgId := int64(binary.BigEndian.Uint64(q.r.data[roffset+1 : roffset+9]))
	msgLen := int(binary.BigEndian.Uint32(q.r.data[roffset+9 : roffset+13]))
	msg := make([]byte, msgLen)
	copy(msg, q.r.data[roffset+13:roffset+13+msgLen])
	q.r.offset += 1 + 8 + 4 + msgLen

	// 当读到文件末尾时,说明文件内消息已被全部读取,可解除映射并移除数据文件
	if q.r.offset == woffset {
		_, ok := q.w.wmap[q.r.fid+1]
		// 当woffset等于文件大小,说明woffset已经是文件的末尾
		// 当已存在下一个写文件,说明woffset已经是文件的末尾
		if woffset == FILE_SIZE || ok {
			if err := q.r.unmap(q.name); err != nil {
				return 0, nil, err
			} else {
				delete(q.w.wmap, q.r.fid)
			}
		}
	}

	return msgId, msg, nil
}

// 新写入信息的长度不能超过文件大小,超过则新建文件
func (q *queue) write(id int64, msg []byte) error {
	q.Lock()
	defer q.Unlock()

	woffset := q.w.offset

	if !q.w.flag {
		if err := q.w.mmap(q.name); err != nil {
			return err
		}
	}

	msgLen := len(msg)
	fmt.Println("b -----", woffset+1+8+4+msgLen, q.w.wmap)

	if woffset+1+8+4+msgLen > FILE_SIZE {
		if err := q.w.unmap(); err != nil {
			return err
		}
		if err := q.w.mmap(q.name); err != nil {
			return err
		}
		woffset = q.w.offset
	}

	// msg = flag + msg.id + msg.len + msg.content
	copy(q.w.data[woffset:woffset+1], []byte{'v'})
	binary.BigEndian.PutUint64(q.w.data[woffset+1:woffset+9], uint64(id))
	binary.BigEndian.PutUint32(q.w.data[woffset+9:woffset+13], uint32(msgLen))
	copy(q.w.data[woffset+13:woffset+13+msgLen], msg)

	q.w.offset += 1 + 8 + 4 + msgLen
	fmt.Println("a -----", q.w.offset, q.w.wmap)
	q.w.wmap[q.w.fid] = q.w.offset

	// fmt.Println("write ---:", "woffset:", woffset, "roffset:", q.r.offset, "wmap:", q.w.wmap)

	return nil
}
