package gnode

import (
	"fmt"
	"github.com/wuzhc/gmq/pkg/store"
	"github.com/wuzhc/gmq/pkg/utils"
	"os"
	"runtime"
	"sync"
	"syscall"
)

const (
	dirname       = "data"
	commitLogSize = 1024 * 1024 * 1024
)

type CommitLog struct {
	readOffset  int64
	writeOffset int64
	startOffset int64 // for filename
	buffer      store.MappedByteBuffer
	service     *CommitLogService
	sync.RWMutex
}

func NewCommitLog(startOffset int64, service *CommitLogService) (*CommitLog, error) {
	cl := &CommitLog{
		readOffset:  0,
		writeOffset: 0,
		startOffset: startOffset,
		service:     service,
		buffer:      nil,
	}

	var file *os.File
	var err error
	path := fmt.Sprintf("%s/%020d.log", dirname, startOffset)
	if exist, _ := utils.PathExists(path); !exist {
		file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	} else {
		file, err = os.OpenFile(path, os.O_RDWR, 0600)
	}
	if err != nil {
		return nil, err
	} else {
		defer file.Close()
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	initMmapSize := int(stat.Size())
	if initMmapSize == 0 {
		// new file need to extend size
		if _, err := file.WriteAt([]byte{'0'}, commitLogSize-1); err != nil {
			return nil, err
		}

		if runtime.GOOS != "windows" {
			if err := syscall.Fdatasync(int(file.Fd())); err != nil {
				return nil, err
			}
		} else {
			if err := file.Sync(); err != nil {
				return nil, err
			}
		}

		initMmapSize = commitLogSize
	}

	if err := mmapCommitLog(file, initMmapSize, cl); err != nil {
		return nil, err
	}

	return cl, nil
}

// write message to commitLog file
func (cl *CommitLog) PutMessage(data []byte) {
	cl.Lock()
	defer cl.Unlock()

	SetCommitLogOffset(data, uint64(cl.writeOffset))
	cl.write(data)
}

func (cl *CommitLog) write(data []byte) {
	dataLen := len(data)
	cl.writeOffset = int64(dataLen)
	copy(cl.buffer[cl.writeOffset:cl.writeOffset+int64(dataLen)], data)
}

func (cl *CommitLog) read(readOffset int64, msgSize int) *CommitLogMsg {
	data := cl.buffer[readOffset:msgSize]
	return DecodeCommitLogMsg(data)
}

// dispatch message to consume queue from commitLog file
func (cl *CommitLog) dispatchToConsumeQueue() error {
	for cl.readOffset < cl.writeOffset {
		msgLen, err := cl.buffer.Uint32(int(cl.readOffset))
		if err != nil {
			return err
		}

		// read the whole message by length
		data := cl.buffer[cl.readOffset : cl.readOffset+int64(msgLen)]
		commitLogMsg := DecodeCommitLogMsg(data)
		fmt.Println("eeeeeeeeeeee",commitLogMsg,cl.buffer)
		queueMsg := &ConsumeQueueMsg{
			Topic:     commitLogMsg.Topic,
			QueueId:   int16(commitLogMsg.QueueId),
			LogOffset: commitLogMsg.Offset,
			MsgLen:    commitLogMsg.MsgLen,
			Tag:       commitLogMsg.Tag,
		}
		consumeQueue := cl.service.broker.getTopicQueues(string(commitLogMsg.Topic))
		if err := consumeQueue.putMessage(queueMsg); err != nil {
			return err
		}
	}

	return nil
}

