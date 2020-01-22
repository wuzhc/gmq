// 消费逻辑队列
// 复制
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

const queueSize = 1024 * 1024 * 10

type ConsumeQueues struct {
	broker *Broker
	topic  string
	queues map[string]*ConsumeQueue // topic@queueId
}

func NewConsumeQueues(topicName string, broker *Broker) *ConsumeQueues {
	return &ConsumeQueues{
		broker: broker,
		topic:  topicName,
		queues: make(map[string]*ConsumeQueue),
	}
}

func (cqs *ConsumeQueues) putMessage(msg *ConsumeQueueMsg) error {
	key := string(msg.Topic) + "@" + string(msg.QueueId)
	fmt.Println("vvvvvvvvv", string(msg.Topic), string(msg.QueueId))
	if _, ok := cqs.queues[key]; !ok {
		cqs.queues[key] = NewConsumeQueue(string(msg.Topic), int(msg.QueueId), cqs.broker)
	}

	return cqs.queues[key].putMessage(msg)
}

func (cqs *ConsumeQueues) pullMessage(topic string, queueId int, readOffset int64) (*CommitLogMsg, error) {
	key := topic + "@" + string(queueId)
	if _, ok := cqs.queues[key]; !ok {
		return nil, fmt.Errorf("queue isn't exist.")
	}

	msg := cqs.queues[key].pullMessage(readOffset)
	return msg, nil
}

type queueFile struct {
	readOffset  int64
	writeOffset int64
	beginOffset int64 // for filename
	buffer      store.MappedByteBuffer
	broker      *Broker
}

func NewQueueFile(beginOffset int64, topic string, queueId int, broker *Broker) (*queueFile, error) {
	queueFile := &queueFile{
		readOffset:  0,
		writeOffset: 0,
		beginOffset: beginOffset,
		buffer:      nil,
		broker:      broker,
	}

	var file *os.File
	var err error
	fmt.Println("-------", dirname, topic, queueId, beginOffset)
	path := fmt.Sprintf("%s/%s/%s/%020d.log", dirname, topic, queueId, beginOffset)
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
		if _, err := file.WriteAt([]byte{'0'}, queueSize-1); err != nil {
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

		initMmapSize = queueSize
	}

	if err := mmapQueueFile(file, initMmapSize, queueFile); err != nil {
		return nil, err
	}

	return queueFile, nil
}

func (queueFile *queueFile) write(msg *ConsumeQueueMsg) {
	data := EncodeConsumeQueueMsg(msg)
	SetCommitLogOffset(data, uint64(queueFile.writeOffset))
	dataLen := len(data)
	queueFile.writeOffset = int64(dataLen)
	copy(queueFile.buffer[queueFile.writeOffset:queueFile.writeOffset+int64(dataLen)], data)
}

func (queueFile *queueFile) read(readOffset int64) *CommitLogMsg {
	queueMsg := DecodeConsumeQueueMsg(queueFile.buffer[readOffset : readOffset+28])
	return queueFile.broker.commitLogService.read(queueMsg.LogOffset, int(queueMsg.MsgLen))
}

type ConsumeQueue struct {
	topic         string
	queueId       int
	queueFiles    map[int64]*queueFile
	lastQueueFile *queueFile
	broker        *Broker
	sync.RWMutex
}

func NewConsumeQueue(topic string, queueId int, broker *Broker) *ConsumeQueue {
	return &ConsumeQueue{
		topic:      topic,
		queueId:    queueId,
		broker:     broker,
		queueFiles: make(map[int64]*queueFile),
	}
}

// put message
func (queue *ConsumeQueue) putMessage(msg *ConsumeQueueMsg) error {
	queueFile, err := queue.getLastQueueFile()
	if err != nil {
		return err
	}

	data := EncodeConsumeQueueMsg(msg)
	nextBeginOffset := queueFile.writeOffset + int64(len(data))
	// insufficient space for storage, create an new commitLog file
	if nextBeginOffset > queueSize {
		queueFile, err = queue.createQueueFile(nextBeginOffset)
		if err != nil {
			return err
		}
	}

	queueFile.write(msg)
	return nil
}

// 要怎么知道readOffset是落在哪个队列文件
func (queue *ConsumeQueue) pullMessage(readOffset int64) *CommitLogMsg {
	var tempOffset int64
	for queueBeginOffset, _ := range queue.queueFiles {
		if readOffset > queueBeginOffset && tempOffset > queueBeginOffset {
			tempOffset = queueBeginOffset
		}
	}

	queueFile := queue.queueFiles[tempOffset]
	return queueFile.read(readOffset)
}

func (queue *ConsumeQueue) getLastQueueFile() (*queueFile, error) {
	if queue.lastQueueFile == nil {
		return queue.createQueueFile(0)
	}

	return queue.lastQueueFile, nil
}

func (queue *ConsumeQueue) createQueueFile(beginOffset int64) (*queueFile, error) {
	queue.Lock()
	defer queue.Unlock()

	if _, ok := queue.queueFiles[beginOffset]; ok {
		return queue.queueFiles[beginOffset], nil
	}

	queueFile, err := NewQueueFile(beginOffset, queue.topic, queue.queueId, queue.broker)
	if err != nil {
		return nil, err
	}

	queue.lastQueueFile = queueFile
	queue.queueFiles[beginOffset] = queueFile
	return queueFile, nil
}
