// 消费进度保存
// 每个topic下，每个queue的消费进度
// 每隔5秒，持久化到磁盘
package gnode

import (
	"encoding/json"
	"github.com/wuzhc/gmq/pkg/logs"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

type ConsumeOffsetSchema struct {
	Data map[string]map[int]int64
}

type QueueOffsetStore struct {
	queueId int
	offset  int64
}

func NewQueueOffsetStore(queueId int, offset int64) *QueueOffsetStore {
	return &QueueOffsetStore{
		queueId: queueId,
		offset:  offset,
	}
}

type ConsumeOffsetStoreService struct {
	filename string
	topics   map[string][]*QueueOffsetStore
	logger   *logs.Dispatcher
	exitChan chan struct{}
	sync.RWMutex
}

func NewConsumeOffsetStoreService(logger *logs.Dispatcher) *ConsumeOffsetStoreService {
	return &ConsumeOffsetStoreService{
		filename: "consume_offset_store.json",
		logger:   logger,
		topics:   make(map[string][]*QueueOffsetStore),
		exitChan: make(chan struct{}),
	}
}

func (service *ConsumeOffsetStoreService) stop() {
	close(service.exitChan)
}

func (service *ConsumeOffsetStoreService) updateConsumeOffset(topic string, queueId int, offset int64) {
	service.Lock()
	defer service.Unlock()

	topicStore, ok := service.topics[topic]
	if !ok {
		service.topics[topic] = append(service.topics[topic], NewQueueOffsetStore(queueId, offset))
		return
	}

	for k, _ := range topicStore {
		if topicStore[k].queueId == queueId {
			if offset < topicStore[k].offset {
				service.logger.Error("Invaild offset.")
			} else {
				topicStore[k].offset = offset
			}
			return
		}
	}
}

// load consume offset from disk
func (service *ConsumeOffsetStoreService) load() error {
	file, err := os.OpenFile(service.filename, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	nbyte, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	var data ConsumeOffsetSchema
	if err := json.Unmarshal(nbyte, data); err != nil {
		return err
	}

	service.topics = nil
	for topic, queues := range data.Data {
		service.topics[topic] = make([]*QueueOffsetStore, len(queues))
		for queueId, offset := range queues {
			service.topics[topic] = append(service.topics[topic], NewQueueOffsetStore(queueId, offset))
		}
	}

	return nil
}

// persist consume offset to disk
func (service *ConsumeOffsetStoreService) persist() error {
	file, err := os.OpenFile(service.filename, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	var data ConsumeOffsetSchema
	for topic, queues := range service.topics {
		for k, _ := range queues {
			if _, ok := data.Data[topic]; !ok {
				data.Data[topic] = make(map[int]int64)
			}
			data.Data[topic][queues[k].queueId] = queues[k].offset
		}
	}

	nbyte, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if _, err = file.Write(nbyte); err != nil {
		return err
	}
	return file.Sync()
}

func (service *ConsumeOffsetStoreService) syncToDisk() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			if err := service.persist(); err != nil {
				service.logger.Error(err.Error())
			}
		case <-service.exitChan:
			ticker.Stop()
			return
		}
	}
}
