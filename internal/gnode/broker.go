// broker负责：
// commitLog写入
// 延迟消息写入
// 定时将延迟消息写入到commitLog
// 定时将commitLog写入到messageQueue
// 维护topic列表
package gnode

import (
	"fmt"
	bolt "github.com/wuzhc/bbolt"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
	"os"
	"sync"
)

type Broker struct {
	globalCtx                 *Context
	consumeOffsetStoreService *ConsumeOffsetStoreService

	topicQueues         map[string]*ConsumeQueues
	consumeGroupService *ConsumeGroupService
	commitLogService    *CommitLogService
	grpcService         *GrpcService
	waitGroup           utils.WaitGroupWrapper
	snowflake           *utils.Snowflake
	boltDB              *bolt.DB
	exitChan            chan struct{}
	sync.RWMutex
}

func NewBroker(globalCtx *Context) *Broker {
	broker := &Broker{
		globalCtx:   globalCtx,
		topicQueues: make(map[string]*ConsumeQueues),
		exitChan:    make(chan struct{}),
	}

	var err error
	sn, err := utils.NewSnowflake(1)
	if err != nil {
		panic(err)
	}

	broker.snowflake = sn
	broker.grpcService = NewGrpcService(globalCtx.Logger, broker, globalCtx.Conf)
	broker.commitLogService = NewCommitLogService(globalCtx, broker)
	broker.consumeGroupService = NewConsumerGroupService()
	broker.consumeOffsetStoreService = NewConsumeOffsetStoreService(globalCtx.Logger)

	dbFile := fmt.Sprintf("%s/gmq.db", globalCtx.Conf.DataSavePath)
	broker.boltDB, err = bolt.Open(dbFile, 0600, nil)
	if err != nil {
		broker.LogError(err.Error())
		os.Exit(1)
	}

	return broker
}

func (b *Broker) Start() {
	defer b.Stop()

	b.LogInfo("start broker.")
	b.waitGroup.Wrap(b.grpcService.startServ)
	b.waitGroup.Wrap(b.commitLogService.dispatch)
	b.waitGroup.Wrap(b.consumeGroupService.keepAlive)
	b.waitGroup.Wrap(b.consumeOffsetStoreService.syncToDisk)

	select {
	case <-b.globalCtx.Gnode.exitChan:
		return
	}
}

func (b *Broker) Stop() {
	close(b.exitChan)
	b.grpcService.stop()
	b.commitLogService.stop()
	b.consumeGroupService.stop()
	b.consumeOffsetStoreService.stop()
	b.waitGroup.Wait()
	b.LogInfo("end broker.")
}

func (b *Broker) generateMsgId() uint64 {
	return b.snowflake.Generate()
}

func (b *Broker) registerConsumerToGroup(topicName, groupName, consumerAddr string) error {
	name := fmt.Sprintf("%s@%s", topicName, groupName)
	return b.consumeGroupService.addConsumer(name, consumerAddr)
}

func (b *Broker) unregisterConsumerToGroup(topicName, groupName, consumerAddr string) error {
	name := fmt.Sprintf("%s@%s", topicName, groupName)
	return b.consumeGroupService.removeConsumer(name)
}

func (b *Broker) getTopicQueues(topicName string) *ConsumeQueues {
	if queues, ok := b.topicQueues[topicName]; ok {
		return queues
	}

	b.topicQueues[topicName] = NewConsumeQueues(topicName, b)
	return b.topicQueues[topicName]
}

func (b *Broker) LogError(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Broker"))
	v = append(v, msg...)
	b.globalCtx.Logger.Error(v...)
}

func (b *Broker) LogWarn(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Broker"))
	v = append(v, msg...)
	b.globalCtx.Logger.Warn(v...)
}

func (b *Broker) LogInfo(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Broker"))
	v = append(v, msg...)
	b.globalCtx.Logger.Info(v...)
}

func (b *Broker) LogDebug(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Broker"))
	v = append(v, msg...)
	b.globalCtx.Logger.Debug(v...)
}
