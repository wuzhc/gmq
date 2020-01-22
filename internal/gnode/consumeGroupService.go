// 消费组
// 定时检测组内消费者是否存活
package gnode

import (
	"fmt"
	"time"
)

type ConsumeGroup struct {
	name      string // topic_name + @ + group_name
	filterTag string
	consumers map[string]bool
}

func NewConsumeGroup(groupName, filterTag string) *ConsumeGroup {
	return &ConsumeGroup{
		name:      groupName,
		filterTag: filterTag,
		consumers: make(map[string]bool),
	}
}

type ConsumeGroupService struct {
	groups   map[string]*ConsumeGroup
	exitChan chan struct{}
}

func NewConsumerGroupService() *ConsumeGroupService {
	return &ConsumeGroupService{
		groups:   make(map[string]*ConsumeGroup),
		exitChan: make(chan struct{}),
	}
}

func (service *ConsumeGroupService) stop() {
	close(service.exitChan)
}

func (service *ConsumeGroupService) addGroup(groupName, filterTag string) {
	service.groups[groupName] = NewConsumeGroup(groupName, filterTag)
}

// add consumer
func (service *ConsumeGroupService) addConsumer(key, addr string) error {
	if consumeGroup, ok := service.groups[key]; ok {
		consumeGroup.addConsumer(addr)
		return nil
	}
	return fmt.Errorf("unkown group %s", key)
}

// remove consumer when the connection is disable
func (service *ConsumeGroupService) removeConsumer(key string) error {
	if _, ok := service.groups[key]; ok {
		delete(service.groups, key)
		return nil
	}
	return fmt.Errorf("group %s isn't exist", key)
}

func (service *ConsumeGroupService) keepAlive() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			for _, consumeGroup := range service.groups {
				consumeGroup.keepAlive()
			}
		case <-service.exitChan:
			ticker.Stop()
			return
		}
	}
}

func (cg *ConsumeGroup) addConsumer(addr string) {
	if _, ok := cg.consumers[addr]; ok {
		return
	}
	cg.consumers[addr] = true
}

func (cg *ConsumeGroup) removeConsumer(addr string) {
	if _, ok := cg.consumers[addr]; !ok {
		return
	}
	delete(cg.consumers, addr)
}

func (cg *ConsumeGroup) keepAlive() {
	for consumer, _ := range cg.consumers {
		// send heartbreat
		if len(consumer) == 0 {
			// remove disconnected consumer
			delete(cg.consumers, consumer)
		}
	}
}
