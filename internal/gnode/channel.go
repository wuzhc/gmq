package gnode

import (
	"fmt"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
	"sync"
	"time"
)

type Channel struct {
	key         string
	clients     map[*ChannelClient]bool
	exitChan    chan struct{}
	pushMsgChan chan []byte
	ctx         *Context
	wg          utils.WaitGroupWrapper
	sync.RWMutex
}

func NewChannel(key string, ctx *Context) *Channel {
	ch := &Channel{
		key:         key,
		ctx:         ctx,
		exitChan:    make(chan struct{}),
		pushMsgChan: make(chan []byte),
		clients:     make(map[*ChannelClient]bool),
	}
	ch.wg.Wrap(ch.distribute)
	return ch
}

// exit channel
func (c *Channel) exit() {
	c.ctx.Dispatcher.RemoveChannel(c.key)
	close(c.exitChan)
	c.wg.Wait()
}

// publish message to all connections
func (c *Channel) publish(msg []byte) error {
	c.RLock()
	defer c.RUnlock()

	if len(c.clients) == 0 {
		return fmt.Errorf("no subscribers")
	}

	select {
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout.")
	case c.pushMsgChan <- msg:
		return nil
	}
}

func (c *Channel) distribute() {
	for {
		select {
		case <-c.exitChan:
			c.LogInfo(fmt.Sprintf("channel %s has exit distribute.", c.key))
			return
		case msg := <-c.pushMsgChan:
			for client, _ := range c.clients {
				client.readChan <- msg
			}
		}
	}
}

func (c *Channel) AddClient(client *ChannelClient) {
	c.clients[client] = true
}

func (c *Channel) removeClient(client *ChannelClient) {
	if _, ok := c.clients[client]; ok {
		delete(c.clients, client)
	}
}

type ChannelClient struct {
	readChan chan []byte
}

func NewChannelClient() *ChannelClient {
	return &ChannelClient{
		readChan: make(chan []byte),
	}
}

func (c *Channel) LogError(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Channel_"+c.key))
	v = append(v, msg...)
	c.ctx.Logger.Error(v...)
}

func (c *Channel) LogWarn(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Channel_"+c.key))
	v = append(v, msg...)
	c.ctx.Logger.Warn(v...)
}

func (c *Channel) LogInfo(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Channel_"+c.key))
	v = append(v, msg...)
	c.ctx.Logger.Info(v...)
}

func (c *Channel) logDebug(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("Channel_"+c.key))
	v = append(v, msg...)
	c.ctx.Logger.Debug(v...)
}
