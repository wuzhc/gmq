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
	conns       map[*TcpConn]bool
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
		conns:       make(map[*TcpConn]bool),
		pushMsgChan: make(chan []byte),
	}
	ch.wg.Wrap(ch.distribute)
	return ch
}

// exit channel
func (c *Channel) exit() {
	close(c.exitChan)
	c.wg.Wait()
}

// add connection to channel
func (c *Channel) addConn(tcpConn *TcpConn) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.conns[tcpConn]; ok {
		return fmt.Errorf("client %s had connection.", tcpConn.conn.LocalAddr().String())
	}

	start := make(chan struct{})
	c.wg.Wrap(func() {
		start <- struct{}{}
		select {
		case <-tcpConn.exitChan:
			// delete connection on close
			c.LogInfo(fmt.Sprintf("connection %s has exit.", tcpConn.conn.RemoteAddr()))
			delete(c.conns, tcpConn)

			// exit channel when the number of connections is zero
			if len(c.conns) == 0 {
				c.exit()
			}
			return
		case <-c.exitChan:
			return
		}
	})

	// wait for goroutine to start
	<-start
	c.conns[tcpConn] = true

	return nil
}

// publish message to all connections
func (c *Channel) publish(msg []byte) error {
	c.RLock()
	defer c.RUnlock()

	if len(c.conns) == 0 {
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
			c.LogInfo(fmt.Sprintf("channel %s has distribute.", c.key))
			return
		case msg := <-c.pushMsgChan:
			c.RLock()
			for tcpConn, _ := range c.conns {
				tcpConn.Response(RESP_CHANNEL, msg)
			}
			c.RUnlock()
		}
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
