package gnode

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

const (
	RESP_MESSAGE = 101
	RESP_ERROR   = 102
	RESP_RESULT  = 103
)

type TcpConn struct {
	conn     net.Conn
	serv     *TcpServ
	wg       utils.WaitGroupWrapper
	exitChan chan struct{}
	once     sync.Once
	writer   *bufio.Writer
	reader   *bufio.Reader
}

// <cmd_name> <param_1> ... <param_n>\n
func (c *TcpConn) Handle() {
	defer c.LogInfo("tcp connection handle exit.")

	go func() {
		select {
		case <-c.serv.exitChan:
			c.conn.Close()
		case <-c.exitChan:
			return
		}
	}()

	var buf bytes.Buffer
	for {
		var err error
		var t time.Time
		c.conn.SetDeadline(t)

		line, isPrefix, err := c.reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				c.LogWarn("closed.")
			} else {
				c.LogError(err)
			}
			break
		}
		if len(line) == 0 {
			c.LogError("cmd is empty")
			break
		}
		buf.Write(line)
		if isPrefix { // conn.buffer is full,but we don't get '\n',continue to read
			continue
		}

		params := bytes.Split(buf.Bytes(), []byte(" "))
		buf.Reset() // reset buf after reading
		if len(params) < 2 {
			c.LogError("params muset be greater than 2")
			break
		}

		cmd := params[0]
		params = params[1:]
		err = nil

		switch {
		case bytes.Equal(cmd, []byte("pub")):
			err = c.PUB(params)
		case bytes.Equal(cmd, []byte("pop")):
			err = c.POP(params)
		case bytes.Equal(cmd, []byte("ack")):
			err = c.ACK(params)
		case bytes.Equal(cmd, []byte("mpub")):
			err = c.MPUB(params)
		case bytes.Equal(cmd, []byte("dead")):
			err = c.DEAD(params)
		case bytes.Equal(cmd, []byte("set")):
			err = c.SET(params)
		default:
			c.LogError(fmt.Sprintf("unkown cmd: %s", cmd))
		}

		if err != nil {
			c.RespErr(err)
			c.LogError(err)
			break
		}
	}

	// force close conn
	c.conn.Close()
	close(c.exitChan)
}

// pub <topic_name> <delay-time>
// [ 4-byte size in bytes ][ N-byte binary data ]
func (c *TcpConn) PUB(params [][]byte) error {
	var err error

	if len(params) != 2 {
		c.LogError(params)
		return errors.New("pub params is error")
	}

	topic := string(params[0])
	delay, _ := strconv.Atoi(string(params[1]))
	if delay > MSG_MAX_DELAY {
		return errors.New("pub.delay exceeding the maximum")
	}

	bodylenBuf := make([]byte, 4)
	_, err = io.ReadFull(c.reader, bodylenBuf)
	if err != nil {
		return errors.New(fmt.Sprintf("read bodylen failed, %v", err))
	}

	bodylen := int(binary.BigEndian.Uint32(bodylenBuf))
	body := make([]byte, bodylen)
	_, err = io.ReadFull(c.reader, body)
	if err != nil {
		return errors.New(fmt.Sprintf("read body failed, %v", err))
	}

	cb := make([]byte, len(body))
	copy(cb, body)

	if msgId, err := c.serv.ctx.Dispatcher.push(topic, cb, delay); err != nil {
		c.LogError(err)
		c.RespErr(err)
	} else {
		c.RespRes(strconv.FormatInt(int64(msgId), 10))
	}

	return nil
}

// pub <topic_name> <num>
// <delay-time>[ 4-byte size in bytes ][ N-byte binary data ]
// <delay-time>[ 4-byte size in bytes ][ N-byte binary data ]
func (c *TcpConn) MPUB(params [][]byte) error {
	var err error

	if len(params) != 2 {
		c.LogError(params)
		return errors.New("pub params is error")
	}

	topic := string(params[0])
	num, _ := strconv.Atoi(string(params[1]))
	if num <= 0 {
		return errors.New("num must be greather than 0")
	}

	delays := make([]int, num, num)
	msgs := make([][]byte, num, num)
	defer func() {
		delays = nil
		msgs = nil
	}()

	for i := 0; i < num; i++ {
		delayBuf := make([]byte, 4)
		_, err = io.ReadFull(c.reader, delayBuf)
		if err != nil {
			return errors.New(fmt.Sprintf("read delay failed, %v", err))
		}

		bodylenBuf := make([]byte, 4)
		_, err = io.ReadFull(c.reader, bodylenBuf)
		if err != nil {
			return errors.New(fmt.Sprintf("read bodylen failed, %v", err))
		}

		bodylen := int(binary.BigEndian.Uint32(bodylenBuf))
		body := make([]byte, bodylen)
		_, err = io.ReadFull(c.reader, body)
		if err != nil {
			return errors.New(fmt.Sprintf("read body failed, %v", err))
		}

		delays[i] = int(binary.BigEndian.Uint32(delayBuf))
		msgs[i] = body
	}

	if _, err := c.serv.ctx.Dispatcher.mpush(topic, msgs, delays); err != nil {
		c.LogError(err)
		c.RespErr(err)
	} else {
		c.RespRes("ok")
	}

	return nil
}

// pop <topic_name>
func (c *TcpConn) POP(params [][]byte) error {
	if len(params) != 1 {
		return errors.New("pop params is error")
	}

	topic := string(params[0])
	msg, err := c.serv.ctx.Dispatcher.pop(topic)
	defer func() {
		msg = nil
	}()

	if err != nil {
		c.RespErr(err)
		return nil
	}

	// if topic.isAutoAck is false, add to waiting queue
	t := c.serv.ctx.Dispatcher.GetTopic(topic)
	if !t.isAutoAck {
		msg.Retry += 1
		msg.Delay = uint32(msg.Retry) * MSG_DELAY_INTERVAL
		msg.Expire = int64(msg.Delay) + time.Now().Unix()
		if msg.Retry > MSG_MAX_RETRY {
			t.LogWarn("noauto ack to failure bucket")
			if err := t.pushMsgToDeadBucket(msg); err != nil {
				c.RespErr(err)
				return nil
			}
		} else {
			if err := t.pushMsgToBucket(msg); err != nil {
				c.RespErr(err)
				return nil
			}

			t.waitAckMux.Lock()
			t.waitAckMap[msg.Id] = msg.Expire
			t.waitAckMux.Unlock()
		}
	}

	c.RespMsg(msg)
	return nil
}

// ack <message_id>
func (c *TcpConn) ACK(params [][]byte) error {
	if len(params) != 2 {
		return errors.New("ack params is error")
	}

	msgId, _ := strconv.ParseInt(string(params[0]), 10, 64)
	topic := string(params[1])

	if err := c.serv.ctx.Dispatcher.ack(topic, uint64(msgId)); err != nil {
		c.RespErr(err)
		return nil
	}

	c.RespRes("ok")
	return nil
}

// 消费死信
// dead <topic_name> <message_number>
func (c *TcpConn) DEAD(params [][]byte) error {
	if len(params) != 2 {
		return errors.New("dead params is error")
	}

	topic := string(params[0])
	num := params[1]
	n, _ := strconv.Atoi(string(num))
	msgs, err := c.serv.ctx.Dispatcher.dead(topic, n)
	if err != nil {
		c.RespErr(err)
		return nil
	}
	if len(msgs) == 0 {
		c.RespErr(errors.New("no message"))
		return nil
	}

	c.RespMsgs(msgs)
	return nil
}

// 设置topic信息,目前只有isAutoAck选项
// set <topic> <isAutoAck>
func (c *TcpConn) SET(params [][]byte) error {
	if len(params) != 2 {
		return errors.New("ack params is error")
	}

	topic := string(params[0])
	isAutoAck, _ := strconv.Atoi(string(params[1]))

	if err := c.serv.ctx.Dispatcher.set(topic, isAutoAck); err != nil {
		c.RespErr(err)
		return nil
	}

	c.RespRes("ok")
	return nil
}

func (c *TcpConn) Response(respType int16, respData []byte) {
	var err error

	// write response type
	respTypeBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(respTypeBuf, uint16(respType))
	_, err = c.writer.Write(respTypeBuf)
	if err != nil {
		c.LogError(err)
		return
	}

	// write data length
	dataLenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(dataLenBuf, uint32(len(respData)))
	_, err = c.writer.Write(dataLenBuf)
	if err != nil {
		c.LogError(err)
		return
	}

	// write data
	_, err = c.writer.Write(respData)
	if err != nil {
		c.LogError(err)
		return
	}

	c.writer.Flush()
}

func (c *TcpConn) RespMsg(msg *Msg) bool {
	msgData := RespMsgData{}
	msgData.Body = string(msg.Body)
	msgData.Retry = msg.Retry
	msgData.Id = msg.Id

	data, err := json.Marshal(msgData)
	if err != nil {
		c.LogError(err)
		return false
	}

	c.Response(RESP_MESSAGE, data)
	return true
}

func (c *TcpConn) RespMsgs(msgs []*Msg) bool {
	var v []RespMsgData
	for _, msg := range msgs {
		msgData := RespMsgData{}
		msgData.Body = string(msg.Body)
		msgData.Retry = msg.Retry
		msgData.Id = msg.Id
		v = append(v, msgData)
		msg = nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		c.LogError(err)
		return false
	}

	c.Response(RESP_MESSAGE, data)
	return true
}

func (c *TcpConn) RespErr(err error) bool {
	c.Response(RESP_ERROR, []byte(err.Error()))
	return false
}

func (c *TcpConn) RespRes(msg string) bool {
	c.Response(RESP_RESULT, []byte(msg))
	return true
}

func (c *TcpConn) LogError(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory(c.conn.RemoteAddr().String()))
	v = append(v, msg...)
	c.serv.ctx.Logger.Error(v...)
}

func (c *TcpConn) LogWarn(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory(c.conn.RemoteAddr().String()))
	v = append(v, msg...)
	c.serv.ctx.Logger.Warn(v...)
}

func (c *TcpConn) LogInfo(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory(c.conn.RemoteAddr().String()))
	v = append(v, msg...)
	c.serv.ctx.Logger.Info(v...)
}
