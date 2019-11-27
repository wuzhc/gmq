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
// 涉及到两种错误
// 一种是协议解析出错,在一个命令出错之后,连接可能存在数据未读取完毕,
// 例如推送pub由两个命令行组成,当第一个命令行解析出错时,第二个命令行
// 还未读取,此时应该由server主动关闭连接
// 一种是协议解析完成,但是执行业务的时候失败,此时server不需要断开连接,
// 它可以正常执行下个命令,所以应该由客户端自己决定是否关闭连接
func (c *TcpConn) Handle() {
	defer c.LogInfo("tcp connection handle exit.")

	// todo make be error
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
		case bytes.Equal(cmd, []byte("queue")):
			err = c.DECLAREQUEUE(params) // declare queue
		default:
			err = errors.New(fmt.Sprintf("unkown cmd: %s", cmd))
		}

		if err != nil {
			c.RespErr(err)
			c.LogError(err)
			break
		}
	}

	// force close conn
	time.Sleep(2 * time.Second)
	c.conn.Close()
	close(c.exitChan)
}

// pub <topic_name> <route_key> <delay-time>
// [ 4-byte size in bytes ][ N-byte binary data ]
func (c *TcpConn) PUB(params [][]byte) error {
	if len(params) != 3 {
		return errors.New("pub.params is error")
	}

	topic := string(params[0])
	routeKey := string(params[1])
	delay, _ := strconv.Atoi(string(params[2]))
	if delay > MSG_MAX_DELAY {
		return errors.New("pub.delay exceeding the maximum")
	}

	bodylenBuf := make([]byte, 4)
	_, err := io.ReadFull(c.reader, bodylenBuf)
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

	msgId, err := c.serv.ctx.Dispatcher.push(topic, routeKey, cb, delay)
	if err != nil {
		c.RespErr(err)
	} else {
		c.RespRes(strconv.FormatUint(msgId, 10))
	}

	return nil
}

// pub <topic_name> <num>
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
func (c *TcpConn) MPUB(params [][]byte) error {
	var err error

	if len(params) != 2 {
		return errors.New("pub params is error")
	}

	topic := string(params[0])
	num, _ := strconv.Atoi(string(params[1]))
	if num <= 0 || num > c.serv.ctx.Conf.MsgMaxPushNum {
		return errors.New(fmt.Sprintf("number of push must be between 1 and %v", c.serv.ctx.Conf.MsgMaxPushNum))
	}

	msgIds := make([]uint64, num)
	for i := 0; i < num; i++ {
		msglenBuf := make([]byte, 4)
		_, err = io.ReadFull(c.reader, msglenBuf)
		if err != nil {
			return fmt.Errorf("read msg.length failed, %v", err)
		}

		msglen := int(binary.BigEndian.Uint32(msglenBuf))
		msg := make([]byte, msglen)
		_, err = io.ReadFull(c.reader, msg)
		if err != nil {
			return fmt.Errorf("read body failed, %v", err)
		}

		var recvMsg RecvMsgData
		if err := json.Unmarshal(msg, &recvMsg); err != nil {
			c.RespErr(fmt.Errorf("decode msg failed, %s", err))
		}

		msgId, err := c.serv.ctx.Dispatcher.push(topic, recvMsg.RouteKey, []byte(recvMsg.Body), recvMsg.Delay)
		if err != nil {
			c.RespErr(err)
		}

		msgIds[i] = msgId
		msg = nil
		msglenBuf = nil
	}

	nbytes, err := json.Marshal(msgIds)
	if err != nil {
		c.RespErr(err)
	} else {
		c.RespRes(string(nbytes))
	}

	return nil
}

// pop <topic_name> <bind_key>
func (c *TcpConn) POP(params [][]byte) error {
	if len(params) != 2 {
		return errors.New("pop params is error")
	}

	topic := string(params[0])
	bindKey := string(params[1])
	msg, err := c.serv.ctx.Dispatcher.pop(topic, bindKey)

	if err != nil {
		c.RespErr(err)
	} else {
		c.RespMsg(msg)
	}

	msg = nil
	return nil
}

// ack <message_id>
func (c *TcpConn) ACK(params [][]byte) error {
	if len(params) != 3 {
		return errors.New("ack params is error")
	}

	msgId, _ := strconv.ParseInt(string(params[0]), 10, 64)
	topic := string(params[1])
	bindKey := string(params[2])

	if err := c.serv.ctx.Dispatcher.ack(topic, uint64(msgId), bindKey); err != nil {
		c.RespErr(err)
	} else {
		c.RespRes("ok")
	}

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
		return err
	}
	if len(msgs) == 0 {
		c.RespErr(errors.New("no message"))
		return nil
	}

	c.RespMsgs(msgs)
	return nil
}

type topicConfigure struct {
	isAutoAck int
	msgTTR    int
	msgRetry  int
	mode      int
}

// 设置topic信息,目前只有isAutoAck选项
// set <topic_name> <isAutoAck> <mode> <msg_ttr> <msg_retry>
func (c *TcpConn) SET(params [][]byte) error {
	if len(params) != 2 {
		return errors.New("ack params is error")
	}

	topic := string(params[0])
	if len(topic) == 0 {
		c.RespErr(fmt.Errorf("topic is empty"))
		return nil
	}

	configure := &topicConfigure{}
	configure.isAutoAck, _ = strconv.Atoi(string(params[1]))
	configure.mode, _ = strconv.Atoi(string(params[2]))
	configure.msgTTR, _ = strconv.Atoi(string(params[3]))
	configure.msgRetry, _ = strconv.Atoi(string(params[4]))

	err := c.serv.ctx.Dispatcher.set(topic, configure)
	configure = nil

	if err != nil {
		c.RespErr(err)
	} else {
		c.RespRes("ok")
	}

	return nil
}

// declare queue
// queue <topic_name> <bind_key>
func (c *TcpConn) DECLAREQUEUE(params [][]byte) error {
	if len(params) != 2 {
		return errors.New("queue params is error")
	}

	topic := string(params[0])
	if len(topic) == 0 {
		c.RespErr(fmt.Errorf("topic name is empty"))
		return nil
	}
	bindKey := string(params[1])
	if len(bindKey) == 0 {
		c.RespErr(fmt.Errorf("bindKey is empty"))
		return nil
	}

	if err := c.serv.ctx.Dispatcher.declareQueue(topic, bindKey); err != nil {
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
	msgData.Id = strconv.FormatUint(msg.Id, 10)

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
		msgData.Id = strconv.FormatUint(msg.Id, 10)
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
