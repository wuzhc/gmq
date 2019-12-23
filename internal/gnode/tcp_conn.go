package gnode

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
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
	RESP_CHANNEL = 104
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
	defer c.LogInfo(fmt.Sprintf("tcp connection %s handle exit.", c.conn.RemoteAddr()))

	// 监控系统退出
	c.wg.Wrap(func() {
		select {
		case <-c.serv.exitChan:
			_ = c.conn.Close()
		case <-c.exitChan:
			return
		}
	})

	var buf bytes.Buffer // todo 待优化
	for {
		var err error
		if err := c.conn.SetDeadline(time.Time{}); err != nil {
			c.LogError(fmt.Sprintf("set deadlie failed, %s", err))
			break
		}

		line, isPrefix, err := c.reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				c.LogWarn("closed.")
			} else {
				c.LogError(fmt.Sprintf("connection error, %s", err))
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

		var (
			data     []byte
			respType int16
		)

		switch {
		case bytes.Equal(cmd, []byte("pub")):
			respType, data, err = c.PUB(params)
		case bytes.Equal(cmd, []byte("pop")):
			respType, data, err = c.POP(params)
		case bytes.Equal(cmd, []byte("ack")):
			respType, data, err = c.ACK(params)
		case bytes.Equal(cmd, []byte("mpub")):
			respType, data, err = c.MPUB(params)
		case bytes.Equal(cmd, []byte("dead")):
			respType, data, err = c.DEAD(params)
		case bytes.Equal(cmd, []byte("set")):
			respType, data, err = c.SET(params)
		case bytes.Equal(cmd, []byte("queue")):
			respType, data, err = c.DECLAREQUEUE(params)
		case bytes.Equal(cmd, []byte("subscribe")):
			respType, data, err = c.SUBSCRIBE(params)
		case bytes.Equal(cmd, []byte("publish")):
			respType, data, err = c.PUBLISH(params)
		default:
			respType = RESP_ERROR
			err = NewClientErr(ErrUnkownCmd, fmt.Sprintf("unkown cmd: %s", cmd))
		}

		if err != nil {
			if _, ok := err.(*FatalClientErr); ok {
				c.LogError(err)
				break
			} else {
				c.Response(respType, []byte(err.Error()))
				continue
			}
		}

		c.Response(respType, data)
	}

	// force close conn
	_ = c.conn.Close()
	close(c.exitChan)
}

// pub <topic_name> <route_key> <delay-time>\n
// [ 4-byte size in bytes ][ N-byte binary data ]
func (c *TcpConn) PUB(params [][]byte) (int16, []byte, error) {
	if len(params) != 3 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrParams, "3 parameters required")
	}

	topic := string(params[0])
	routeKey := string(params[1])
	delay, _ := strconv.Atoi(string(params[2]))
	if delay > MSG_MAX_DELAY {
		return RESP_ERROR, nil, NewClientErr(ErrDelay, fmt.Sprintf("delay can't exceeding the maximum %s", MSG_MAX_DELAY))
	}

	bodylenBuf := make([]byte, 4)
	_, err := io.ReadFull(c.reader, bodylenBuf)
	if err != nil {
		return RESP_ERROR, nil, NewFatalClientErr(ErrReadConn, err.Error())
	}

	bodylen := int(binary.BigEndian.Uint32(bodylenBuf))
	body := make([]byte, bodylen)
	_, err = io.ReadFull(c.reader, body)
	if err != nil {
		return RESP_ERROR, nil, NewFatalClientErr(ErrReadConn, err.Error())
	}

	cb := make([]byte, len(body))
	copy(cb, body)

	msgId, err := c.serv.ctx.Dispatcher.push(topic, routeKey, cb, delay)
	if err != nil {
		return RESP_ERROR, nil, NewFatalClientErr(ErrReadConn, err.Error())
	} else {
		return RESP_RESULT, []byte(strconv.FormatUint(msgId, 10)), nil
	}
}

// mpub <topic_name> <num>\n
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
func (c *TcpConn) MPUB(params [][]byte) (int16, []byte, error) {
	var err error

	if len(params) != 2 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	num, _ := strconv.Atoi(string(params[1]))
	if num <= 0 || num > c.serv.ctx.Conf.MsgMaxPushNum {
		return RESP_ERROR, nil, NewFatalClientErr(ErrPushNum, fmt.Sprintf("number of push must be between 1 and %v", c.serv.ctx.Conf.MsgMaxPushNum))
	}

	msgIds := make([]uint64, num)
	for i := 0; i < num; i++ {
		msglenBuf := make([]byte, 4)
		_, err = io.ReadFull(c.reader, msglenBuf)
		if err != nil {
			return RESP_ERROR, nil, NewFatalClientErr(ErrReadConn, err.Error())
		}

		msglen := int(binary.BigEndian.Uint32(msglenBuf))
		msg := make([]byte, msglen)
		_, err = io.ReadFull(c.reader, msg)
		if err != nil {
			return RESP_ERROR, nil, NewFatalClientErr(ErrReadConn, err.Error())
		}

		var recvMsg RecvMsgData
		if err := json.Unmarshal(msg, &recvMsg); err != nil {
			c.RespErr(fmt.Errorf("decode msg failed, %s", err))
		}

		msgId, err := c.serv.ctx.Dispatcher.push(topic, recvMsg.RouteKey, []byte(recvMsg.Body), recvMsg.Delay)
		if err != nil {
			return RESP_ERROR, nil, NewClientErr(ErrPush, err.Error())
		}

		msgIds[i] = msgId
		msg = nil
		msglenBuf = nil
	}

	nbytes, err := json.Marshal(msgIds)
	if err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrJson, err.Error())
	} else {
		return RESP_MESSAGE, nbytes, nil
	}
}

// 消费消息
// pop <topic_name> <bind_key>\n
func (c *TcpConn) POP(params [][]byte) (int16, []byte, error) {
	if len(params) != 2 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	bindKey := string(params[1])
	msg, err := c.serv.ctx.Dispatcher.pop(topic, bindKey)
	if err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrPopMsg, err.Error())
	}

	msgData := RespMsgData{}
	msgData.Body = string(msg.Body)
	msgData.Retry = msg.Retry
	msgData.Id = strconv.FormatUint(msg.Id, 10)
	data, err := json.Marshal(msgData)
	if err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrJson, err.Error())
	} else {
		return RESP_MESSAGE, data, nil
	}
}

// 确认消息
// ack <message_id> <topic> <bind_key>\n
func (c *TcpConn) ACK(params [][]byte) (int16, []byte, error) {
	if len(params) != 3 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrParams, "3 parameters required")
	}

	msgId, _ := strconv.ParseInt(string(params[0]), 10, 64)
	topic := string(params[1])
	bindKey := string(params[2])

	if err := c.serv.ctx.Dispatcher.ack(topic, uint64(msgId), bindKey); err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrAckMsg, err.Error())
	} else {
		return RESP_RESULT, []byte{'o', 'k'}, nil
	}
}

// 死信队列消费
// dead <topic_name> <bind_key>\n
func (c *TcpConn) DEAD(params [][]byte) (int16, []byte, error) {
	if len(params) != 2 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	bindKey := string(params[1])
	msg, err := c.serv.ctx.Dispatcher.dead(topic, bindKey)

	if err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrDead, err.Error())
	}

	msgData := RespMsgData{}
	msgData.Body = string(msg.Body)
	msgData.Retry = msg.Retry
	msgData.Id = strconv.FormatUint(msg.Id, 10)
	data, err := json.Marshal(msgData)
	if err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrJson, err.Error())
	} else {
		return RESP_MESSAGE, data, nil
	}
}

type topicConfigure struct {
	isAutoAck int
	msgTTR    int
	msgRetry  int
	mode      int
}

// 设置topic信息,目前只有isAutoAck选项
// set <topic_name> <isAutoAck> <mode> <msg_ttr> <msg_retry>\n
func (c *TcpConn) SET(params [][]byte) (int16, []byte, error) {
	if len(params) != 2 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	if len(topic) == 0 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrTopicEmpty, "topic is empty")
	}

	configure := &topicConfigure{}
	configure.isAutoAck, _ = strconv.Atoi(string(params[1]))
	configure.mode, _ = strconv.Atoi(string(params[2]))
	configure.msgTTR, _ = strconv.Atoi(string(params[3]))
	configure.msgRetry, _ = strconv.Atoi(string(params[4]))

	err := c.serv.ctx.Dispatcher.set(topic, configure)
	configure = nil

	if err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrSet, err.Error())
	} else {
		return RESP_RESULT, []byte{'o', 'k'}, nil
	}
}

// declare queue
// queue <topic_name> <bind_key>\n
func (c *TcpConn) DECLAREQUEUE(params [][]byte) (int16, []byte, error) {
	if len(params) != 2 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	if len(topic) == 0 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrTopicEmpty, "topic name required")
	}
	bindKey := string(params[1])
	if len(bindKey) == 0 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrBindKeyEmpty, "bind key required")
	}

	if err := c.serv.ctx.Dispatcher.declareQueue(topic, bindKey); err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrDeclare, err.Error())
	} else {
		return RESP_RESULT, []byte{'o', 'k'}, nil
	}
}

// subscribe channel
// subscribe <channel_name> \n
func (c *TcpConn) SUBSCRIBE(params [][]byte) (int16, []byte, error) {
	if len(params) != 1 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrParams, "1 parameters required")
	}

	channelName := string(params[0])
	if len(channelName) == 0 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrChannelEmpty, "channel name is empty")
	}

	if err := c.serv.ctx.Dispatcher.subscribe(channelName, c); err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrSubscribe, err.Error())
	} else {
		return RESP_RESULT, []byte{'o', 'k'}, nil
	}
}

// publish message to channel
// publish <channel_name> <message>\n
func (c *TcpConn) PUBLISH(params [][]byte) (int16, []byte, error) {
	if len(params) != 2 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrParams, "2 parameters required")
	}

	channelName := string(params[0])
	if len(channelName) == 0 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrChannelEmpty, "channel name required")
	}

	message := params[1]
	if len(message) == 0 {
		return RESP_ERROR, nil, NewFatalClientErr(ErrMsgEmpty, "message required")
	}

	if err := c.serv.ctx.Dispatcher.publish(channelName, message); err != nil {
		return RESP_ERROR, nil, NewClientErr(ErrPublish, err.Error())
	} else {
		return RESP_RESULT, []byte{'o', 'k'}, nil
	}
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
