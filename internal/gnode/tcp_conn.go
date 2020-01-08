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
	RESP_PING    = 105
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
	if err := c.conn.SetDeadline(time.Time{}); err != nil {
		c.LogError(fmt.Sprintf("set deadlie failed, %s", err))
	}

	var buf bytes.Buffer // todo 待优化
	for {
		var err error

		// todo 如果是服务器自己退出，这里应该会报错的
		line, isPrefix, err := c.reader.ReadLine()
		if err != nil {
			if err != io.EOF {
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
			err = c.DECLAREQUEUE(params)
		case bytes.Equal(cmd, []byte("subscribe")):
			err = c.SUBSCRIBE(params)
		case bytes.Equal(cmd, []byte("publish")):
			err = c.PUBLISH(params)
		case bytes.Equal(cmd, []byte("ping")):
			c.PING()
		default:
			err = NewClientErr(ErrUnkownCmd, fmt.Sprintf("unkown cmd: %s", cmd))
		}

		if err != nil {
			// response error to client
			if err := c.Send(RESP_ERROR, []byte(err.Error())); err != nil {
				break
			}
			// fatal error must be closed automatically
			if _, ok := err.(*FatalClientErr); ok {
				break
			} else {
				continue
			}
		}
	}

	// force close conn
	_ = c.conn.Close()
	close(c.exitChan) // notify channel to remove connection
}

func (c *TcpConn) exit() {
	c.conn.Close()
}

// pub <topic_name> <route_key> <delay-time>\n
// [ 4-byte size in bytes ][ N-byte binary data ]
func (c *TcpConn) PUB(params [][]byte) error {
	if len(params) != 3 {
		return NewFatalClientErr(ErrParams, "3 parameters required")
	}

	topic := string(params[0])
	routeKey := string(params[1])
	delay, _ := strconv.Atoi(string(params[2]))
	if delay > MSG_MAX_DELAY {
		return NewClientErr(ErrDelay, fmt.Sprintf("delay can't exceeding the maximum %s", MSG_MAX_DELAY))
	}

	bodylenBuf := make([]byte, 4)
	_, err := io.ReadFull(c.reader, bodylenBuf)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	bodylen := int(binary.BigEndian.Uint32(bodylenBuf))
	body := make([]byte, bodylen)
	_, err = io.ReadFull(c.reader, body)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	cb := make([]byte, len(body))
	copy(cb, body)

	msgId, err := c.serv.ctx.Dispatcher.push(topic, routeKey, cb, delay)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	if err := c.Send(RESP_RESULT, []byte(strconv.FormatUint(msgId, 10))); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// mpub <topic_name> <num>\n
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
// <msg.len> <[]byte({"delay":1,"body":"xxx","topic":"xxx","routeKey":"xxx"})>
func (c *TcpConn) MPUB(params [][]byte) error {
	var err error

	if len(params) != 2 {
		return NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	num, _ := strconv.Atoi(string(params[1]))
	if num <= 0 || num > c.serv.ctx.Conf.MsgMaxPushNum {
		return NewFatalClientErr(ErrPushNum, fmt.Sprintf("number of push must be between 1 and %v", c.serv.ctx.Conf.MsgMaxPushNum))
	}

	msgIds := make([]uint64, num)
	for i := 0; i < num; i++ {
		msglenBuf := make([]byte, 4)
		_, err = io.ReadFull(c.reader, msglenBuf)
		if err != nil {
			return NewFatalClientErr(ErrReadConn, err.Error())
		}

		msglen := int(binary.BigEndian.Uint32(msglenBuf))
		msg := make([]byte, msglen)
		_, err = io.ReadFull(c.reader, msg)
		if err != nil {
			return NewFatalClientErr(ErrReadConn, err.Error())
		}

		var recvMsg RecvMsgData
		if err := json.Unmarshal(msg, &recvMsg); err != nil {
			c.RespErr(fmt.Errorf("decode msg failed, %s", err))
		}

		msgId, err := c.serv.ctx.Dispatcher.push(topic, recvMsg.RouteKey, []byte(recvMsg.Body), recvMsg.Delay)
		if err != nil {
			return NewClientErr(ErrPush, err.Error())
		}

		msgIds[i] = msgId
		msg = nil
		msglenBuf = nil
	}

	nbytes, err := json.Marshal(msgIds)
	if err != nil {
		return NewClientErr(ErrJson, err.Error())
	}

	if err := c.Send(RESP_RESULT, nbytes); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// 消费消息
// pop <topic_name> <bind_key>\n
func (c *TcpConn) POP(params [][]byte) error {
	if len(params) != 2 {
		return NewFatalClientErr(ErrParams, "2 parameters required.")
	}

	topic := string(params[0])
	if len(topic) == 0 {
		return NewFatalClientErr(ErrParams, "topic name required.")
	}

	bindKey := string(params[1])
	t := c.serv.ctx.Dispatcher.GetTopic(topic)
	queue := t.getQueueByBindKey(bindKey)
	if queue == nil {
		return NewClientErr(ErrPopMsg, fmt.Sprintf("bindKey:%s can't match queue", bindKey))
	}

	var msg *Msg
	ticker := time.NewTicker(time.Duration(t.ctx.Conf.HeartbeatInterval) * time.Second)
	defer ticker.Stop()

	var queueData *readQueueData
	for {
		select {
		case <-t.exitChan:
			return NewFatalClientErr(ErrReadConn, "closed.")
		case <-ticker.C:
			// when there is no message, a heartbeat packet is sent.
			// sending fails express the client is disconnected
			if err := c.Send(RESP_PING, []byte{'p', 'i', 'n', 'g'}); err != nil {
				return NewFatalClientErr(ErrPopMsg, "closed.")
			}
			continue
		case queueData = <-queue.readChan:
			if queueData != nil {
				msg = Decode(queueData.data)
				if msg.Id == 0 {
					return NewClientErr(ErrPopMsg, "message decode failed.")
				}
				goto exitLoop
			}
		}
	}

exitLoop:
	msgData := RespMsgData{}
	msgData.Body = string(msg.Body)
	msgData.Retry = msg.Retry
	msgData.Id = strconv.FormatUint(msg.Id, 10)
	data, err := json.Marshal(msgData)
	if err != nil {
		return NewClientErr(ErrJson, err.Error())
	}

	if err := c.Send(RESP_MESSAGE, data); err != nil {
		if !t.isAutoAck {
			_ = queue.ack(msg.Id)
		}

		// client disconnected unexpectedly
		// add to the queue again to ensure message is not lose
		c.LogWarn(fmt.Sprintf("client disconnected unexpectedly, the message is written to queue again. message.id %d", msg.Id))
		_ = queue.write(queueData.data)
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	return nil
}

// 确认消息
// ack <message_id> <topic> <bind_key>\n
func (c *TcpConn) ACK(params [][]byte) error {
	if len(params) != 3 {
		return NewFatalClientErr(ErrParams, "3 parameters required")
	}

	msgId, _ := strconv.ParseInt(string(params[0]), 10, 64)
	topic := string(params[1])
	bindKey := string(params[2])

	if err := c.serv.ctx.Dispatcher.ack(topic, uint64(msgId), bindKey); err != nil {
		return NewClientErr(ErrAckMsg, err.Error())
	}

	if err := c.Send(RESP_RESULT, []byte{'o', 'k'}); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// 死信队列消费
// dead <topic_name> <bind_key>\n
func (c *TcpConn) DEAD(params [][]byte) error {
	if len(params) != 2 {
		return NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	bindKey := string(params[1])
	msg, err := c.serv.ctx.Dispatcher.dead(topic, bindKey)
	if err != nil {
		return NewClientErr(ErrDead, err.Error())
	}

	msgData := RespMsgData{}
	msgData.Body = string(msg.Body)
	msgData.Retry = msg.Retry
	msgData.Id = strconv.FormatUint(msg.Id, 10)
	data, err := json.Marshal(msgData)
	if err != nil {
		return NewClientErr(ErrJson, err.Error())
	}
	if err := c.Send(RESP_MESSAGE, data); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

type topicConfigure struct {
	isAutoAck int
	msgTTR    int
	msgRetry  int
	mode      int
}

// 设置topic信息,目前只有isAutoAck选项
// set <topic_name> <isAutoAck> <mode> <msg_ttr> <msg_retry>\n
func (c *TcpConn) SET(params [][]byte) error {
	if len(params) != 2 {
		return NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	if len(topic) == 0 {
		return NewFatalClientErr(ErrTopicEmpty, "topic is empty")
	}

	configure := &topicConfigure{}
	configure.isAutoAck, _ = strconv.Atoi(string(params[1]))
	configure.mode, _ = strconv.Atoi(string(params[2]))
	configure.msgTTR, _ = strconv.Atoi(string(params[3]))
	configure.msgRetry, _ = strconv.Atoi(string(params[4]))

	err := c.serv.ctx.Dispatcher.set(topic, configure)
	configure = nil
	if err != nil {
		return NewClientErr(ErrSet, err.Error())
	}
	if err := c.Send(RESP_RESULT, []byte{'o', 'k'}); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// declare queue
// queue <topic_name> <bind_key>\n
func (c *TcpConn) DECLAREQUEUE(params [][]byte) error {
	if len(params) != 2 {
		return NewFatalClientErr(ErrParams, "2 parameters required")
	}

	topic := string(params[0])
	if len(topic) == 0 {
		return NewFatalClientErr(ErrTopicEmpty, "topic name required")
	}
	bindKey := string(params[1])
	if len(bindKey) == 0 {
		return NewFatalClientErr(ErrBindKeyEmpty, "bind key required")
	}

	if err := c.serv.ctx.Dispatcher.declareQueue(topic, bindKey); err != nil {
		return NewClientErr(ErrDeclare, err.Error())
	}
	if err := c.Send(RESP_RESULT, []byte{'o', 'k'}); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// subscribe channel
// subscribe <channel_name> \n
func (c *TcpConn) SUBSCRIBE(params [][]byte) error {
	if len(params) != 1 {
		return NewFatalClientErr(ErrParams, "1 parameters required")
	}

	channelName := string(params[0])
	if len(channelName) == 0 {
		return NewFatalClientErr(ErrChannelEmpty, "channel name is empty")
	}

	if err := c.serv.ctx.Dispatcher.subscribe(channelName, c); err != nil {
		return NewClientErr(ErrSubscribe, err.Error())
	}

	if err := c.Send(RESP_RESULT, []byte{'o', 'k'}); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

// publish message to channel
// publish <channel_name>\n
// <message_len> <message>
func (c *TcpConn) PUBLISH(params [][]byte) error {
	if len(params) != 1 {
		return NewFatalClientErr(ErrParams, "1 parameters required")
	}

	channelName := string(params[0])
	if len(channelName) == 0 {
		return NewFatalClientErr(ErrChannelEmpty, "channel name required")
	}

	bodylenBuf := make([]byte, 4)
	_, err := io.ReadFull(c.reader, bodylenBuf)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	bodylen := int(binary.BigEndian.Uint32(bodylenBuf))
	body := make([]byte, bodylen)
	_, err = io.ReadFull(c.reader, body)
	if err != nil {
		return NewFatalClientErr(ErrReadConn, err.Error())
	}

	if err := c.serv.ctx.Dispatcher.publish(channelName, body); err != nil {
		return NewClientErr(ErrPublish, err.Error())
	}

	if err := c.Send(RESP_RESULT, []byte{'o', 'k'}); err != nil {
		return NewFatalClientErr(ErrResp, err.Error())
	}

	return nil
}

func (c *TcpConn) PING() {
	_ = c.conn.SetDeadline(time.Now().Add(5 * time.Second))
}

func (c *TcpConn) Send(respType int16, respData []byte) error {
	var buf = make([]byte, 2+4+len(respData))
	binary.BigEndian.PutUint16(buf[:2], uint16(respType))
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(respData)))
	copy(buf[6:], respData)
	_, err := c.conn.Write(buf)
	return err

	//var err error
	// write response type
	//respTypeBuf := make([]byte, 2)
	//binary.BigEndian.PutUint16(respTypeBuf, uint16(respType))
	//_, err = c.writer.Write(respTypeBuf)
	//if err != nil {
	//	return err
	//}

	// write data length
	//dataLenBuf := make([]byte, 4)
	//binary.BigEndian.PutUint32(dataLenBuf, uint32(len(respData)))
	//_, err = c.writer.Write(dataLenBuf)
	//if err != nil {
	//	return err
	//}

	// write data
	//_, err = c.writer.Write(respData)
	//if err != nil {
	//	return err
	//}

	//return nil
	//return c.writer.Flush()
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

	c.Send(RESP_MESSAGE, data)
	return true
}

func (c *TcpConn) RespErr(err error) bool {
	c.Send(RESP_ERROR, []byte(err.Error()))
	return false
}

func (c *TcpConn) RespRes(msg string) bool {
	c.Send(RESP_RESULT, []byte(msg))
	return true
}

func (c *TcpConn) LogError(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("TcpConn."+c.conn.RemoteAddr().String()))
	v = append(v, msg...)
	c.serv.ctx.Logger.Error(v...)
}

func (c *TcpConn) LogWarn(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("TcpConn."+c.conn.RemoteAddr().String()))
	v = append(v, msg...)
	c.serv.ctx.Logger.Warn(v...)
}

func (c *TcpConn) LogInfo(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("TcpConn."+c.conn.RemoteAddr().String()))
	v = append(v, msg...)
	c.serv.ctx.Logger.Info(v...)
}
