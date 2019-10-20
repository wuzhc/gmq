package gnode

import (
	"bufio"
	"bytes"
	"encoding/binary"
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
	RESP_JOB = iota
	RESP_ERR
	RESP_MSG
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
			// err = c.ACK(params)
		case bytes.Equal(cmd, []byte("mpub")):
			err = c.MPUB(params)
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

// pub <topic_name> <delay-time> <ttr-time>
// [ 4-byte size in bytes ][ N-byte binary data ]
func (c *TcpConn) PUB(params [][]byte) error {
	var err error

	if len(params) != 3 {
		c.LogError(params)
		return errors.New("pub params is error")
	}

	topic := string(params[0])
	delay, _ := strconv.Atoi(string(params[1]))
	if delay > JOB_MAX_DELAY {
		return errors.New("pub.delay exceeding the maximum")
	}

	ttr, _ := strconv.Atoi(string(params[2]))
	if ttr > JOB_MAX_TTR {
		return errors.New("pub.ttr exceeding the maximum")
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
		c.RespMsg(strconv.FormatInt(msgId, 10))
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
		c.RespMsg("success")
	}

	return nil
}

// pop <topic_name>
func (c *TcpConn) POP(params [][]byte) error {
	if len(params) != 1 {
		return errors.New("pop params is error")
	}

	topic := string(params[0])
	msgId, msg, err := c.serv.ctx.Dispatcher.pop(topic)
	if err != nil {
		c.RespErr(err)
		return nil
	}

	// if topic.isAutoAck is false, add to waiting queue
	t := c.serv.ctx.Dispatcher.GetTopic(topic)
	if !t.isAutoAck {
		if err := t.pushMsgToBucket(msgId, msg, 60); err != nil {
			c.RespErr(err)
			return nil
		}
	}

	c.RespJob22(msgId, msg)
	return nil
}

// ack <job_id>
func (c *TcpConn) ACK(params [][]byte) error {
	if len(params) != 2 {
		return errors.New("ack params is error")
	}

	msgId, _ := strconv.ParseInt(string(params[0]), 10, 64)
	topic := string(params[1])

	if err := c.serv.ctx.Dispatcher.ack(topic, msgId); err != nil {
		c.RespErr(err)
		return nil
	}

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

func (c *TcpConn) RespJob(job *Job) bool {
	var data [][]byte
	data = append(data, job.Body)
	data = append(data, []byte(strconv.Itoa(job.ConsumeNum)))
	c.Response(RESP_JOB, bytes.Join(data, []byte{' '}))
	job = nil
	return true
}

func (c *TcpConn) RespJob22(msgId int64, msg []byte) bool {
	var data [][]byte
	data = append(data, msg)
	data = append(data, []byte(strconv.Itoa(int(msgId))))
	c.Response(RESP_JOB, bytes.Join(data, []byte{' '}))
	return true
}

func (c *TcpConn) RespErr(err error) bool {
	c.Response(RESP_ERR, []byte(err.Error()))
	return false
}

func (c *TcpConn) RespMsg(msg string) bool {
	c.Response(RESP_MSG, []byte(msg))
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
