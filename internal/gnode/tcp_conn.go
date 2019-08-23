package gnode

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

const (
	RESP_JOB = iota // 响应job,用于客户端消费job
	RESP_ERR        // 响应错误
	RESP_MSG        // 响应消息(提示)
)

const (
	CMD_POP  = "pop"
	CMD_PUSH = "push"
	CMD_PING = "ping"
	CMD_ACK  = "ack"
)

type TcpConn struct {
	conn     net.Conn
	serv     *TcpServ
	wg       utils.WaitGroupWrapper
	doChan   chan *TcpPkg
	exitChan chan struct{}
	once     sync.Once
}

// 处理连接,拆包处理
//   xxxx   |    xx    |    xx    |    ...   |    ...   |
//   包头      命令长度     数据长度     命令        数据
//  4-bytes   2-bytes     2-bytes    n-bytes     n-bytes
func (c *TcpConn) Handle() {
	defer c.wg.Wait()

	c.wg.Wrap(c.router)

	scanner := bufio.NewScanner(c.conn)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		// c.serv.ctx.Logger.Warn(len(data), string(data))
		if len(data) > 4 && !atEOF && bytes.Equal(data[:4], []byte{'v', '1', '1', '1'}) {
			if len(data) > 6 { // must be greater than 6 (headerLen + cmdLen + dataLen)
				var cmdLen, dataLen int16
				binary.Read(bytes.NewReader(data[4:6]), binary.BigEndian, &cmdLen)
				binary.Read(bytes.NewReader(data[6:8]), binary.BigEndian, &dataLen)
				plen := 4 + 2 + 2 + int(cmdLen) + int(dataLen)
				if plen <= len(data) {
					return plen, data[:plen], nil
				}
			}
		}
		return
	})

	for scanner.Scan() {
		pkg := &TcpPkg{}
		if err := pkg.UnPack(bytes.NewReader(scanner.Bytes())); err != nil {
			c.LogError("Tcp read failed")
		}

		c.doChan <- pkg
	}
}

func (c *TcpConn) exit() {
	c.once.Do(func() {
		close(c.exitChan)
		c.conn.Close()
	})
}

func (c *TcpConn) router() {
	defer c.LogWarn("Tcp conn router had been closed.")

	for {
		select {
		case <-c.serv.exitChan:
			return
		case <-c.exitChan:
			return
		case pkg := <-c.doChan:
			c.handlePackage(pkg)
		}
	}
}

func (c *TcpConn) handlePackage(pkg *TcpPkg) bool {
	cmd := string(pkg.Cmd)
	switch cmd {
	case CMD_PUSH:
		job := &Job{}
		if err := c.serv.coder.Decode(pkg.Data, job); err != nil {
			return c.RespErr(err)
		}
		if err := c.serv.ctx.Dispatcher.AddToJobPool(job); err != nil {
			return c.RespErr(err)
		}

		return c.RespMsg(job.Id)
	case CMD_POP:
		topicStr := string(pkg.Data)
		if len(topicStr) == 0 {
			return c.RespErr(errors.New("topic is empty"))
		}

		topics := strings.Split(topicStr, ",")
		job, err := PopFromMutilTopic(c.serv.ctx, topics...)
		if err != nil && err != redis.ErrNil {
			return c.RespErr(err)
		}

		if err == redis.ErrNil {
			return c.RespMsg("nothing")
		} else {
			return c.RespJob(job)
		}
	case CMD_ACK:
		req := struct {
			JobId string
		}{}
		if err := c.serv.coder.Decode(pkg.Data, req); err != nil {
			return c.RespErr(err)
		}
		if len(req.JobId) == 0 {
			return c.RespErr(errors.New("job.id is empty"))
		}

		res, err := Ack(req.JobId)
		if err != nil {
			return c.RespErr(err)
		}
		if res {
			return c.RespMsg("success")
		} else {
			return c.RespErr(err)
		}
	case CMD_PING:
		// 心跳包,不做任何的处理
		return true
	default:
		c.LogWarn(fmt.Sprintf("Unkown cmd %s", cmd))
		return c.RespErr(ErrUnkownCmd)
	}
}

func (c *TcpConn) Response(respType int16, respData []byte) {
	pkg := &TcpRespPkg{
		Type:    int16(respType),
		DataLen: int16(len(respData)),
		Data:    respData,
	}

	if err := pkg.Pack(c.conn); err != nil {
		c.LogError(err)
		c.exit()
	}
}

func (c *TcpConn) RespJob(job *Job) bool {
	v, _ := c.serv.coder.Encode(job)
	c.Response(RESP_JOB, v)
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

func (c *TcpConn) LogError(msg interface{}) {
	c.serv.ctx.Logger.Error(logs.LogCategory(c.conn.RemoteAddr().String()), msg)
}

func (c *TcpConn) LogWarn(msg interface{}) {
	c.serv.ctx.Logger.Warn(logs.LogCategory(c.conn.RemoteAddr().String()), msg)
}
