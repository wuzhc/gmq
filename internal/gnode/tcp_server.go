// 功能:
// 	- tcp粘包处理,TCP连接是长连接，即一次连接多次发送数据。
// 	- job消息序列化处理(json, glob, protocol)
package gnode

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/wuzhc/gmq/pkg/coder"
	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

type TcpServ struct {
	ctx      *Context
	coder    coder.Coder
	wg       utils.WaitGroupWrapper
	mux      sync.RWMutex
	m        map[string][]net.Conn
	ch       chan string
	exitChan chan struct{}
	closed   bool
}

func NewTcpServ(ctx *Context) *TcpServ {
	return &TcpServ{
		ctx:      ctx,
		coder:    coder.New(ctx.Conf.TcpServCoder),
		m:        make(map[string][]net.Conn),
		ch:       make(chan string),
		exitChan: make(chan struct{}),
	}
}

func (s *TcpServ) Run() {
	defer func() {
		s.wg.Wait()
		s.LogInfo("Tcp server exit.")
	}()

	addr := s.ctx.Conf.TcpServAddr
	s.LogInfo(fmt.Sprintf("Tcp server(%s) is running.", addr))

	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		<-s.ctx.Gnode.exitChan
		s.exit()
		listen.Close()
	}()

	for {
		conn, err := listen.Accept()
		if err != nil {
			if s.closed {
				return
			} else {
				s.LogError(err)
				continue
			}
		}

		tcpConn := &TcpConn{
			conn:     conn,
			serv:     s,
			doChan:   make(chan *TcpPkg),
			exitChan: make(chan struct{}),
		}

		s.wg.Wrap(tcpConn.Handle)
	}
}

func (s *TcpServ) exit() {
	s.closed = true
	close(s.exitChan)
}

func (s *TcpServ) LogError(msg interface{}) {
	s.ctx.Logger.Error(logs.LogCategory("TcpServer"), msg)
}

func (s *TcpServ) LogWarn(msg interface{}) {
	s.ctx.Logger.Warn(logs.LogCategory("TcpServer"), msg)
}

func (s *TcpServ) LogInfo(msg interface{}) {
	s.ctx.Logger.Info(logs.LogCategory("TcpServer"), msg)
}

func (s *TcpServ) LogDebug(msg interface{}) {
	s.ctx.Logger.Debug(logs.LogCategory("TcpServer"), msg)
}
