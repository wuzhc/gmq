package gnode

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/wuzhc/gmq/pkg/logs"
	"github.com/wuzhc/gmq/pkg/utils"
)

type TcpServ struct {
	ctx      *Context
	wg       utils.WaitGroupWrapper
	mux      sync.RWMutex
	exitChan chan struct{}
}

func NewTcpServ(ctx *Context) *TcpServ {
	return &TcpServ{
		ctx:      ctx,
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
		select {
		case <-s.ctx.Gnode.exitChan:
			close(s.exitChan)
		case <-s.exitChan:
		}
		listen.Close()
	}()

	for {
		conn, err := listen.Accept()
		if err != nil {
			s.LogError(err)
			break
		}

		tcpConn := &TcpConn{
			conn:     conn,
			serv:     s,
			exitChan: make(chan struct{}),
			reader:   bufio.NewReaderSize(conn, 16*1024),
			writer:   bufio.NewWriterSize(conn, 16*1024),
		}
		s.wg.Wrap(tcpConn.Handle)
	}
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
