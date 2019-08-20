// 功能:
// 		- 维护topic和客户端链接的关联,一对多关系
// 		- 一个topic一个goroutine,执行brpop阻塞,等有消息时分配给客户端,主动推送
// 难点:
//		- 分配机制,均匀轮询? 权重? 客户端消费情况?
// 		- tcp粘包处理,TCP连接是长连接，即一次连接多次发送数据。
// 		- job消息序列化处理(json, glob, protocol)
package gnode

import (
	"log"
	"net"
	"sync"

	"github.com/wuzhc/gmq/pkg/coder"
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
	defer s.wg.Wait()

	s.ctx.Logger.Info("tcp_server is running")
	addr := s.ctx.Conf.TcpServAddr
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-s.exitChan:
			return
		default:
			conn, err := listen.Accept()
			if err != nil {
				log.Printf("Accept failed \n")
				continue
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
}
