package mq

import (
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strconv"
)

//自己的数据类
type Service struct {
}

func (s *Service) Push(j map[string]string, reply *string) error {
	delay, _ := strconv.Atoi(j["delay"])
	TTR, _ := strconv.Atoi(j["TTR"])
	job := &Job{
		Id:    j["id"],
		Body:  j["body"],
		Topic: j["topic"],
		Delay: delay,
		TTR:   TTR,
	}

	err := Dper.AddToJobPool(job)
	if err != nil {
		*reply = err.Error()
	} else {
		*reply = "success"
	}
	return nil
}

// 1. 没有数据的时候
func (s *Service) Pop(topic []string, reply *map[string]string) (err error) {
	*reply, err = Pop(topic...)
	return err
}

type RpcServer struct {
}

func (s *RpcServer) Run() {
	rpc.Register(new(Service))
	listener, err := net.Listen("tcp", ":9503")
	if err != nil {
		log.Error("listen error:", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		//新协程来处理--json
		go jsonrpc.ServeConn(conn)
	}
}
