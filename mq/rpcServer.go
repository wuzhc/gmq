package mq

import (
	"context"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strconv"
)

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

	err := gmq.dispatcher.AddToJobPool(job)
	if err != nil {
		*reply = err.Error()
	} else {
		*reply = "success"
	}
	return nil
}

func (s *Service) Pop(topic []string, reply *map[string]string) (err error) {
	*reply, err = Pop(topic...)
	return err
}

func (s *Service) Ack(id string, reply *bool) (err error) {
	*reply, err = Ack(id)
	return err
}

type RpcServer struct {
}

func (s *RpcServer) Run(ctx context.Context) {
	rpc.Register(new(Service))
	listener, err := net.Listen("tcp", ":9503")
	if err != nil {
		log.Error("listen error:", err)
	} else {
		defer listener.Close()
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		select {
		case <-ctx.Done():
			log.Info("rpcServer exit")
			return
		default:
		}
		go jsonrpc.ServeConn(conn)
	}
}
