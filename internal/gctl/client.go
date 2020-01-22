package gctl

import (
	"context"
	"fmt"
	pb "github.com/wuzhc/gmq/proto"
	"google.golang.org/grpc"
	"log"
)

type RpcClient struct {
	client pb.RpcClient
	addr   string
	weight int
}

func NewRpcClient(addr string, weight int) *RpcClient {
	if len(addr) == 0 {
		log.Fatalln("address is empty")
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}

	client := pb.NewRpcClient(conn)

	return &RpcClient{
		client: client,
		addr:   addr,
		weight: weight,
	}
}

func (rpc *RpcClient) Exit() {

}

// get remote server addr
func (rpc *RpcClient) GetAddr() string {
	return rpc.addr
}

// pull command
// push <topic:required> <route_key:optional> <message:required> <delay:optional>
func (rpc *RpcClient) Put(data *PushSchema) (int64, error) {
	if err := data.validate(); err != nil {
		return 0, err
	}

	req := new(pb.PutRequest)
	req.Topic = []byte("wuzhc")
	req.QueueId = 1
	req.Tag = []byte("xxx")
	req.Message = []byte("hello world")
	rsp, err := rpc.client.Put(context.Background(), req)
	if err != nil {
		return 0, NewFatalClientErr(ErrRequest, err.Error())
	} else {
		return rsp.MessageId, nil
	}
}

// pop command
// pop <topic:required> <queue:optional>
func (rpc *RpcClient) Pull(data *PopSchema) error {
	if err := data.validate(); err != nil {
		return err
	}

	stream, err := rpc.client.Pull(context.Background())
	if err != nil {
		return NewFatalClientErr(ErrRequest, err.Error())
	}

	// ready to read
	req := new(pb.PullRequest)
	req.Topic = []byte("wuzhc")
	req.Group = []byte("wgroup")
	req.QueueId = 1
	req.ReadOffset = 0
	req.Tag = []byte("xxx")
	if err := stream.Send(req); err != nil {
		return NewFatalClientErr(ErrRequest, err.Error())
	}
	resp, err := stream.Recv()
	if err != nil {
		return NewFatalClientErr(ErrRequest, err.Error())
	}
	fmt.Println(string(resp.Message))

	// begin to read
	for {
		req := new(pb.PullRequest)
		req.PullNum = 32
		req.ReadOffset = 0
		if err := stream.Send(req); err != nil {
			return NewFatalClientErr(ErrRequest, err.Error())
		}
		resp, err := stream.Recv()
		if err != nil {
			return NewFatalClientErr(ErrRequest, err.Error())
		}

		log.Printf("message.id:%v, message:%v \n", resp.MessageId, resp.Message)
	}
}
