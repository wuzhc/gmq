package gctl

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	pb "github.com/wuzhc/gmq/proto"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
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

// push command
// push <topic:required> <route_key:optional> <message:required> <delay:optional>
func (rpc *RpcClient) Push(data *PushSchema) (int64, error) {
	if err := data.validate(); err != nil {
		return 0, err
	}

	req := new(pb.PushRequest)
	req.Topic = data.Topic
	req.Delay = data.Delay
	req.Message = data.Message
	rsp, err := rpc.client.Push(context.Background(), req)
	if err != nil {
		return 0, NewFatalClientErr(ErrRequest, err.Error())
	} else {
		return rsp.MessageId, nil
	}
}

// pop command
// pop <topic:required> <queue:optional>
func (rpc *RpcClient) Pop(data *PopSchema) error {
	if err := data.validate(); err != nil {
		return err
	}

	stream, err := rpc.client.Pop(context.Background())
	if err != nil {
		return NewFatalClientErr(ErrRequest, err.Error())
	}

	// ready to read
	req := new(pb.PopRequest)
	req.Topic = data.Topic
	req.Queue = data.Queue
	req.ReqStatus = 1
	if err := stream.Send(req); err != nil {
		return NewFatalClientErr(ErrRequest, err.Error())
	}
	resp, err := stream.Recv()
	if err != nil {
		return NewFatalClientErr(ErrRequest, err.Error())
	}
	if resp.Message != "ready" {
		return NewFatalClientErr(ErrRequest, "unknown error.")
	}

	// begin to read
	for {
		req := new(pb.PopRequest)
		req.ReqStatus = 2

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

// publish <channel:required> <message:required>
func (rpc *RpcClient) Publish(data *PublishSchema) (string, error) {
	if err := data.validate(); err != nil {
		return "", err
	}

	req := new(pb.PublishRequest)
	req.Message = data.Message
	req.Channel = data.Channel
	rsp, err := rpc.client.Publish(context.Background(), req)
	if err != nil {
		return "", NewFatalClientErr(ErrRequest, err.Error())
	} else {
		return rsp.Result, nil
	}
}

// subscribe command
// subscribe <channel:required>
func (rpc *RpcClient) Subscribe(data *SubscribeSchema) error {
	if err := data.validate(); err != nil {
		return err
	}

	req := new(pb.SubscribeRequest)
	req.Channel = data.Channel
	stream, err := rpc.client.Subscribe(context.Background(), req)
	if err != nil {
		return NewFatalClientErr(ErrRequest, err.Error())
	}

	for {
		rsp, err := stream.Recv()
		if err != nil {
			return NewFatalClientErr(ErrRequest, err.Error())
		}

		log.Printf("recv:%v", rsp.Message)
	}
}

var clients []*RpcClient

func InitRpcClients(endpoints string) ([]*RpcClient, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("endpoints is empty.")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("can't new etcd client.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	resp, err := cli.Get(ctx, "/gmq/node", clientv3.WithPrefix())
	cancel()
	if err != nil {
		return nil, err
	}

	var clients []*RpcClient
	node := make(map[string]string)
	for _, ev := range resp.Kvs {
		fmt.Printf("%s => %s\n", ev.Key, ev.Value)
		if err := json.Unmarshal(ev.Value, &node); err != nil {
			return nil, err
		}

		tcpAddr := node["tcp_addr"]
		weight, _ := strconv.Atoi(node["weight"])
		c := NewRpcClient(tcpAddr, weight)
		clients = append(clients, c)
	}

	return clients, nil
}

func GetRpcClientByWeightMode(endpoints string) (*RpcClient, error) {
	if len(clients) == 0 {
		var err error
		clients, err = InitRpcClients(endpoints)
		if err != nil {
			return nil, err
		}
	}

	total := 0
	for _, c := range clients {
		total += c.weight
	}

	w := 0
	rand.Seed(time.Now().UnixNano())
	randValue := rand.Intn(total) + 1
	for _, c := range clients {
		prev := w
		w = w + c.weight
		if randValue > prev && randValue <= w {
			return c, nil
		}
	}

	return nil, NewClientErr(ErrRequest, "not client.")
}

func GetClientByRandomMode(endpoints string) (*RpcClient, error) {
	if len(clients) == 0 {
		var err error
		clients, err = InitRpcClients(endpoints)
		if err != nil {
			return nil, err
		}
	}

	rand.Seed(time.Now().UnixNano())
	k := rand.Intn(len(clients))
	return clients[k], nil
}

func GetClientByAvgMode(endpoints string) (*RpcClient, error) {
	if len(clients) == 0 {
		var err error
		clients, err = InitRpcClients(endpoints)
		if err != nil {
			return nil, err
		}
	}

	c := clients[0]
	if len(clients) > 1 {
		// 已处理过的消息客户端重新放在最后
		clients = append(clients[1:], c)
	}

	return c, nil
}
