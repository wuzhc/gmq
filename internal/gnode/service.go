package gnode

import (
	"fmt"
	"github.com/wuzhc/gmq/pkg/logs"
	pb "github.com/wuzhc/gmq/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func NewGrpcServer(dispatcher *Dispatcher, ctx context.Context) *grpc.Server {
	serv := grpc.NewServer()
	pb.RegisterRpcServer(serv, ClientService{
		dispatcher: dispatcher,
		context:    ctx,
		logger:     ctx.Value("logger").(*logs.Dispatcher),
	})
	return serv
}

type ClientService struct {
	context    context.Context
	dispatcher *Dispatcher
	logger     *logs.Dispatcher
}

func (c ClientService) Push(ctx context.Context, in *pb.PushRequest) (*pb.PushReply, error) {
	if len(in.Topic) == 0 {
		return nil, fmt.Errorf("topic can't empty.")
	}

	msgId := c.dispatcher.snowflake.Generate()
	msg := &Msg{}
	msg.Id = msgId
	msg.Delay = uint32(in.Delay)
	msg.Body = []byte(in.Message)

	topic := c.dispatcher.GetTopic(in.Topic)
	err := topic.push(msg, in.RouteKey)
	msg = nil

	return &pb.PushReply{MessageId: int64(msgId)}, err
}

// pop message to consumer
func (c ClientService) Pop(server pb.Rpc_PopServer) error {
	var topic *Topic
	var queue string

	for {
		// 1. ready,setting topic object and queue name
		// 2. begin send message,send message to consumer
		in, err := server.Recv()
		if err != nil {
			return err
		}

		if in.ReqStatus == 1 {
			if len(in.Topic) == 0 {
				return fmt.Errorf("ready status must be need topic.")
			}

			queue = in.Queue
			topic, err = c.dispatcher.GetExistTopic(in.Topic)
			if err != nil {
				return err
			}

			err = server.Send(&pb.PopReply{Message: "ready"})
			if err != nil {
				return err
			}
		} else if in.ReqStatus == 2 {
			if topic == nil {
				return fmt.Errorf("pop don't ready.")
			}

			msg, err := topic.pop(queue)
			if err != nil {
				return err
			}

			err = server.Send(&pb.PopReply{Message: string(msg.Body), MessageId: int64(msg.Id)})
			if err != nil {
				errMsg := fmt.Sprintf("consumer connection is closed, write msg.id %v back to queue again", msg.Id)
				c.logger.Warn(logs.LogCategory("rpc service"), errMsg)
				if !topic.isAutoAck {
					if ackerr := topic.ack(msg.Id, queue); ackerr != nil {
						errMsg := fmt.Sprintf("write back to queue again,but prev ack failed, %v", ackerr)
						c.logger.Warn(logs.LogCategory("rpc service"), errMsg)
						return err
					}
				}

				if pusherr := topic.push(msg, queue); pusherr != nil {
					errMsg := fmt.Sprintf("failed to write back to queue, %v", pusherr)
					c.logger.Warn(logs.LogCategory("rpc service"), errMsg)
				}

				return err
			}
		} else {
			return fmt.Errorf("unkown request status.")
		}
	}
}

func (c ClientService) Ack(ctx context.Context, in *pb.AckRequest) (*pb.AckReply, error) {
	if in.MessageId == 0 {
		return nil, fmt.Errorf("messageId can't empty.")
	}
	if len(in.Topic) == 0 {
		return nil, fmt.Errorf("topic can't empty.")
	}

	topic, err := c.dispatcher.GetExistTopic(in.Topic)
	if err != nil {
		return nil, err
	}

	err = topic.ack(uint64(in.MessageId), in.Queue)
	if err != nil {
		return nil, err
	} else {
		return &pb.AckReply{Result: "ok"}, nil
	}
}

func (c ClientService) Declare(ctx context.Context, in *pb.DeclareRequest) (*pb.DeclareReply, error) {
	if len(in.Queue) == 0 {
		return nil, fmt.Errorf("queue can't empty.")
	}
	if len(in.Topic) == 0 {
		return nil, fmt.Errorf("topic can't empty.")
	}

	topic, err := c.dispatcher.GetExistTopic(in.Topic)
	if err != nil {
		return nil, err
	}

	err = topic.delcareQueue(in.Queue)
	if err != nil {
		return nil, err
	} else {
		return &pb.DeclareReply{Result: "ok"}, nil
	}
}

func (c ClientService) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishReply, error) {
	if len(in.Channel) == 0 {
		return nil, fmt.Errorf("channel can't empty.")
	}
	if len(in.Message) == 0 {
		return nil, fmt.Errorf("message can't empty.")
	}

	channel := c.dispatcher.GetChannel(in.Channel)
	err := channel.publish([]byte(in.Message))
	if err != nil {
		return nil, err
	} else {
		return &pb.PublishReply{Result: "ok"}, nil
	}
}

func (c ClientService) Subscribe(in *pb.SubscribeRequest, server pb.Rpc_SubscribeServer) error {
	if len(in.Channel) == 0 {
		return fmt.Errorf("channel can't empty.")
	}
	channel := c.dispatcher.GetChannel(in.Channel)
	client := NewChannelClient()
	channel.AddClient(client)
	defer func() {
		fmt.Println("退出啦休息休息")
		channel.removeClient(client)
		client = nil
	}()

	for {
		select {
		case msg := <-client.readChan:
			err := server.Send(&pb.SubscribeReply{Message: string(msg)})
			if err != nil {
				return err
			}
		}
	}
}
