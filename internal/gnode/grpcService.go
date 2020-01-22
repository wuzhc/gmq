package gnode

import (
	"fmt"
	"github.com/wuzhc/gmq/configs"
	"github.com/wuzhc/gmq/pkg/logs"
	pb "github.com/wuzhc/gmq/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"net"
	"os"
	"time"
)

type GrpcService struct {
	logger   *logs.Dispatcher
	serv     *grpc.Server
	broker   *Broker
	conf     *configs.GnodeConfig
	exitChan chan struct{}
}

func NewGrpcService(logger *logs.Dispatcher, broker *Broker, conf *configs.GnodeConfig) *GrpcService {
	return &GrpcService{
		logger:   logger,
		serv:     grpc.NewServer(),
		broker:   broker,
		conf:     conf,
		exitChan: make(chan struct{}),
	}
}

func (service *GrpcService) stop() {
	close(service.exitChan)
}

func (service *GrpcService) startServ() {
	listener, err := net.Listen("tcp", service.conf.RpcServAddr)
	if err != nil {
		service.LogError(err.Error())
		os.Exit(1) // exit process
	}

	go func() {
		select {
		case <-service.exitChan:
			if err := listener.Close(); err != nil {
				service.LogError(err.Error())
			}
		}
	}()

	pb.RegisterRpcServer(service.serv, ClientService{
		context: context.TODO(),
		broker:  service.broker,
	})
	if err := service.serv.Serve(listener); err != nil {
		service.LogInfo(err.Error())
		return
	}
}

type ClientService struct {
	broker  *Broker
	context context.Context
}

// put message
func (service ClientService) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutReply, error) {
	if len(in.Topic) == 0 {
		return nil, fmt.Errorf("topic can't empty.")
	}

	msgId := service.broker.generateMsgId()
	msg := &CommitLogMsg{
		Id:              msgId,
		Delay:           uint32(in.Delay),
		QueueId:         uint16(in.QueueId),
		QueueOffset:     0,
		CreateTimestamp: time.Now().Unix(),
		TopicLen:        uint32(len(in.Topic)),
		Topic:           in.Topic,
		BodyLen:         uint32(len(in.Message)),
		Body:            in.Message,
		TagLen:          uint32(len(in.Tag)),
		Tag:             in.Tag,
	}
	if err := msg.validate(); err != nil {
		return nil, err
	}

	err := service.broker.commitLogService.PutMessage(msg)
	return &pb.PutReply{MessageId: int64(msgId)}, err
}

// pop message to consumer
func (service ClientService) Pull(server pb.Rpc_PullServer) error {
	var (
		topic   string
		group   string
		tag     string
		queueId int
	)

	for {
		// 1. ready,setting topic object and queue name
		// 2. begin send message,send message to consumer
		in, err := server.Recv()
		if err != nil {
			return err
		}

		if len(in.Topic) > 0 {
			topic = string(in.Topic)
			tag = string(in.Tag)
			queueId = int(in.QueueId)

			pr, ok := peer.FromContext(server.Context())
			if !ok {
				return fmt.Errorf("invoke FromContext() failed.")
			}
			if pr.Addr == net.Addr(nil) {
				return fmt.Errorf("peer.Addr is nil")
			}
			err = service.broker.registerConsumerToGroup(topic, group, pr.Addr.String())
			if err != nil {
				return err
			}
			if err := server.Send(&pb.PullReply{Message: []byte("订阅成功.")}); err != nil {
				return err
			}
			continue
		}

		if len(topic) == 0 || len(group) == 0 || len(tag) == 0 {
			return fmt.Errorf("please subscribe before consume queue.")
		}
		pullNum := in.PullNum
		if pullNum > 32 {
			pullNum = 32
		}

		consumeQueues, ok := service.broker.topicQueues[topic]
		if !ok {
			return fmt.Errorf("topic %s isn't exist.", topic)
		}

		for i := 0; i < int(pullNum); i++ {
			msg, err := consumeQueues.pullMessage(topic, queueId, in.ReadOffset)
			if err != nil {
				return err
			}
			err = server.Send(&pb.PullReply{Message: msg.Body, MessageId: int64(msg.Id)})
			if err != nil {
				return err
			}
		}
	}
}

func (service *GrpcService) LogError(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("GrpcService"))
	v = append(v, msg...)
	service.logger.Error(v...)
}

func (service *GrpcService) LogWarn(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("GrpcService"))
	v = append(v, msg...)
	service.logger.Warn(v...)
}

func (service *GrpcService) LogInfo(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("GrpcService"))
	v = append(v, msg...)
	service.logger.Info(v...)
}

func (service *GrpcService) LogDebug(msg ...interface{}) {
	var v []interface{}
	v = append(v, logs.LogCategory("GrpcService"))
	v = append(v, msg...)
	service.logger.Debug(v...)
}
