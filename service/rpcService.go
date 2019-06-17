package service

import (
	"go-mq/mq"
)

type rpcService struct {
	dispatcher *mq.Dispatcher
}

func (r *rpcService) Run() {
}
