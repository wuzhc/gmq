package service

import (
	"go-mq/mq"
)

// "gopkg.in/ini.v1"

const (
	SERVICE_HTTP = "http"
	SERVICE_RPC  = "rpc"
)

type IService interface {
	Run()
}

type Service struct {
	Target IService // http or rpc
	// Conf   *ini.File
}

func NewService(d *mq.Dispatcher) *Service {
	// cfg, err := ini.Load("conf.ini")
	// if err != nil {
	// panic("Load conf.ini failed, " + err.Error())
	// }

	var target IService
	stype := SERVICE_HTTP
	if stype == SERVICE_HTTP {
		target = &httpService{
			dispatcher: d,
		}
	} else if stype == SERVICE_RPC {
		target = &rpcService{
			dispatcher: d,
		}
	} else {
		panic("Service type need to http or rpc")
	}

	return &Service{
		Target: target,
		// Conf:   cfg,
	}
}
