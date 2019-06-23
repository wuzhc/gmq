package mq

type IServer interface {
	Run()
}

var Serv IServer

func init() {
	conf := "tcp"
	if conf == "tcp" {
		Serv = &RpcServer{}
	} else {
		Serv = &HttpServer{}
	}
}
