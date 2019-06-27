package mq

type IServer interface {
	Run()
}

func NewServ() IServer {
	var Serv IServer
	servType := gmq.cfg.Section("server").Key("type").String()
	if servType == "rpc" {
		Serv = &RpcServer{}
	} else {
		Serv = &HttpServer{}
	}
	return Serv
}
