package main

import (
	"go-mq/mq"
	// "go-mq/service"
)

func main() {
	// new dispatcher
	q := mq.NewGmq()
	q.Run()

	// new service
	// serv := service.NewService(dispatcher)
	// serv.Target.Run()
}
