package main

import (
	"github.com/wuzhc/gmq/mq"
)

func main() {
	q := mq.NewGmq("conf.ini")
	q.Run()
}
