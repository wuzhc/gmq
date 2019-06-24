package main

import (
	"gmq/mq"
)

func main() {
	q := mq.NewGmq()
	q.Run()
}
