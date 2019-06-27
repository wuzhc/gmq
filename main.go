package main

import (
	"gmq/mq"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(8)
	q := mq.NewGmq()
	q.Run()
}
