package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"syscall"

	"github.com/wuzhc/gmq/mq"
)

func main() {
	args := os.Args
	if len(args) == 1 {
		fmt.Println("Usage gmq start")
		return
	}
	switch args[1] {
	case "start":
		pid := os.Getpid()
		fd, err := os.OpenFile("gmq.pid", os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			fmt.Println(err)
			return
		}
		if _, err := fd.WriteString(strconv.Itoa(pid)); err != nil {
			fmt.Println(err)
			return
		}

		q := mq.NewGmq("conf.ini")
		q.Run()
	case "stop":
		nbyte, err := ioutil.ReadFile("gmq.pid")
		if err != nil {
			fmt.Println(err)
			return
		}
		pid, err := strconv.Atoi(string(nbyte))
		if err != nil {
			fmt.Print(err)
			return
		}
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("stop")
	default:
		fmt.Println("Usage gmq start")
		fmt.Println("Usage gmq status")
		fmt.Println("Usage gmq stop")
	}
}
