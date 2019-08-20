// gmq节点,可以部署多个以实现分布式
// 功能:
// 	- 负责接收客户端消息,推送消息给客户端
package main

import (
	"runtime"
	// "flag"
	"github.com/wuzhc/gmq/internal/gnode"
	// "log"
	// "os"
	// "runtime/pprof"
)

// var cpuprofile = flag.String("cpuprofile", "", "cpu")

func main() {
	runtime.GOMAXPROCS(8)
	gn := gnode.New("./conf.ini")

	// flag.Parse()
	// if *cpuprofile != "" {
	// 	f, err := os.Create(*cpuprofile)
	// 	if err != nil {
	// 		log.Fatalln(err)
	// 	}
	// 	pprof.StartCPUProfile(f)
	// 	defer pprof.StopCPUProfile()
	// }

	gn.Run()
}
