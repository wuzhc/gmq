// gmq节点,可以部署多个以实现分布式
// 功能:
// 	- 负责接收客户端消息,推送消息给客户端
package main

import (
	"github.com/wuzhc/gmq/internal/gnode"
)

func main() {
	gn := gnode.New()
	gn.SetConfig("./conf.ini")
	gn.Run()
}
