// gnode注册器
// 功能:
// 	- 负责gnode注册和注销
package main

import (
	"github.com/wuzhc/gmq/internal/gregister"
)

func main() {
	gr := gregister.New()
	gr.SetConfig("./conf.ini")
	gr.Run()
}
