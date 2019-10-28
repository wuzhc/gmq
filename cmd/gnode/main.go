package main

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/wuzhc/gmq/internal/gnode"
)

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:9512", nil)
	}()

	gn := gnode.New()
	// gn.SetConfig("./conf.ini")
	gn.Run()
}
