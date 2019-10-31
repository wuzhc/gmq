package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"

	"github.com/wuzhc/gmq/internal/gnode"
)

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:9512", nil)
	}()

	gn := gnode.New()

	cfgFile := flag.String("config_file", "", "config file")
	flag.Parse()
	if len(*cfgFile) > 0 {
		gn.SetConfig(*cfgFile)
	}

	gn.Run()
}
