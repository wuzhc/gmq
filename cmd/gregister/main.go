package main

import (
	"flag"

	"github.com/wuzhc/gmq/internal/gregister"
)

func main() {
	gr := gregister.New()

	cfgFile := flag.String("config_file", "", "config file")
	flag.Parse()
	if len(*cfgFile) > 0 {
		gr.SetConfig(*cfgFile)
	}

	gr.Run()
}
