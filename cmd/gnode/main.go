package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"

	"github.com/kardianos/service"
	"github.com/wuzhc/gmq/internal/gnode"
)

var logger service.Logger

type program struct {
	gn   *gnode.Gnode
	once sync.Once
}

func (p *program) Start(s service.Service) error {
	// Start should not block. Do the actual work async.
	cfg := gnode.NewGnodeConfig()
	p.gn = gnode.New(cfg)

	go func() {
		p.gn.Run()
	}()

	go func() {
		http.ListenAndServe("0.0.0.0:8877", nil)
	}()

	return nil
}

func (p *program) Stop(s service.Service) error {
	// Stop should not block. Return with a few seconds.
	p.once.Do(func() {
		p.gn.Exit()
	})
	return nil
}

func main() {
	svcConfig := &service.Config{
		Name:        "gmq-node",
		DisplayName: "gmq-node",
		Description: "This is an gmq-node service.",
		Arguments:   []string{"-config_file="},
		Option:      make(map[string]interface{}),
	}

	svcConfig.Option["LogOutput"] = true

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	logger, err = s.Logger(nil)
	if err != nil {
		log.Fatal(err)
	}

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "install":
			if err := s.Install(); err != nil {
				logger.Error(err)
			} else {
				logger.Info("gnode.service install success!")
			}
			if err := s.Start(); err != nil {
				logger.Error(err)
			} else {
				logger.Info("gnode.service start success!")
			}
			return
		case "uninstall":
			if err := s.Stop(); err != nil {
				logger.Error(err)
			} else {
				logger.Info("gnode.service stop success!")
			}
			if err := s.Uninstall(); err != nil {
				logger.Error(err)
			} else {
				logger.Info("gnode.service uninstall success!")
			}
			return
		case "start":
			if err := s.Start(); err != nil {
				logger.Error(err)
			} else {
				logger.Info("gnode.service start success!")
			}
			return
		case "stop":
			if err := s.Stop(); err != nil {
				logger.Error(err)
			} else {
				logger.Info("gnode.service stop success!")
			}
			return
		case "restart":
			if err := s.Stop(); err != nil {
				logger.Error(err)
			} else {
				logger.Info("gnode.service stop success!")
			}
			if err := s.Start(); err != nil {
				logger.Error(err)
			} else {
				logger.Info("gnode.service start success!")
			}
			return
		case "status":
			if status, err := s.Status(); err != nil {
				logger.Error(err)
			} else {
				if int(status) == 1 {
					logger.Info("gnode.service is running.")
				} else {
					logger.Info("gnode.service is stop.")
				}
			}
			return
		}
	}

	if err = s.Run(); err != nil {
		log.Fatalln(err)
	}
}
