package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/platform/infserver"
	"os"
	"os/signal"
	"syscall"
)

const (
	SERVICENAME = "infnode"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "config", "", "config path with raft")
	flag.Parse()
}
func main() {
	c := &infserver.Config{}
	if _, err := toml.DecodeFile(configPath, &c); err != nil {
		fmt.Fprintf(os.Stderr, "deocde config file: %s", err)
	}
	s := infserver.New(c)
	if err := s.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "error open %s:%s", SERVICENAME, err)
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalCh:
	}
}
