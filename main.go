package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pantopic/kitsune/embed"
	"github.com/pantopic/turbokube/embed"
)

func main() {
	ctx := context.Background()
	cfg := getConfig()
	logger := getLogger(cfg)
	c := newCluster(ctx, cfg, logger)

	turbokube.Register(c)
	kitsune.Register(c)

	if err := c.Start(); err != nil {
		panic(err)
	}
	defer c.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	<-stop
}
