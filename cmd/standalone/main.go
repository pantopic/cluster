package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pantopic/kitsune/embed"
	"github.com/pantopic/turbokube/embed"

	"github.com/pantopic/cluster"
)

func main() {
	ctx := context.Background()
	cfg := cluster.EnvConfig()
	host := cluster.New(ctx, cfg)

	turbokube.Register(host)
	kitsune.Register(host)

	if err := host.Start(); err != nil {
		panic(err)
	}
	defer host.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	signal.Notify(stop, syscall.SIGTERM)
	<-stop
}
