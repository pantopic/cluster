package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/logbn/zongzi"
	// "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(t *testing.T) {
	// zongzi.SetLogLevel(zongzi.LogLevelDebug)
	basedir := `./tmp/microstack`
	var ctx = context.Background()
	var mutex sync.Mutex
	var err error
	os.RemoveAll(basedir)
	peers := []string{
		`127.0.0.1:18011`,
		`127.0.0.1:18021`,
		`127.0.0.1:18031`,
	}
	agents := make([]*zongzi.Agent, len(peers))
	start := func(t *testing.T, peers []string, class string, notifyCommit bool, portSets ...[]uint16) {
		for i, ports := range portSets {
			cfg := getConfig()
			cfg.HostName = "127.0.0.1"
			if agents[i], err = zongzi.NewAgent(cfg.ClusterName, peers,
				zongzi.WithDirRaft(fmt.Sprintf(basedir+`/%d/raft`, i)),
				zongzi.WithDirWAL(fmt.Sprintf(basedir+`/%d/wal`, i)),
				zongzi.WithAddrApi(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[0])),
				zongzi.WithAddrRaft(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[1])),
				zongzi.WithAddrGossip(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[2])),
				zongzi.WithHostTags(`microstack:host`),
			); err != nil {
				panic(err)
			}
			go start(ctx, agents[i], getLogger(cfg))
		}
		time.Sleep(time.Second)
		// 10 seconds to start the cluster.
		require.True(t, await(10, 100, func() bool {
			mutex.Lock()
			defer mutex.Unlock()
			for j := range agents {
				if agents[j] == nil || agents[j].Status() != zongzi.AgentStatus_Ready {
					return false
				}
			}
			return true
		}), `%#v`, agents)
	}
	t.Run(`start`, func(t *testing.T) {
		start(t, peers, `concurrent`, false,
			[]uint16{18011, 18012, 18013},
			[]uint16{18021, 18022, 18023},
			[]uint16{18031, 18032, 18033},
		)
	})
	log.Println("Done")
}

func await(d, n time.Duration, fn func() bool) bool {
	for i := 0; i < int(n); i++ {
		if fn() {
			return true
		}
		time.Sleep(d * time.Second / n)
	}
	return false
}
