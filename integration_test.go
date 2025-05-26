package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/logbn/zongzi"
	// "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pantopic/microstack/internal"
)

func TestMain(t *testing.T) {
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
	ctrls := make([]internal.ControllerCluster, len(peers))
	cfg := getConfig()
	cfg.HostName = "127.0.0.1"
	logger := getLogger(cfg)
	start := func(t *testing.T, peers []string, class string, portSets ...[]uint16) {
		for i, ports := range portSets {
			ctrls[i] = internal.NewControllerCluster(logger)
			if agents[i], err = zongzi.NewAgent(cfg.ClusterName, peers,
				zongzi.WithDirRaft(fmt.Sprintf(basedir+`/%d/raft`, i)),
				zongzi.WithDirWAL(fmt.Sprintf(basedir+`/%d/wal`, i)),
				zongzi.WithAddrApi(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[0])),
				zongzi.WithAddrRaft(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[1])),
				zongzi.WithAddrGossip(fmt.Sprintf(`%s:%d`, cfg.HostName, ports[2])),
				zongzi.WithHostTags(`microstack:host`, `host:class=`+class),
				zongzi.WithRaftEventListener(ctrls[i]),
			); err != nil {
				panic(err)
			}
			go start(ctx, agents[i], ctrls[i], logger)
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
		start(t, peers, `main`,
			[]uint16{18011, 18012, 18013},
			[]uint16{18021, 18022, 18023},
			[]uint16{18031, 18032, 18033},
		)
	})
	client := agents[0].Client(internal.ShardID)
	t.Run(`namespaces`, func(t *testing.T) {
		for _, namespace := range []string{"system", "default"} {
			q, err := json.Marshal(map[string]any{
				"type": "namespaceFind",
				"data": internal.Namespace{Name: namespace},
			})
			require.Nil(t, err)
			var val uint64
			var data []byte
			require.True(t, await(10, 100, func() bool {
				val, data, err = client.Read(ctx, q, false)
				return err == nil && val == internal.ResultSuccess && len(data) > 0
			}), `%d %s %v`, val, data, err)
		}
	})
	time.Sleep(time.Second)
	logger.Infof("Stopping")
	for i := range len(peers) {
		ctrls[i].Stop()
	}
	for i := range len(peers) {
		agents[i].Stop()
	}
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
