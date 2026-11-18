package main

import (
	"github.com/pantopic/ext-cluster/sdk-go"
	"github.com/pantopic/ext-cluster/sdk-go/pb"
)

func init() {
	cluster.Init(
		cluster.WithComponentReconciler(reconcile),
	)
}

func main() {
}

func reconcile(component *pb.Component) uint32 {
	name := component.Namespace + `.` + component.Resource + `.` + component.Name
	var found bool
	var s *pb.Shard
	for s = range cluster.ShardIter() {
		if s.Name == name+`.test` {
			found = true
			break
		}
	}
	if !found {
		_, err := cluster.ShardCreate(`test`, `pantopic/cluster-test/storage/test`,
			cluster.WithTags(component.Tags))
		if err != nil {
			println(err)
			return cluster.StatusPending
		}
	}
	return cluster.StatusActive
}
