package internal

import (
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdClusterPut(entry *Entry, state *State, cmd []byte) (err error) {
	var cluster = &pb.Cluster{}
	err = proto.Unmarshal(cmd, cluster)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Name", cluster.Name,
	) {
		return
	}
	found, exists := state.ClusterFindByName(cluster.Name)
	if !exists {
		cluster.Version = 0
		cluster.Created = entry.Index
	} else if cluster.Version != found.Version {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Version mismatch")
		return
	}
	cluster.Version++
	cluster.Updated = entry.Index
	state.clusterPut(cluster, !exists)
	entry.Result.Data, err = proto.Marshal(cluster)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}

func cmdClusterDelete(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Cluster{}
	err = proto.Unmarshal(cmd, item)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry, "Name", item.Name) {
		return
	}
	for range state.ResourceIter(item.Name) {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte(`Cluster Not Empty`)
		return
	}
	entry.Result.Value = state.clusterDelete(item)
	if entry.Result.Value == ResultErrNotFound {
		entry.Result.Data = []byte(`Cluster Not Found`)
	}
	return
}
