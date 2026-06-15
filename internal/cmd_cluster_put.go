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
	if existing, ok := state.ClusterFindByName(cluster.Name); ok {
		if cluster.Version != existing.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = []byte("Version mismatch")
			return
		}
	} else {
		cluster.Created = entry.Index
	}
	cluster.Version++
	cluster.Updated = entry.Index
	state.clusterPut(cluster, true)
	entry.Result.Data, err = proto.Marshal(cluster)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}
