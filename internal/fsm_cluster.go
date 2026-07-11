package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdClusterPut(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Cluster{}
	err = proto.Unmarshal(cmd, item)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Name", item.Name,
	) {
		return
	}
	found, exists := state.ClusterFindByName(item.Name)
	if !exists {
		item.Version = 0
		item.Created = entry.Index
	} else if item.Version != found.Version {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Version mismatch")
		return
	}
	item.Version++
	item.Updated = entry.Index
	state.clusterPut(item, !exists)
	entry.Result.Data, err = proto.Marshal(item)
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
	for range state.ResourceIterByCluster(item.Name) {
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

func queryClusterGet(result *Result, state *State, query []byte) {
	var cluster = &pb.Cluster{}
	err := proto.Unmarshal(query, cluster)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Cluster get request is invalid")
		return
	}
	found, ok := state.ClusterFindByName(cluster.Name)
	if !ok {
		result.Value = ResultErrNotFound
		result.Data = fmt.Appendf(nil, "Cluster does not exist: %s", cluster.Name)
		return
	}
	result.Data, err = proto.Marshal(found)
	if err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}

func queryClusterList(result *Result, state *State, query []byte) {
	var req = &pb.ClusterListRequest{}
	err := proto.Unmarshal(query, req)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Cluster list request is invalid")
		return
	}
	var resp = &pb.ClusterListResponse{}
	for cluster := range state.ClusterIter() {
		resp.Item = append(resp.Item, cluster)
	}
	if result.Data, err = proto.Marshal(resp); err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}
