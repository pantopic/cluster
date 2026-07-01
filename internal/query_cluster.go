package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

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
