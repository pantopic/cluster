package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func queryNamespaceGet(result *Result, state *State, query []byte) {
	var namespace = &pb.Namespace{}
	err := proto.Unmarshal(query, namespace)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Cluster get request is invalid")
		return
	}
	found, ok := state.NamespaceFindByName(namespace.Name)
	if !ok {
		result.Value = ResultErrNotFound
		result.Data = fmt.Appendf(nil, "Namespace does not exist: %s", namespace.Name)
		return
	}
	result.Data, err = proto.Marshal(found)
	if err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}

func queryNamespaceList(result *Result, state *State, query []byte) {
	var req = &pb.NamespaceListRequest{}
	err := proto.Unmarshal(query, req)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Namespace list request is invalid")
		return
	}
	var resp = &pb.NamespaceListResponse{}
	for namespace := range state.NamespaceIter() {
		resp.Item = append(resp.Item, namespace)
	}
	if result.Data, err = proto.Marshal(resp); err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}
