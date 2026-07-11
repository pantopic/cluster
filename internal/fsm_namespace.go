package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdNamespacePut(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Namespace{}
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
	if item.Name == NAMESPACE_INTERNAL {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Internal namespace cannot be modified")
		return
	}
	found, exists := state.NamespaceFindByName(item.Name)
	if exists {
		if item.Version != found.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = []byte("Version mismatch")
			return
		}
	} else {
		item.Version = 0
		item.Created = entry.Index
	}
	item.Version++
	item.Updated = entry.Index
	state.namespacePut(item, !exists)
	entry.Result.Data, err = proto.Marshal(item)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}

func cmdNamespaceDelete(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Namespace{}
	err = proto.Unmarshal(cmd, item)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry, "Name", item.Name) {
		return
	}
	if item.Name == NAMESPACE_INTERNAL {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Internal namespace cannot be modified")
		return
	}
	for range state.ResourceIter(item.Name) {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte(`Namespace Not Empty`)
		return
	}
	// TODO - Coordinate deletion with all clusters
	entry.Result.Value = state.namespaceDelete(item)
	if entry.Result.Value == ResultErrNotFound {
		entry.Result.Data = []byte(`Namespace Not Found`)
	}
	return
}

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
