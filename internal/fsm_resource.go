package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdResourcePut(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Resource{}
	err = proto.Unmarshal(cmd, item)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Namespace", item.Namespace,
		"Name", item.Name,
	) {
		return
	}
	if item.Namespace == NAMESPACE_INTERNAL {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Internal resources cannot be modified")
		return
	}
	found, exists := state.ResourceFindByName(item.Namespace, item.Name)
	if exists {
		if item.Version != found.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = fmt.Appendf(nil, "Version mismatch: %s %d != %d", item.Name, item.Version, found.Version)
			return
		}
	} else {
		if _, ok := state.NamespaceFindByName(item.Namespace); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Namespace not found")
			return
		}
		item.Version = 0
		item.Created = entry.Index
	}
	item.Version++
	item.Updated = entry.Index
	state.resourcePut(item, !exists)
	entry.Result.Data, err = proto.Marshal(item)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}

func cmdResourceDelete(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Resource{}
	err = proto.Unmarshal(cmd, item)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Namespace", item.Namespace,
		"Name", item.Name,
	) {
		return
	}
	if item.Namespace == NAMESPACE_INTERNAL {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Internal resources cannot be modified")
		return
	}
	for range state.ServiceIter(item.Namespace, item.Name) {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte(`Resource Not Empty`)
		return
	}
	for range state.BankIter(item.Namespace, item.Name) {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte(`Resource Not Empty`)
		return
	}
	entry.Result.Value = state.resourceDelete(item)
	if entry.Result.Value == ResultErrNotFound {
		entry.Result.Data = []byte(`Resource Not Found`)
	}
	return
}
func queryResourceGet(result *Result, state *State, query []byte) {
	var resource = &pb.Resource{}
	err := proto.Unmarshal(query, resource)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Resource get request is invalid")
		return
	}
	found, ok := state.ResourceFindByName(resource.Namespace, resource.Name)
	if !ok {
		result.Value = ResultErrNotFound
		result.Data = fmt.Appendf(nil, "Resource does not exist: %s", resource.Name)
		return
	}
	result.Data, err = proto.Marshal(found)
	if err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}

func queryResourceList(result *Result, state *State, query []byte) {
	var req = &pb.ResourceListRequest{}
	err := proto.Unmarshal(query, req)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Resource list request is invalid")
		return
	}
	var resp = &pb.ResourceListResponse{}
	if req.Namespace == "" {
		for resource := range state.ResourceIterAll() {
			resp.Item = append(resp.Item, resource)
		}
	} else {
		for resource := range state.ResourceIter(req.Namespace) {
			resp.Item = append(resp.Item, resource)
		}
	}
	if result.Data, err = proto.Marshal(resp); err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}
