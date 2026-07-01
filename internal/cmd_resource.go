package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdResourcePut(entry *Entry, state *State, cmd []byte) (err error) {
	var resource = &pb.Resource{}
	err = proto.Unmarshal(cmd, resource)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Namespace", resource.Namespace,
		"Name", resource.Name,
	) {
		return
	}
	found, exists := state.ResourceFindByName(resource.Namespace, resource.Name)
	if exists {
		if resource.Version != found.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = fmt.Appendf(nil, "Version mismatch: %s %d != %d", resource.Name, resource.Version, found.Version)
			return
		}
	} else {
		if _, ok := state.NamespaceFindByName(resource.Namespace); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Namespace not found")
			return
		}
		resource.Version = 0
		resource.Created = entry.Index
	}
	resource.Version++
	resource.Updated = entry.Index
	state.resourcePut(resource, !exists)
	entry.Result.Data, err = proto.Marshal(resource)
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
	for range state.ServiceIter(item.Namespace, item.Name) {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte(`Resource Not Empty`)
		return
	}
	for range state.ComponentIter(item.Namespace, item.Name) {
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
