package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdComponentPut(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Component{}
	err = proto.Unmarshal(cmd, item)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Namespace", item.Namespace,
		"Resource", item.Resource,
		"Cluster", item.Cluster,
		"Name", item.Name,
	) {
		return
	}
	if item.Namespace == NAMESPACE_INTERNAL {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Internal components cannot be modified")
		return
	}
	found, exists := state.ComponentFindByName(item.Namespace, item.Resource, item.Cluster, item.Name)
	if exists {
		if item.Version != found.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = []byte("Version mismatch")
			return
		}
	} else {
		if _, ok := state.NamespaceFindByName(item.Namespace); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Namespace not found")
			return
		}
		if _, ok := state.ResourceFindByName(item.Namespace, item.Resource); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Resource not found")
			return
		}
		if _, ok := state.ClusterFindByName(item.Cluster); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Cluster not found")
			return
		}
		item.Version = 0
		item.Created = entry.Index
	}
	item.Version++
	item.Updated = entry.Index
	state.componentPut(item, !exists)
	entry.Result.Data, err = proto.Marshal(item)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}

func cmdComponentDelete(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Component{}
	err = proto.Unmarshal(cmd, item)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	// Validate that the component exists and is not empty
	if !validateFieldPresence(entry,
		"Namespace", item.Namespace,
		"Resource", item.Resource,
		"Cluster", item.Cluster,
		"Name", item.Name,
	) {
		return
	}
	if item.Namespace == NAMESPACE_INTERNAL {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Internal components cannot be modified")
		return
	}
	// For component deletion, we don't need to check if it's empty since components are just items
	// We just need to delete the component by its full identifier
	entry.Result.Value = state.componentDelete(item)
	if entry.Result.Value == ResultErrNotFound {
		entry.Result.Data = []byte(`Component Not Found`)
	}
	return
}

func queryComponentGet(result *Result, state *State, query []byte) {
	var component = &pb.Component{}
	err := proto.Unmarshal(query, component)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Component get request is invalid")
		return
	}
	found, ok := state.ComponentFindByName(component.Namespace, component.Resource, component.Cluster, component.Name)
	if !ok {
		result.Value = ResultErrNotFound
		result.Data = fmt.Appendf(nil, "Component does not exist: %s", component.Name)
		return
	}
	result.Data, err = proto.Marshal(found)
	if err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}

func queryComponentList(result *Result, state *State, query []byte) {
	var req = &pb.ComponentListRequest{}
	err := proto.Unmarshal(query, req)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Component list request is invalid")
		return
	}
	var resp = &pb.ComponentListResponse{}
	if req.Namespace == "" {
		for component := range state.ComponentIterAll() {
			resp.Item = append(resp.Item, component)
		}
	} else {
		for component := range state.ComponentIter(req.Namespace, req.Resource) {
			resp.Item = append(resp.Item, component)
		}
	}
	if result.Data, err = proto.Marshal(resp); err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}
