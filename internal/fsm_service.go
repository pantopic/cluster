package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdServicePut(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Service{}
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
		entry.Result.Data = []byte("Internal services cannot be modified")
		return
	}
	found, exists := state.ServiceFindByName(item.Namespace, item.Resource, item.Name)
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
		item.Version = 0
		item.Created = entry.Index
	}
	item.Version++
	item.Updated = entry.Index
	state.servicePut(item, !exists)
	entry.Result.Data, err = proto.Marshal(item)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}

func cmdServiceDelete(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Service{}
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
		entry.Result.Data = []byte("Internal services cannot be modified")
		return
	}
	entry.Result.Value = state.serviceDelete(item)
	if entry.Result.Value == ResultErrNotFound {
		entry.Result.Data = []byte(`Service Not Found`)
	}
	return
}

func queryServiceGet(result *Result, state *State, query []byte) {
	var service = &pb.Service{}
	err := proto.Unmarshal(query, service)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Service get request is invalid")
		return
	}
	found, ok := state.ServiceFindByName(service.Namespace, service.Resource, service.Name)
	if !ok {
		result.Value = ResultErrNotFound
		result.Data = fmt.Appendf(nil, "Service does not exist: %s", service.Name)
		return
	}
	result.Data, err = proto.Marshal(found)
	if err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}

func queryServiceList(result *Result, state *State, query []byte) {
	var req = &pb.ServiceListRequest{}
	err := proto.Unmarshal(query, req)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Service get request is invalid")
		return
	}
	var resp = &pb.ServiceListResponse{}
	if req.Namespace == "" {
		for service := range state.ServiceIterAll() {
			resp.Item = append(resp.Item, service)
		}
	} else {
		for service := range state.ServiceIter(req.Namespace, req.Resource) {
			resp.Item = append(resp.Item, service)
		}
	}
	if result.Data, err = proto.Marshal(resp); err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}
