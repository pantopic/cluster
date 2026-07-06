package internal

import (
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdServicePut(entry *Entry, state *State, cmd []byte) (err error) {
	var service = &pb.Service{}
	err = proto.Unmarshal(cmd, service)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Namespace", service.Namespace,
		"Resource", service.Resource,
		"Name", service.Name,
	) {
		return
	}
	found, exists := state.ServiceFindByName(service.Namespace, service.Resource, service.Name)
	if exists {
		if service.Version != found.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = []byte("Version mismatch")
			return
		}
	} else {
		if _, ok := state.NamespaceFindByName(service.Namespace); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Namespace not found")
			return
		}
		if _, ok := state.ResourceFindByName(service.Namespace, service.Resource); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Resource not found")
			return
		}
		service.Version = 0
		service.Created = entry.Index
	}
	service.Version++
	service.Updated = entry.Index
	state.servicePut(service, !exists)
	entry.Result.Data, err = proto.Marshal(service)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}

func cmdServiceDelete(entry *Entry, state *State, cmd []byte) (err error) {
	var svc = &pb.Service{}
	err = proto.Unmarshal(cmd, svc)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Namespace", svc.Namespace,
		"Resource", svc.Resource,
		"Name", svc.Name,
	) {
		return
	}
	entry.Result.Value = state.serviceDelete(svc)
	if entry.Result.Value == ResultErrNotFound {
		entry.Result.Data = []byte(`Service Not Found`)
	}
	return
}
