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
	if existing, ok := state.ServiceFindByName(service.Namespace, service.Resource, service.Name); ok {
		if service.Version != existing.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = []byte("Version mismatch")
			return
		}
	} else {
		service.Created = entry.Index
	}
	service.Version++
	service.Updated = entry.Index
	state.servicePut(service, true)
	entry.Result.Data, err = proto.Marshal(service)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}
