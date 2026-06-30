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
		"Cluster", service.Cluster,
		"Name", service.Name,
	) {
		return
	}
	found, exists := state.ServiceFindByName(service.Namespace, service.Resource, service.Cluster, service.Name)
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
		if _, ok := state.ClusterFindByName(service.Cluster); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Cluster not found")
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
