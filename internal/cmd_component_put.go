package internal

import (
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdComponentPut(entry *Entry, state *State, cmd []byte) (err error) {
	var component = &pb.Component{}
	err = proto.Unmarshal(cmd, component)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Namespace", component.Namespace,
		"Resource", component.Resource,
		"Cluster", component.Cluster,
		"Name", component.Name,
	) {
		return
	}
	found, exists := state.ComponentFindByName(component.Namespace, component.Resource, component.Cluster, component.Name)
	if exists {
		if component.Version != found.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = []byte("Version mismatch")
			return
		}
	} else {
		if _, ok := state.NamespaceFindByName(component.Namespace); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Namespace not found")
			return
		}
		if _, ok := state.ResourceFindByName(component.Namespace, component.Resource); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Resource not found")
			return
		}
		if _, ok := state.ClusterFindByName(component.Cluster); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Cluster not found")
			return
		}
		component.Version = 0
		component.Created = entry.Index
	}
	component.Version++
	component.Updated = entry.Index
	state.componentPut(component, !exists)
	entry.Result.Data, err = proto.Marshal(component)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}
