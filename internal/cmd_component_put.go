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
		"Name", component.Name,
	) {
		return
	}
	if existing, ok := state.ComponentFindByName(component.Namespace, component.Resource, component.Name); ok {
		if component.Version != existing.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = []byte("Version mismatch")
			return
		}
	} else {
		// TODO: Validate existence of namespace
		// TODO: Validate existence of resource
		component.Created = entry.Index
	}
	component.Version++
	component.Updated = entry.Index
	state.componentPut(component, true)
	entry.Result.Data, err = proto.Marshal(component)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}
