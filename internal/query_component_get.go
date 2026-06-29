package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

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
