package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

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
