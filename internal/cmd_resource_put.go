package internal

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

// Create a resource
func (s *svcControl) ResourcePut(
	ctx context.Context,
	req *pb.ResourcePutRequest,
) (res *pb.ResourcePutResponse, err error) {
	data, err := proto.Marshal(req.Item)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(data, CMD_RESOURCE_PUT))
	if err != nil {
		return
	}
	res = &pb.ResourcePutResponse{
		Code: val,
	}
	if val != ResultSuccess {
		res.Error = string(data)
		return
	}
	if err = proto.Unmarshal(data, req.Item); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Item = req.Item
	return
}

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
	if existing, ok := state.ResourceFindByName(resource.Namespace, resource.Name); ok {
		if resource.Version != existing.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = fmt.Appendf(nil, "Version mismatch: %s %d != %d", resource.Name, resource.Version, existing.Version)
			return
		}
	} else {
		// TODO: Validate existence of namespace
		resource.Created = entry.Index
	}
	resource.Version++
	resource.Updated = entry.Index
	state.resourcePut(resource, true)
	entry.Result.Data, err = proto.Marshal(resource)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}
