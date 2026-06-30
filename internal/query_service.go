package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func queryServiceGet(result *Result, state *State, query []byte) {
	var service = &pb.Service{}
	err := proto.Unmarshal(query, service)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Service get request is invalid")
		return
	}
	found, ok := state.ServiceFindByName(service.Namespace, service.Resource, service.Cluster, service.Name)
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
