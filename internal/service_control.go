package internal

import (
	"context"

	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

type svcControl struct {
	pb.UnimplementedControlServer

	client zongzi.ShardClient
	// authClient zongzi.ShardClient
}

func NewServiceControl(client zongzi.ShardClient) *svcControl {
	return &svcControl{client: client}
}

// Create or Update a namespace
func (s *svcControl) NamespacePut(
	ctx context.Context,
	req *pb.NamespacePutRequest,
) (res *pb.NamespacePutResponse, err error) {
	return &pb.NamespacePutResponse{
		Item: req.Item,
	}, nil
}

// Delete a namespace
func (s *svcControl) NamespaceDelete(
	ctx context.Context,
	req *pb.NamespaceDeleteRequest,
) (res *pb.NamespaceDeleteResponse, err error) {
	data, err := proto.Marshal(&pb.Namespace{
		Name: req.Name,
	})
	if err != nil {
		return
	}
	val, _, err := s.client.Apply(ctx, append(data, CMD_NAMESPACE_DELETE))
	if err != nil {
		return
	}
	res = &pb.NamespaceDeleteResponse{
		Code: val,
	}
	return
}

// Create or Update a resource
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

// Delete a resource
func (s *svcControl) ResourceDelete(
	ctx context.Context,
	req *pb.ResourceDeleteRequest,
) (res *pb.ResourceDeleteResponse, err error) {
	return
}

// Create or Update a service
func (s *svcControl) ServicePut(
	ctx context.Context,
	req *pb.ServicePutRequest,
) (res *pb.ServicePutResponse, err error) {
	data, err := proto.Marshal(req.Item)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(data, CMD_SERVICE_PUT))
	if err != nil {
		return
	}
	res = &pb.ServicePutResponse{
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

// List services
func (s *svcControl) ServiceList(
	ctx context.Context,
	req *pb.ServiceListRequest,
) (res *pb.ServiceListResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_SERVICE_LIST), true)
	if err != nil {
		return
	}
	res = &pb.ServiceListResponse{}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}

// Create or Update a component
func (s *svcControl) ComponentPut(
	ctx context.Context,
	req *pb.ComponentPutRequest,
) (res *pb.ComponentPutResponse, err error) {
	data, err := proto.Marshal(req.Item)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(data, CMD_COMPONENT_PUT))
	if err != nil {
		return
	}
	res = &pb.ComponentPutResponse{
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

// List components
func (s *svcControl) ComponentList(
	ctx context.Context,
	req *pb.ComponentListRequest,
) (res *pb.ComponentListResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_COMPONENT_LIST), true)
	if err != nil {
		return
	}
	res = &pb.ComponentListResponse{}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}
