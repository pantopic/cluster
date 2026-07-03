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
}

func NewGrpcServiceControl(client zongzi.ShardClient) *svcControl {
	return &svcControl{client: client}
}

func (s *svcControl) NamespacePut(
	ctx context.Context,
	req *pb.NamespacePutRequest,
) (res *pb.NamespacePutResponse, err error) {
	data, err := proto.Marshal(req.Item)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(data, CMD_RESOURCE_PUT))
	if err != nil {
		return
	}
	res = &pb.NamespacePutResponse{
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
	val, data, err := s.client.Apply(ctx, append(data, CMD_NAMESPACE_DELETE))
	if err != nil {
		return
	}
	res = &pb.NamespaceDeleteResponse{
		Code: val,
	}
	if val != ResultSuccess {
		res.Error = string(data)
		return
	}
	return
}

func (s *svcControl) NamespaceGet(
	ctx context.Context,
	req *pb.NamespaceGetRequest,
) (res *pb.NamespaceGetResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_NAMESPACE_GET), true)
	if err != nil {
		return
	}
	res = &pb.NamespaceGetResponse{
		Code: val,
	}
	if val != ResultSuccess {
		res.Error = string(data)
		return
	}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}

func (s *svcControl) NamespaceList(
	ctx context.Context,
	req *pb.NamespaceListRequest,
) (res *pb.NamespaceListResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_NAMESPACE_LIST), true)
	if err != nil {
		return
	}
	res = &pb.NamespaceListResponse{}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}

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

func (s *svcControl) ResourceDelete(
	ctx context.Context,
	req *pb.ResourceDeleteRequest,
) (res *pb.ResourceDeleteResponse, err error) {
	data, err := proto.Marshal(&pb.Resource{
		Namespace: req.Namespace,
		Name:      req.Name,
	})
	if err != nil {
		return
	}
	val, _, err := s.client.Apply(ctx, append(data, CMD_RESOURCE_DELETE))
	if err != nil {
		return
	}
	res = &pb.ResourceDeleteResponse{
		Code: val,
	}
	return
}

func (s *svcControl) ResourceGet(
	ctx context.Context,
	req *pb.ResourceGetRequest,
) (res *pb.ResourceGetResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_RESOURCE_GET), true)
	if err != nil {
		return
	}
	res = &pb.ResourceGetResponse{}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}

func (s *svcControl) ResourceList(
	ctx context.Context,
	req *pb.ResourceListRequest,
) (res *pb.ResourceListResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_RESOURCE_LIST), true)
	if err != nil {
		return
	}
	res = &pb.ResourceListResponse{}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}

func (s *svcControl) ClusterPut(
	ctx context.Context,
	req *pb.ClusterPutRequest,
) (res *pb.ClusterPutResponse, err error) {
	data, err := proto.Marshal(req.Item)
	if err != nil {
		return
	}
	val, data, err := s.client.Apply(ctx, append(data, CMD_CLUSTER_PUT))
	if err != nil {
		return
	}
	res = &pb.ClusterPutResponse{
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

func (s *svcControl) ClusterDelete(
	ctx context.Context,
	req *pb.ClusterDeleteRequest,
) (res *pb.ClusterDeleteResponse, err error) {
	data, err := proto.Marshal(&pb.Cluster{
		Name: req.Name,
	})
	if err != nil {
		return
	}
	val, _, err := s.client.Apply(ctx, append(data, CMD_CLUSTER_DELETE))
	if err != nil {
		return
	}
	res = &pb.ClusterDeleteResponse{
		Code: val,
	}
	return
}

func (s *svcControl) ClusterGet(
	ctx context.Context,
	req *pb.ClusterGetRequest,
) (res *pb.ClusterGetResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_CLUSTER_GET), true)
	if err != nil {
		return
	}
	res = &pb.ClusterGetResponse{}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}

func (s *svcControl) ClusterList(
	ctx context.Context,
	req *pb.ClusterListRequest,
) (res *pb.ClusterListResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_CLUSTER_LIST), true)
	if err != nil {
		return
	}
	res = &pb.ClusterListResponse{}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}

func (s *svcControl) ServicePut(
	ctx context.Context,
	req *pb.ServicePutRequest,
) (res *pb.ServicePutResponse, err error) {
	if req.Item.Tags == nil {
		res = &pb.ServicePutResponse{
			Code:  ResultErrSemantic,
			Error: "Service requires placement tags",
		}
		return
	}
	if _, ok := req.Item.Tags["placement:cover"]; !ok {
		res = &pb.ServicePutResponse{
			Code:  ResultErrSemantic,
			Error: "Service requires tag placement:cover",
		}
		return
	}
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

func (s *svcControl) ServiceDelete(
	ctx context.Context,
	req *pb.ServiceDeleteRequest,
) (res *pb.ServiceDeleteResponse, err error) {
	data, err := proto.Marshal(&pb.Service{
		Namespace: req.Namespace,
		Resource:  req.Resource,
		Cluster:   req.Cluster,
		Name:      req.Name,
	})
	if err != nil {
		return
	}
	val, _, err := s.client.Apply(ctx, append(data, CMD_SERVICE_DELETE))
	if err != nil {
		return
	}
	res = &pb.ServiceDeleteResponse{
		Code: val,
	}
	return
}

func (s *svcControl) ServiceGet(
	ctx context.Context,
	req *pb.ServiceGetRequest,
) (res *pb.ServiceGetResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_SERVICE_GET), true)
	if err != nil {
		return
	}
	res = &pb.ServiceGetResponse{}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}

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

func (s *svcControl) ComponentPut(
	ctx context.Context,
	req *pb.ComponentPutRequest,
) (res *pb.ComponentPutResponse, err error) {
	if req.Item.Tags == nil {
		res = &pb.ComponentPutResponse{
			Code:  ResultErrSemantic,
			Error: "Component requires placement tags",
		}
		return
	}
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

func (s *svcControl) ComponentDelete(
	ctx context.Context,
	req *pb.ComponentDeleteRequest,
) (res *pb.ComponentDeleteResponse, err error) {
	data, err := proto.Marshal(&pb.Component{
		Namespace: req.Namespace,
		Resource:  req.Resource,
		Cluster:   req.Cluster,
		Name:      req.Name,
	})
	if err != nil {
		return
	}
	val, _, err := s.client.Apply(ctx, append(data, CMD_COMPONENT_DELETE))
	if err != nil {
		return
	}
	res = &pb.ComponentDeleteResponse{
		Code: val,
	}
	return
}

func (s *svcControl) ComponentGet(
	ctx context.Context,
	req *pb.ComponentGetRequest,
) (res *pb.ComponentGetResponse, err error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return
	}
	val, data, err := s.client.Read(ctx, append(data, QUERY_COMPONENT_GET), true)
	if err != nil {
		return
	}
	res = &pb.ComponentGetResponse{}
	if err = proto.Unmarshal(data, res); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Code = val
	return
}

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
