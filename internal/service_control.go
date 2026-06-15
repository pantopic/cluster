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

// Create a namespace
func (s *svcControl) NamespaceCreate(
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

// Delete a resource
func (s *svcControl) ResourceDelete(
	ctx context.Context,
	req *pb.ResourceDeleteRequest,
) (res *pb.ResourceDeleteResponse, err error) {
	return
}
