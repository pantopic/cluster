package internal

import (
	"context"
	"encoding/json"

	"github.com/logbn/zongzi"

	"github.com/pantopic/cluster/proto"
)

type svcControl struct {
	proto.UnimplementedControlServer

	client zongzi.ShardClient
	// authClient zongzi.ShardClient
}

func NewServiceControl(client zongzi.ShardClient) *svcControl {
	return &svcControl{client: client}
}

// Create a namespace
func (s *svcControl) NamespaceCreate(
	ctx context.Context,
	req *proto.NamespaceCreateRequest,
) (res *proto.NamespaceCreateResponse, err error) {
	return &proto.NamespaceCreateResponse{
		Item: &proto.Namespace{
			Name: req.Name,
			Tags: req.Tags,
		},
	}, nil
}

// Delete a namespace
func (s *svcControl) NamespaceDelete(
	ctx context.Context,
	req *proto.NamespaceDeleteRequest,
) (res *proto.NamespaceDeleteResponse, err error) {
	return
}

// Create a resource
func (s *svcControl) ResourceCreate(
	ctx context.Context,
	req *proto.ResourceCreateRequest,
) (res *proto.ResourceCreateResponse, err error) {
	q, err := json.Marshal(map[string]any{
		"type": "resource",
		"meth": "Create",
		"data": Resource{
			NamespaceID: 1,
			Name:        req.Name,
			Tags:        req.Tags,
			Uri:         req.Uri,
		},
	})
	val, data, err := s.client.Apply(ctx, q)
	if err != nil {
		return
	}
	res = &proto.ResourceCreateResponse{
		Code: val,
	}
	if val != ResultSuccess {
		res.Error = string(data)
		return
	}
	var resource Resource
	if err = json.Unmarshal(data, &resource); err != nil {
		res.Code = ResultErrInternal
		res.Error = err.Error()
		return
	}
	res.Item = &proto.Resource{
		Id:        resource.ID,
		Namespace: req.Namespace,
		Name:      resource.Name,
		Tags:      resource.Tags,
		Created:   resource.Created,
		Updated:   resource.Updated,
		Deleted:   resource.Deleted,
		Status:    string(resource.Status),
	}
	return
}

// Delete a resource
func (s *svcControl) ResourceDelete(
	ctx context.Context,
	req *proto.ResourceDeleteRequest,
) (res *proto.ResourceDeleteResponse, err error) {
	return
}
