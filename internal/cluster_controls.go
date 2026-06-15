package internal

import (
	"context"

	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

type ClusterControls struct {
	Cluster   *clusterItemControls[*pb.Cluster]
	Component *clusterItemControls[*pb.Component]
	Namespace *clusterItemControls[*pb.Namespace]
	Resource  *clusterItemControls[*pb.Resource]
	Service   *clusterItemControls[*pb.Service]
}

func NewClusterControls(client zongzi.ShardClient) *ClusterControls {
	return &ClusterControls{
		Cluster:   &clusterItemControls[*pb.Cluster]{client},
		Component: &clusterItemControls[*pb.Component]{client},
		Namespace: &clusterItemControls[*pb.Namespace]{client},
		Resource:  &clusterItemControls[*pb.Resource]{client},
		Service:   &clusterItemControls[*pb.Service]{client},
	}
}

type clusterItemControls[T proto.Message] struct {
	client zongzi.ShardClient
}

func (c *clusterItemControls[T]) Put(ctx context.Context, item T) T {
	b, err := proto.Marshal(item)
	if err != nil {
		panic(err)
	}
	switch any(item).(type) {
	case *pb.Cluster:
		b = append(b, CMD_CLUSTER_PUT)
	case *pb.Component:
		b = append(b, CMD_COMPONENT_PUT)
	case *pb.Namespace:
		b = append(b, CMD_NAMESPACE_PUT)
	case *pb.Resource:
		b = append(b, CMD_RESOURCE_PUT)
	case *pb.Service:
		b = append(b, CMD_SERVICE_PUT)
	default:
		panic("unsupported type")
	}
	res, b, err := c.client.Apply(ctx, b)
	if err != nil || res != ResultSuccess {
		return item
	}
	if err := proto.Unmarshal(b, item); err != nil {
		panic(err.Error())
	}
	return item
}

func (c *clusterItemControls[T]) FindByName(ctx context.Context, item T) T {
	b, err := proto.Marshal(item)
	if err != nil {
		panic(err)
	}
	switch any(item).(type) {
	case *pb.Cluster:
		b = append(b, QUERY_CLUSTER_GET)
	case *pb.Component:
		b = append(b, QUERY_COMPONENT_GET)
	case *pb.Namespace:
		b = append(b, QUERY_NAMESPACE_GET)
	case *pb.Resource:
		b = append(b, QUERY_RESOURCE_GET)
	case *pb.Service:
		b = append(b, QUERY_SERVICE_GET)
	default:
		panic("unsupported type")
	}
	res, b, err := c.client.Read(ctx, b, true)
	if err != nil || res != ResultSuccess {
		return item
	}
	if err := proto.Unmarshal(b, item); err != nil {
		panic(err.Error())
	}
	return item
}
