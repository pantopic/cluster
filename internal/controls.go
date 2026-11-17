package internal

import (
	"context"

	"github.com/logbn/zongzi"
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

type ClusterControls struct {
	Bank      *clusterItemControls[*pb.Bank]
	Cluster   *clusterItemControls[*pb.Cluster]
	Namespace *clusterItemControls[*pb.Namespace]
	Resource  *clusterItemControls[*pb.Resource]
	Service   *clusterItemControls[*pb.Service]
	updated   bool
}

func NewClusterControls(client zongzi.ShardClient) *ClusterControls {
	c := &ClusterControls{}
	c.Bank = &clusterItemControls[*pb.Bank]{c, client}
	c.Cluster = &clusterItemControls[*pb.Cluster]{c, client}
	c.Namespace = &clusterItemControls[*pb.Namespace]{c, client}
	c.Resource = &clusterItemControls[*pb.Resource]{c, client}
	c.Service = &clusterItemControls[*pb.Service]{c, client}
	return c
}

type clusterItemControls[T proto.Message] struct {
	ctrl   *ClusterControls
	client zongzi.ShardClient
}

func (c *clusterItemControls[T]) Put(ctx context.Context, item T) T {
	b, err := proto.Marshal(item)
	if err != nil {
		panic(err)
	}
	switch any(item).(type) {
	case *pb.Bank:
		b = append(b, CMD_BANK_PUT)
	case *pb.Cluster:
		b = append(b, CMD_CLUSTER_PUT)
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
	c.ctrl.updated = true
	return item
}

func (c *clusterItemControls[T]) FindByName(ctx context.Context, item T) T {
	b, err := proto.Marshal(item)
	if err != nil {
		panic(err)
	}
	switch any(item).(type) {
	case *pb.Bank:
		b = append(b, QUERY_BANK_GET)
	case *pb.Cluster:
		b = append(b, QUERY_CLUSTER_GET)
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
