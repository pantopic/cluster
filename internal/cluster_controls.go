package internal

import (
	"context"
	"encoding/json"

	"github.com/logbn/zongzi"
)

type ClusterControls struct {
	Namespace *clusterItemControls[Namespace]
	Resource  *clusterItemControls[Resource]
}

func NewClusterControls(client zongzi.ShardClient) *ClusterControls {
	return &ClusterControls{
		Namespace: &clusterItemControls[Namespace]{client},
		Resource:  &clusterItemControls[Resource]{client},
	}
}

type clusterItemControls[T Item] struct {
	client zongzi.ShardClient
}

func (c *clusterItemControls[T]) Create(ctx context.Context, item T) T {
	b, err := json.Marshal(map[string]any{
		"type": item.Type(),
		"meth": "Create",
		"data": item,
	})
	if err != nil {
		panic(err)
	}
	res, b, err := c.client.Apply(ctx, b)
	if err != nil || res != ResultSuccess {
		return item
	}
	if err := json.Unmarshal(b, &item); err != nil {
		panic(err.Error())
	}
	return item
}

func (c *clusterItemControls[T]) AddTags(ctx context.Context, item T) T {
	b, err := json.Marshal(map[string]any{
		"type": item.Type(),
		"meth": "TagsAdd",
		"data": item,
	})
	if err != nil {
		panic(err)
	}
	res, b, err := c.client.Apply(ctx, b)
	if err != nil || res != ResultSuccess {
		return item
	}
	if err := json.Unmarshal(b, &item); err != nil {
		panic(err.Error())
	}
	return item
}

func (c *clusterItemControls[T]) FindByName(ctx context.Context, item T) T {
	b, err := json.Marshal(map[string]any{
		"type": item.Type(),
		"meth": "FindByName",
		"data": item,
	})
	if err != nil {
		panic(err)
	}
	res, b, err := c.client.Read(ctx, b, true)
	if err != nil || res != ResultSuccess {
		return item
	}
	if err := json.Unmarshal(b, &item); err != nil {
		panic(err.Error())
	}
	return item
}
