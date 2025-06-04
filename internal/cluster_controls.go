package internal

import (
	"context"
	"encoding/json"

	"github.com/logbn/zongzi"
)

type ClusterControls interface {
	NamespaceCreate(ctx context.Context, name string) Namespace
	NamespaceExists(ctx context.Context, name string) bool
	NamespaceFind(ctx context.Context, name string) Namespace
	ResourceCreate(ctx context.Context, item Resource) Resource
}

type clusterControls struct {
	client zongzi.ShardClient
}

var _ ClusterControls = new(clusterControls)

func (cc clusterControls) NamespaceCreate(ctx context.Context, name string) (n Namespace) {
	n.Name = name
	return itemCreate(ctx, cc.client, n)
}

func (cc clusterControls) NamespaceFind(ctx context.Context, name string) (n Namespace) {
	n.Name = name
	return itemFindByName(ctx, cc.client, n)
}

func (cc clusterControls) NamespaceExists(ctx context.Context, name string) bool {
	return cc.NamespaceFind(ctx, name).Created > 0
}

func (cc clusterControls) ResourceCreate(ctx context.Context, item Resource) Resource {
	return itemCreate(ctx, cc.client, item)
}

func itemCreate[T Item](ctx context.Context, client zongzi.ShardClient, item T) T {
	b, err := json.Marshal(map[string]any{
		"type": item.ItemType() + "Create",
		"data": item,
	})
	if err != nil {
		panic(err)
	}
	res, b, err := client.Apply(ctx, b)
	if err != nil || res != ResultSuccess {
		return item
	}
	if err := json.Unmarshal(b, &item); err != nil {
		panic(err.Error())
	}
	return item
}

func itemFindByName[T Item](ctx context.Context, client zongzi.ShardClient, item T) T {
	b, err := json.Marshal(map[string]any{
		"type": item.ItemType() + "Find",
		"data": item,
	})
	if err != nil {
		panic(err)
	}
	res, b, err := client.Read(ctx, b, true)
	if err != nil || res != ResultSuccess {
		return item
	}
	if err := json.Unmarshal(b, &item); err != nil {
		panic(err.Error())
	}
	return item
}
