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
	b, err := json.Marshal(map[string]any{
		"type": "namespaceCreate",
		"data": n,
	})
	if err != nil {
		panic(err)
	}
	res, b, err := cc.client.Apply(ctx, b)
	if err != nil || res != ResultSuccess {
		return
	}
	if err := json.Unmarshal(b, &n); err != nil {
		panic(err.Error())
	}
	return
}

func (cc clusterControls) NamespaceFind(ctx context.Context, name string) (n Namespace) {
	b, err := json.Marshal(map[string]any{
		"type": "namespaceFind",
		"data": name,
	})
	if err != nil {
		panic(err)
	}
	res, b, err := cc.client.Read(ctx, b, true)
	if err != nil || res != ResultSuccess {
		return
	}
	if err := json.Unmarshal(b, &n); err != nil {
		panic(err.Error())
	}
	return
}

func (cc clusterControls) NamespaceExists(ctx context.Context, name string) bool {
	return cc.NamespaceFind(ctx, name).Created > 0
}

func (cc clusterControls) ResourceCreate(ctx context.Context, item Resource) Resource {
	b, err := json.Marshal(map[string]any{
		"type": "resourceCreate",
		"data": item,
	})
	if err != nil {
		panic(err)
	}
	res, b, err := cc.client.Apply(ctx, b)
	if err != nil || res != ResultSuccess {
		return item
	}
	if err := json.Unmarshal(b, &item); err != nil {
		panic(err.Error())
	}
	return item
}
