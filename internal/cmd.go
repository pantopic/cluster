package internal

import (
	"github.com/logbn/fastjson-valid"
)

type cmdFunc func(*Entry, *State, *fastjson.Value) error

var commands = map[string]cmdFunc{
	"cluster.Create":    cmdClusterCreate,
	"cluster.TagsAdd":   cmdTagsAdd[Cluster],
	"namespace.Create":  cmdNamespaceCreate,
	"namespace.TagsAdd": cmdTagsAdd[Namespace],
	"resource.Create":   cmdResourceCreate,
	"resource.TagsAdd":  cmdTagsAdd[Resource],
}
