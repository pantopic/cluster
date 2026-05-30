package internal

import (
	"github.com/logbn/fastjson-valid"
)

type queryFunc func(*Result, *State, *fastjson.Value) error

var queries = map[string]queryFunc{
	"cluster.FindByName":   queryFindByName[Cluster],
	"namespace.FindByName": queryFindByName[Namespace],
	"resource.FindByName":  queryFindByName[Resource],
}
