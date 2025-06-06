package internal

import (
	"github.com/logbn/fastjson-valid"
)

type queryFunc func(*Result, *State, *fastjson.Value) error

var queries = map[string]queryFunc{
	"namespaceFindByName": queryNamespaceFindByName,
}
