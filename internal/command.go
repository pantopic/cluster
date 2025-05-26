package internal

import (
	"github.com/logbn/fastjson-valid"
)

type cmdFunc func(*Entry, *State, *fastjson.Value) error

var commands = map[string]cmdFunc{
	"namespaceCreate": commandNamespaceCreate,
}
