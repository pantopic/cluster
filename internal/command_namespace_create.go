package internal

import (
	"github.com/logbn/fastjson-valid"
)

func commandNamespaceCreate(entry *Entry, state *State, v *fastjson.Value) error {
	name := string(v.GetStringBytes(`name`))
	if len(name) == 0 {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte(`Name cannot be empty`)
		return nil
	}
	if _, ok := state.NamespaceByName(name); ok {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Namespace exists")
		return nil
	}
	state.namespacePut(Namespace{
		ID:      state.namespaceIncr(),
		Name:    name,
		Created: entry.Index,
		Updated: entry.Index,
	})
	return nil
}
