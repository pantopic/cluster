package internal

import (
	"encoding/json"

	"github.com/logbn/fastjson-valid"
)

func cmdNamespaceCreate(entry *Entry, state *State, v *fastjson.Value) error {
	name := string(v.GetStringBytes(`name`))
	if len(name) == 0 {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte(`Name cannot be empty`)
		return nil
	}
	if _, ok := state.NamespaceFindByName(name); ok {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Namespace exists")
		return nil
	}
	obj := Namespace{
		ID:      state.incrNamespaceID(),
		Name:    name,
		Created: entry.Index,
		Updated: entry.Index,
	}
	state.namespacePut(obj)
	b, err := json.Marshal(obj)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return nil
	}
	entry.Result.Value = ResultSuccess
	entry.Result.Data = b
	return nil
}
