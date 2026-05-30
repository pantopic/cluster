package internal

import (
	"encoding/json"

	"github.com/logbn/fastjson-valid"
)

func cmdResourceCreate(entry *Entry, state *State, v *fastjson.Value) error {
	var item Resource
	if err := json.Unmarshal(v.MarshalTo(nil), &item); err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return nil
	}
	if len(item.Name) == 0 {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte(`Name cannot be empty`)
		return nil
	}
	if _, ok := state.ResourceFindByName(item.NamespaceID, item.Name); ok {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Resource exists")
		return nil
	}
	item.ID = state.incrResourceID()
	item.Created = entry.Index
	item.Updated = entry.Index
	state.resourcePut(item)
	b, err := json.Marshal(item)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return nil
	}
	entry.Result.Value = ResultSuccess
	entry.Result.Data = b
	return nil
}
