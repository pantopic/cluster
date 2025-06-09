package internal

import (
	"encoding/json"

	"github.com/logbn/fastjson-valid"
)

func commandTagsAdd[T Item](entry *Entry, state *State, v *fastjson.Value) error {
	var input T
	if err := json.Unmarshal(v.MarshalTo(nil), &input); err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return nil
	}
	if input.GetID() == 0 {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte(`Name cannot be empty`)
		return nil
	}
	var item Item
	var ok bool
	switch input.Type() {
	case Type_Namespace:
		item, ok = state.Namespace(input.GetID())
	case Type_Resource:
		item, ok = state.Resource(input.GetID())
	case Type_Component:
		item, ok = state.Component(input.GetID())
		// case Type_Interface:
		// 	item, ok = state.Interface(input.GetID())
		// case Type_Endpoint:
		// 	item, ok = state.Endpoint(input.GetID())
	}
	if !ok {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte(input.Type() + " not found")
		return nil
	}
	item.Touch(entry.Index)
	item.TagsAdd(input.TagsGet())
	switch input.Type() {
	case Type_Namespace:
		state.namespacePut(item.(Namespace))
	case Type_Resource:
		state.resourcePut(item.(Resource))
	case Type_Component:
		state.componentPut(item.(Component))
		// case Type_Interface:
		// 	state.interfacePut(item.(Interface))
		// case Type_Endpoint:
		// 	state.endpointPut(item.(Endpoint))
	}
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
