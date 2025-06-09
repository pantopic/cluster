package internal

import (
	"encoding/json"
	"fmt"

	"github.com/logbn/fastjson-valid"
)

func queryFindByName[T Item](result *Result, state *State, v *fastjson.Value) error {
	var item T
	var ok bool
	var obj any
	name := string(v.GetStringBytes(`name`))
	switch item.Type() {
	case Type_Namespace:
		obj, ok = state.NamespaceFindByName(name)
	case Type_Resource:
		obj, ok = state.ResourceFindByName(v.GetUint64(`namespaceID`), name)
	}
	if !ok {
		result.Value = ResultErrNotFound
		result.Data = fmt.Appendf(nil, "%s does not exist: %s: %#v", item.Type(), name, obj)
		return nil
	}
	b, err := json.Marshal(obj)
	if err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
	}
	result.Value = ResultSuccess
	result.Data = b
	return nil
}
