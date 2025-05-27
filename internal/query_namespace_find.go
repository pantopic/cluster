package internal

import (
	"encoding/json"
	"fmt"

	"github.com/logbn/fastjson-valid"
)

func queryNamespaceFind(result *Result, state *State, v *fastjson.Value) error {
	name := string(v.GetStringBytes())
	obj, ok := state.NamespaceByName(name)
	if !ok {
		result.Value = ResultErrNotFound
		result.Data = fmt.Appendf(nil, "Namespace does not exist: %s: %#v", name, obj)
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
