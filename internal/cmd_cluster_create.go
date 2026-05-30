package internal

import (
	"encoding/json"

	"github.com/logbn/fastjson-valid"
)

func cmdClusterCreate(entry *Entry, state *State, v *fastjson.Value) error {
	name := string(v.GetStringBytes(`name`))
	if len(name) == 0 {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte(`Name cannot be empty`)
		return nil
	}
	obj, ok := state.ClusterFindByName(name)
	if ok {
		entry.Result.Value = ResultErrExists
	} else {
		obj = Cluster{
			ID:      state.incrClusterID(),
			Name:    name,
			Created: entry.Index,
			Updated: entry.Index,
		}
		state.clusterPut(obj)
		entry.Result.Value = ResultSuccess
	}
	b, err := json.Marshal(obj)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return nil
	}
	entry.Result.Data = b
	return nil
}
