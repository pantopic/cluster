package internal

import (
	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdNamespacePut(entry *Entry, state *State, cmd []byte) (err error) {
	var namespace = &pb.Namespace{}
	err = proto.Unmarshal(cmd, namespace)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Name", namespace.Name,
	) {
		return
	}
	found, exists := state.NamespaceFindByName(namespace.Name)
	if exists {
		if namespace.Version != found.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = []byte("Version mismatch")
			return
		}
	} else {
		namespace.Version = 0
		namespace.Created = entry.Index
	}
	namespace.Version++
	namespace.Updated = entry.Index
	state.namespacePut(namespace, !exists)
	entry.Result.Data, err = proto.Marshal(namespace)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}
