package internal

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/pantopic/cluster/pb"
)

func cmdBankPut(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Bank{}
	err = proto.Unmarshal(cmd, item)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	if !validateFieldPresence(entry,
		"Namespace", item.Namespace,
		"Resource", item.Resource,
		"Cluster", item.Cluster,
		"Name", item.Name,
	) {
		return
	}
	if item.Namespace == NAMESPACE_INTERNAL {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Internal banks cannot be modified")
		return
	}
	found, exists := state.BankFindByName(item.Namespace, item.Resource, item.Cluster, item.Name)
	if exists {
		if item.Version != found.Version {
			entry.Result.Value = ResultErrConflict
			entry.Result.Data = []byte("Version mismatch")
			return
		}
		if item.Status == "" {
			item.Status = found.Status
		}
	} else {
		if _, ok := state.NamespaceFindByName(item.Namespace); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Namespace not found")
			return
		}
		if _, ok := state.ResourceFindByName(item.Namespace, item.Resource); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Resource not found")
			return
		}
		if _, ok := state.ClusterFindByName(item.Cluster); !ok {
			entry.Result.Value = ResultErrNotFound
			entry.Result.Data = []byte("Cluster not found")
			return
		}
		item.Version = 0
		item.Created = entry.Index
		item.Status = Status_Pending
	}
	item.Version++
	item.Updated = entry.Index
	state.bankPut(item, !exists)
	entry.Result.Data, err = proto.Marshal(item)
	if err != nil {
		entry.Result.Value = ResultErrInternal
		entry.Result.Data = []byte(err.Error())
		return
	}
	return
}

func cmdBankDelete(entry *Entry, state *State, cmd []byte) (err error) {
	var item = &pb.Bank{}
	err = proto.Unmarshal(cmd, item)
	if err != nil {
		entry.Result.Value = ResultErrSyntactic
		entry.Result.Data = []byte("Protobuf invalid")
		return
	}
	// Validate that the bank exists and is not empty
	if !validateFieldPresence(entry,
		"Namespace", item.Namespace,
		"Resource", item.Resource,
		"Cluster", item.Cluster,
		"Name", item.Name,
	) {
		return
	}
	if item.Namespace == NAMESPACE_INTERNAL {
		entry.Result.Value = ResultErrConflict
		entry.Result.Data = []byte("Internal banks cannot be modified")
		return
	}
	// For bank deletion, we don't need to check if it's empty since banks are just items
	// We just need to delete the bank by its full identifier
	entry.Result.Value = state.bankDelete(item)
	if entry.Result.Value == ResultErrNotFound {
		entry.Result.Data = []byte(`Bank Not Found`)
	}
	return
}

func queryBankGet(result *Result, state *State, query []byte) {
	var bank = &pb.Bank{}
	err := proto.Unmarshal(query, bank)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Bank get request is invalid")
		return
	}
	found, ok := state.BankFindByName(bank.Namespace, bank.Resource, bank.Cluster, bank.Name)
	if !ok {
		result.Value = ResultErrNotFound
		result.Data = fmt.Appendf(nil, "Bank does not exist: %s", bank.Name)
		return
	}
	result.Data, err = proto.Marshal(found)
	if err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}

func queryBankList(result *Result, state *State, query []byte) {
	var req = &pb.BankListRequest{}
	err := proto.Unmarshal(query, req)
	if err != nil {
		result.Value = ResultErrSyntactic
		result.Data = []byte("Bank list request is invalid")
		return
	}
	var resp = &pb.BankListResponse{}
	if req.Namespace == "" {
		for bank := range state.BankIterAll() {
			resp.Item = append(resp.Item, bank)
		}
	} else {
		for bank := range state.BankIter(req.Namespace, req.Resource) {
			resp.Item = append(resp.Item, bank)
		}
	}
	if result.Data, err = proto.Marshal(resp); err != nil {
		result.Value = ResultErrInternal
		result.Data = []byte(err.Error())
		return
	}
}
