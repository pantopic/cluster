package internal

import (
	"github.com/pantopic/cluster/pb"
	extpb "github.com/pantopic/ext-cluster/host-wazero/pb"
)

func validateFieldPresence(entry *Entry, fields ...string) bool {
	for i := 0; i < len(fields); i += 2 {
		name := fields[i]
		val := fields[i+1]
		if len(val) == 0 {
			entry.Result.Value = ResultErrSyntactic
			entry.Result.Data = []byte(name + ` cannot be empty`)
			return false
		}
	}
	return true
}

func extpbResource(res *pb.Resource) *extpb.Resource {
	return &extpb.Resource{
		Namespace: res.GetNamespace(),
		Name:      res.GetName(),
		Version:   res.GetVersion(),
		Created:   res.GetCreated(),
		Updated:   res.GetUpdated(),
		Deleted:   res.GetDeleted(),
		Tags:      res.GetTags(),
		Type:      res.GetType(),
		Status:    res.GetStatus(),
	}
}

func extpbBank(item *pb.Bank) *extpb.Bank {
	return &extpb.Bank{
		Namespace: item.GetNamespace(),
		Resource:  item.GetResource(),
		Cluster:   item.GetCluster(),
		Name:      item.GetName(),
		Version:   item.GetVersion(),
		Created:   item.GetCreated(),
		Updated:   item.GetUpdated(),
		Deleted:   item.GetDeleted(),
		Tags:      item.GetTags(),
		Type:      item.GetType(),
		Status:    item.GetStatus(),
	}
}
