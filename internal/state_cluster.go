package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// ClusterFindByName returns the cluster with the specified name or ok false if not found.
func (fsm *State) ClusterFindByName(name string) (s *pb.Cluster, ok bool) {
	res, err := fsm.txn.First(`cluster`, `id`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(*pb.Cluster)
		ok = true
	}
	return
}

// ClusterIter executes a callback for every cluster ordered by id ascending.
// Return true to continue iterating, false to stop.
func (fsm *State) ClusterIter() iter.Seq[*pb.Cluster] {
	return clusterIter(fsm.txn.LowerBound(`cluster`, `id`, ""))
}

// ClusterIterByTag executes a callback for every cluster in the cluster matching the specified tag,
// ordered by cluster id ascending. Return true to continue iterating, false to stop.
func (fsm *State) ClusterIterByTag(tag string) iter.Seq[*pb.Cluster] {
	return clusterIter(fsm.txn.Get(`cluster`, `Tags`, tag))
}

// ClusterIterUpdatedAfter executes a callback for every cluster in the cluster having an updated index
// greater than the supplied index
func (fsm *State) ClusterIterUpdatedAfter(index uint64) iter.Seq[*pb.Cluster] {
	return clusterIter(fsm.txn.LowerBound(`cluster`, `Updated`, index+1))
}

func (fsm *State) clusterPut(s *pb.Cluster, create bool) {
	err := fsm.txn.Insert(`cluster`, s)
	if err != nil {
		panic(err)
	}
	if create {
		fsm.metaIncrVal(`clusters`)
	}
}

func (fsm *State) clusterDelete(s *pb.Cluster) {
	err := fsm.txn.Delete(`cluster`, s)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func clusterIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Cluster] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Cluster](iter)
}
