package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"
)

// Cluster returns the cluster with the specified id or ok false if not found.
func (fsm *State) Cluster(id uint64) (s Cluster, ok bool) {
	res, err := fsm.txn.First(`cluster`, `id`, id)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Cluster)
		ok = true
	}
	return
}

// ClusterFindByName returns the cluster with the specified name or ok false if not found.
func (fsm *State) ClusterFindByName(name string) (s Cluster, ok bool) {
	res, err := fsm.txn.First(`cluster`, `Name`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Cluster)
		ok = true
	}
	return
}

// ClusterIter executes a callback for every cluster ordered by id ascending.
// Return true to continue iterating, false to stop.
func (fsm *State) ClusterIter() iter.Seq[Cluster] {
	return clusterIter(fsm.txn.LowerBound(`cluster`, `id`, uint64(0)))
}

// ClusterIterByTag executes a callback for every cluster in the cluster matching the specified tag,
// ordered by cluster id ascending. Return true to continue iterating, false to stop.
func (fsm *State) ClusterIterByTag(tag string) iter.Seq[Cluster] {
	return clusterIter(fsm.txn.Get(`cluster`, `Tags`, tag))
}

// ClusterIterUpdatedAfter executes a callback for every cluster in the cluster having an updated index
// greater than the supplied index
func (fsm *State) ClusterIterUpdatedAfter(index uint64) iter.Seq[Cluster] {
	return clusterIter(fsm.txn.LowerBound(`cluster`, `Updated`, index+1))
}

func (fsm *State) clusterPut(s Cluster) {
	err := fsm.txn.Insert(`cluster`, s)
	if err != nil {
		panic(err)
	}
}

func (fsm *State) clusterDelete(s Cluster) {
	err := fsm.txn.Delete(`cluster`, s)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func (fsm *State) clusterTouch(id, index uint64) {
	if s, ok := fsm.Cluster(id); ok {
		s.Updated = index
		fsm.clusterPut(s)
	}
}

func clusterIter(iter memdb.ResultIterator, err error) iter.Seq[Cluster] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Cluster](iter)
}
