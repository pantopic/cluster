package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// ClusterFindByName returns the cluster with the specified name or ok false if not found.
func (s *State) ClusterFindByName(name string) (c *pb.Cluster, ok bool) {
	res, err := s.txn.First(`cluster`, `id`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		c = res.(*pb.Cluster)
		ok = true
	}
	return
}

// ClusterIter executes a callback for every cluster ordered by id ascending.
// Return true to continue iterating, false to stop.
func (s *State) ClusterIter() iter.Seq[*pb.Cluster] {
	return clusterIter(s.txn.LowerBound(`cluster`, `id`, ""))
}

// ClusterIterByTag executes a callback for every cluster in the cluster matching the specified tag,
// ordered by cluster id ascending. Return true to continue iterating, false to stop.
func (s *State) ClusterIterByTag(tag string) iter.Seq[*pb.Cluster] {
	return clusterIter(s.txn.Get(`cluster`, `Tags`, tag))
}

// ClusterIterUpdatedAfter executes a callback for every cluster in the cluster having an updated index
// greater than the supplied index
func (s *State) ClusterIterUpdatedAfter(index uint64) iter.Seq[*pb.Cluster] {
	return clusterIter(s.txn.LowerBound(`cluster`, `Updated`, index+1))
}

func (s *State) clusterPut(c *pb.Cluster, create bool) {
	err := s.txn.Insert(`cluster`, c)
	if err != nil {
		panic(err)
	}
	if create {
		s.metaIncrVal(`clusters`)
	}
}

func (s *State) clusterDelete(item *pb.Cluster) uint64 {
	return s.itemDelete(`cluster`, item)
}

func clusterIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Cluster] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Cluster](iter)
}
