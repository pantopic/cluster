package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// ResourceFindByName returns the resource with the specified name or ok false if not found.
func (s *State) ResourceFindByName(namespace, name string) (r *pb.Resource, ok bool) {
	res, err := s.txn.First(`resource`, `id`, namespace, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		r = res.(*pb.Resource)
		ok = true
	}
	return
}

// ResourceIterAll returns an iterator enumerating every resource in the resource ordered by resource id ascending.
// Return true to continue iterating, false to stop.
func (s *State) ResourceIterAll() iter.Seq[*pb.Resource] {
	return resourceIter(s.txn.LowerBound(`resource`, `id`, "", "", ""))
}

// ResourceIterByNamespace returns an iterator enumerating every child of a resource.
func (s *State) ResourceIter(namespace string) iter.Seq[*pb.Resource] {
	return resourceIter(s.txn.Get(`resource`, `Namespace`, namespace))
}

// ResourceIterByCluster returns an iterator enumerating every child of a resource.
func (s *State) ResourceIterByCluster(cluster string) iter.Seq[*pb.Resource] {
	return resourceIter(s.txn.Get(`resource`, `Cluster`, cluster))
}

// ResourceIterByUri returns an iterator enumerating every resource of a specific type.
func (s *State) ResourceIterByUri(uri string) iter.Seq[*pb.Resource] {
	return resourceIter(s.txn.Get(`resource`, `Uri`, uri))
}

// ResourceIterByTag returns an iterator enumerating every resource in the resource matching the specified tag,
// ordered by resource id ascending. Return true to continue iterating, false to stop.
func (s *State) ResourceIterByTag(tag string) iter.Seq[*pb.Resource] {
	return resourceIter(s.txn.Get(`resource`, `Tags`, tag))
}

// ResourceIterUpdatedAfter returns an iterator enumerating every resource in the resource having an updated index
// greater than the supplied index
func (s *State) ResourceIterUpdatedAfter(index uint64) iter.Seq[*pb.Resource] {
	return resourceIter(s.txn.LowerBound(`resource`, `Updated`, index+1))
}

func (s *State) resourcePut(r *pb.Resource, create bool) {
	err := s.txn.Insert(`resource`, r)
	if err != nil {
		panic(err)
	}
	if create {
		s.metaIncrVal(`resources`)
	}
}

func (s *State) resourceDelete(item *pb.Resource) uint64 {
	return s.itemDelete(`resource`, item)
}

func (s *State) resourceTouch(namespace, name string, index uint64) {
	if r, ok := s.ResourceFindByName(namespace, name); ok {
		r.Updated = index
		s.resourcePut(r, false)
	}
}

func resourceIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Resource] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Resource](iter)
}
