package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// ComponentFindByName returns the component with the specified name or ok false if not found.
func (s *State) ComponentFindByName(namespace, resource, cluster, name string) (c *pb.Component, ok bool) {
	res, err := s.txn.First(`component`, `id`, namespace, resource, cluster, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		c = res.(*pb.Component)
		ok = true
	}
	return
}

// ComponentIterAll returns an iterator for every component ordered by component id ascending.
func (s *State) ComponentIterAll() iter.Seq[*pb.Component] {
	return componentIter(s.txn.LowerBound(`component`, `id`, "", "", "", ""))
}

// ServiceIter returns an iterator enumerating every service in a resource.
func (s *State) ComponentIter(namespace, resource string) iter.Seq[*pb.Component] {
	return componentIter(s.txn.Get(`component`, `Resource`, namespace, resource))
}

// ComponentIterByUri returns an iterator enumerating every component of a specific Uri.
func (s *State) ComponentIterByUri(uri string) iter.Seq[*pb.Component] {
	return componentIter(s.txn.Get(`component`, `Uri`, uri))
}

// ComponentIterByTag returns an iterator enumerating every component matching the specified tag,
// ordered by component id ascending.
func (s *State) ComponentIterByTag(tag string) iter.Seq[*pb.Component] {
	return componentIter(s.txn.Get(`component`, `Tags`, tag))
}

// ComponentIterateUpdatedAfter returns an iterator enumerating every component having an updated index
// greater than the supplied index
func (s *State) ComponentIterUpdatedAfter(index uint64) iter.Seq[*pb.Component] {
	iter, err := s.txn.LowerBound(`component`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Component](iter)
}

func (s *State) componentPut(c *pb.Component, create bool) {
	err := s.txn.Insert(`component`, c)
	if err != nil {
		panic(err)
	}
	if create {
		s.metaIncrVal(`components`)
	}
}

func (s *State) componentDelete(item *pb.Component) uint64 {
	err := s.txn.Delete(`component`, item)
	if err == nil {
		return ResultSuccess
	}
	if err == memdb.ErrNotFound {
		return ResultErrNotFound
	}
	panic(err)
}

func componentIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Component] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Component](iter)
}
