package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// ComponentFindByName returns the component with the specified name or ok false if not found.
func (fsm *State) ComponentFindByName(namespace, resource, cluster, name string) (s *pb.Component, ok bool) {
	res, err := fsm.txn.First(`component`, `id`, namespace, resource, cluster, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(*pb.Component)
		ok = true
	}
	return
}

// ComponentIterAll returns an iterator for every component ordered by component id ascending.
func (fsm *State) ComponentIterAll() iter.Seq[*pb.Component] {
	return componentIter(fsm.txn.LowerBound(`component`, `id`, "", "", "", ""))
}

// ServiceIter returns an iterator enumerating every service in a resource.
func (fsm *State) ComponentIter(namespace, resource string) iter.Seq[*pb.Component] {
	return componentIter(fsm.txn.Get(`component`, `Resource`, namespace, resource))
}

// ComponentIterByUri returns an iterator enumerating every component of a specific Uri.
func (fsm *State) ComponentIterByUri(uri string) iter.Seq[*pb.Component] {
	return componentIter(fsm.txn.Get(`component`, `Uri`, uri))
}

// ComponentIterByTag returns an iterator enumerating every component matching the specified tag,
// ordered by component id ascending.
func (fsm *State) ComponentIterByTag(tag string) iter.Seq[*pb.Component] {
	return componentIter(fsm.txn.Get(`component`, `Tags`, tag))
}

// ComponentIterateUpdatedAfter returns an iterator enumerating every component having an updated index
// greater than the supplied index
func (fsm *State) ComponentIterUpdatedAfter(index uint64) iter.Seq[*pb.Component] {
	iter, err := fsm.txn.LowerBound(`component`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Component](iter)
}

func (fsm *State) componentPut(s *pb.Component, create bool) {
	err := fsm.txn.Insert(`component`, s)
	if err != nil {
		panic(err)
	}
	if create {
		fsm.metaIncrVal(`components`)
	}
}

func (fsm *State) componentDelete(s *pb.Component) {
	err := fsm.txn.Delete(`component`, s)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func componentIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Component] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Component](iter)
}
