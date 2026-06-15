package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// ResourceFindByName returns the resource with the specified name or ok false if not found.
func (fsm *State) ResourceFindByName(namespace, name string) (s *pb.Resource, ok bool) {
	res, err := fsm.txn.First(`resource`, `id`, namespace, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(*pb.Resource)
		ok = true
	}
	return
}

// ResourceIterAll executes a callback for every resource in the resource ordered by resource id ascending.
// Return true to continue iterating, false to stop.
func (fsm *State) ResourceIterAll() iter.Seq[*pb.Resource] {
	return resourceIter(fsm.txn.LowerBound(`resource`, `id`, "", "", ""))
}

// ResourceIterByNamespace executes a callback for every child of a resource.
func (fsm *State) ResourceIterByNamespace(namespace string) iter.Seq[*pb.Resource] {
	return resourceIter(fsm.txn.Get(`resource`, `Namespace`, namespace))
}

// ResourceIterByUri executes a callback for every resource of a specific type.
func (fsm *State) ResourceIterByUri(uri string) iter.Seq[*pb.Resource] {
	return resourceIter(fsm.txn.Get(`resource`, `Uri`, uri))
}

// ResourceIterByTag executes a callback for every resource in the resource matching the specified tag,
// ordered by resource id ascending. Return true to continue iterating, false to stop.
func (fsm *State) ResourceIterByTag(tag string) iter.Seq[*pb.Resource] {
	return resourceIter(fsm.txn.Get(`resource`, `Tags`, tag))
}

// ResourceIterUpdatedAfter executes a callback for every resource in the resource having an updated index
// greater than the supplied index
func (fsm *State) ResourceIterUpdatedAfter(index uint64) iter.Seq[*pb.Resource] {
	return resourceIter(fsm.txn.LowerBound(`resource`, `Updated`, index+1))
}

func (fsm *State) resourcePut(r *pb.Resource, create bool) {
	err := fsm.txn.Insert(`resource`, r)
	if err != nil {
		panic(err)
	}
	if create {
		fsm.metaIncrVal(`resources`)
	}
}

func (fsm *State) resourceDelete(r *pb.Resource) {
	err := fsm.txn.Delete(`resource`, r)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func (fsm *State) resourceTouch(namespace, name string, index uint64) {
	if s, ok := fsm.ResourceFindByName(namespace, name); ok {
		s.Updated = index
		fsm.resourcePut(s, false)
	}
}

func resourceIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Resource] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Resource](iter)
}
