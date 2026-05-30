package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"
)

// Resource returns the resource with the specified id or ok false if not found.
func (fsm *State) Resource(id uint64) (s Resource, ok bool) {
	res, err := fsm.txn.First(`resource`, `id`, id)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Resource)
		ok = true
	}
	return
}

// ResourceFindByName returns the resource with the specified name or ok false if not found.
func (fsm *State) ResourceFindByName(namespaceID uint64, name string) (s Resource, ok bool) {
	res, err := fsm.txn.First(`resource`, `Name`, namespaceID, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Resource)
		ok = true
	}
	return
}

// ResourceIter executes a callback for every resource in the resource ordered by resource id ascending.
// Return true to continue iterating, false to stop.
func (fsm *State) ResourceIter() iter.Seq[Resource] {
	return resourceIter(fsm.txn.LowerBound(`resource`, `id`, uint64(0)))
}

// ResourceIterByNamespaceID executes a callback for every child of a resource.
func (fsm *State) ResourceIterByNamespaceID(namespaceID uint64) iter.Seq[Resource] {
	return resourceIter(fsm.txn.Get(`resource`, `NamespaceID`, namespaceID))
}

// ResourceIterByUri executes a callback for every resource of a specific type.
func (fsm *State) ResourceIterByUri(uri string) iter.Seq[Resource] {
	return resourceIter(fsm.txn.Get(`resource`, `Uri`, uri))
}

// ResourceIterByTag executes a callback for every resource in the resource matching the specified tag,
// ordered by resource id ascending. Return true to continue iterating, false to stop.
func (fsm *State) ResourceIterByTag(tag string) iter.Seq[Resource] {
	return resourceIter(fsm.txn.Get(`resource`, `Tags`, tag))
}

// ResourceIterUpdatedAfter executes a callback for every resource in the resource having an updated index
// greater than the supplied index
func (fsm *State) ResourceIterUpdatedAfter(index uint64) iter.Seq[Resource] {
	return resourceIter(fsm.txn.LowerBound(`resource`, `Updated`, index+1))
}

func (fsm *State) resourcePut(r Resource) {
	err := fsm.txn.Insert(`resource`, r)
	if err != nil {
		panic(err)
	}
}

func (fsm *State) resourceDelete(r Resource) {
	err := fsm.txn.Delete(`resource`, r)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func (fsm *State) resourceTouch(id, index uint64) {
	if s, ok := fsm.Resource(id); ok {
		s.Updated = index
		fsm.resourcePut(s)
	}
}

func resourceIter(iter memdb.ResultIterator, err error) iter.Seq[Resource] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Resource](iter)
}
