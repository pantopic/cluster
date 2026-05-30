package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"
)

// Component returns the component with the specified id or ok false if not found.
func (fsm *State) Component(id uint64) (s Component, ok bool) {
	res, err := fsm.txn.First(`component`, `id`, id)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Component)
		ok = true
	}
	return
}

// ComponentFindByName returns the component with the specified name or ok false if not found.
func (fsm *State) ComponentFindByName(name string) (s Component, ok bool) {
	res, err := fsm.txn.First(`component`, `Name`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Component)
		ok = true
	}
	return
}

// ComponentIterAll returns an iterator for every component ordered by component id ascending.
func (fsm *State) ComponentIterAll() iter.Seq[Component] {
	return componentIter(fsm.txn.LowerBound(`component`, `id`, uint64(0)))
}

// ComponentIter executes a callback for component in a resource.
func (fsm *State) ComponentIter(resourceID uint64) iter.Seq[Component] {
	return componentIter(fsm.txn.Get(`component`, `ResourceID`, resourceID))
}

// ComponentIterByUri returns an iterator enumerating every component of a specific Uri.
func (fsm *State) ComponentIterByUri(uri string) iter.Seq[Component] {
	return componentIter(fsm.txn.Get(`component`, `Uri`, uri))
}

// ComponentIterByTag returns an iterator enumerating every component matching the specified tag,
// ordered by component id ascending.
func (fsm *State) ComponentIterByTag(tag string) iter.Seq[Component] {
	return componentIter(fsm.txn.Get(`component`, `Tags`, tag))
}

// ComponentIterateUpdatedAfter returns an iterator enumerating every component having an updated index
// greater than the supplied index
func (fsm *State) ComponentIterUpdatedAfter(index uint64) iter.Seq[Component] {
	iter, err := fsm.txn.LowerBound(`component`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Component](iter)
}

func (fsm *State) componentPut(s Component) {
	err := fsm.txn.Insert(`component`, s)
	if err != nil {
		panic(err)
	}
}

func (fsm *State) componentDelete(s Component) {
	err := fsm.txn.Delete(`component`, s)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func (fsm *State) componentTouch(id, index uint64) {
	if s, ok := fsm.Component(id); ok {
		s.Updated = index
		fsm.componentPut(s)
	}
}

func componentIter(iter memdb.ResultIterator, err error) iter.Seq[Component] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Component](iter)
}
