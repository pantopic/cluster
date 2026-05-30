package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"
)

// Service returns the service with the specified id or ok false if not found.
func (fsm *State) Service(id uint64) (s Service, ok bool) {
	res, err := fsm.txn.First(`service`, `id`, id)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Service)
		ok = true
	}
	return
}

// ServiceFindByName returns the service with the specified name or ok false if not found.
func (fsm *State) ServiceFindByName(name string) (s Service, ok bool) {
	res, err := fsm.txn.First(`service`, `Name`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Service)
		ok = true
	}
	return
}

// ServiceIterAll returns an iterator for every service ordered by service id ascending.
func (fsm *State) ServiceIterAll() iter.Seq[Service] {
	return serviceIter(fsm.txn.LowerBound(`service`, `id`, uint64(0)))
}

// ServiceIter executes a callback for service in a resource.
func (fsm *State) ServiceIter(resourceID uint64) iter.Seq[Service] {
	return serviceIter(fsm.txn.Get(`service`, `ResourceID`, resourceID))
}

// ServiceIterByUri returns an iterator enumerating every service of a specific Uri.
func (fsm *State) ServiceIterByUri(uri string) iter.Seq[Service] {
	return serviceIter(fsm.txn.Get(`service`, `Uri`, uri))
}

// ServiceIterByTag returns an iterator enumerating every service matching the specified tag,
// ordered by service id ascending.
func (fsm *State) ServiceIterByTag(tag string) iter.Seq[Service] {
	return serviceIter(fsm.txn.Get(`service`, `Tags`, tag))
}

// ServiceIterateUpdatedAfter returns an iterator enumerating every service having an updated index
// greater than the supplied index
func (fsm *State) ServiceIterUpdatedAfter(index uint64) iter.Seq[Service] {
	iter, err := fsm.txn.LowerBound(`service`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Service](iter)
}

func (fsm *State) servicePut(s Service) {
	err := fsm.txn.Insert(`service`, s)
	if err != nil {
		panic(err)
	}
}

func (fsm *State) serviceDelete(s Service) {
	err := fsm.txn.Delete(`service`, s)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func (fsm *State) serviceTouch(id, index uint64) {
	if s, ok := fsm.Service(id); ok {
		s.Updated = index
		fsm.servicePut(s)
	}
}

func serviceIter(iter memdb.ResultIterator, err error) iter.Seq[Service] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Service](iter)
}
