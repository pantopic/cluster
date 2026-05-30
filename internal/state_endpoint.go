package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"
)

// Endpoint returns the endpoint with the specified id or ok false if not found.
func (fsm *State) Endpoint(id uint64) (s Endpoint, ok bool) {
	res, err := fsm.txn.First(`endpoint`, `id`, id)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Endpoint)
		ok = true
	}
	return
}

// EndpointFindByName returns the endpoint with the specified name or ok false if not found.
func (fsm *State) EndpointFindByName(name string) (s Endpoint, ok bool) {
	res, err := fsm.txn.First(`endpoint`, `Name`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Endpoint)
		ok = true
	}
	return
}

// EndpointIterAll returns an iterator for every endpoint ordered by endpoint id ascending.
func (fsm *State) EndpointIterAll() iter.Seq[Endpoint] {
	return endpointIter(fsm.txn.LowerBound(`endpoint`, `id`, uint64(0)))
}

// EndpointIter executes a callback for endpoint in a resource.
func (fsm *State) EndpointIter(serviceID uint64) iter.Seq[Endpoint] {
	return endpointIter(fsm.txn.Get(`endpoint`, `ServiceID`, serviceID))
}

// EndpointIterByTag returns an iterator enumerating every endpoint matching the specified tag,
// ordered by endpoint id ascending.
func (fsm *State) EndpointIterByTag(tag string) iter.Seq[Endpoint] {
	return endpointIter(fsm.txn.Get(`endpoint`, `Tags`, tag))
}

// EndpointIterateUpdatedAfter returns an iterator enumerating every endpoint having an updated index
// greater than the supplied index
func (fsm *State) EndpointIterUpdatedAfter(index uint64) iter.Seq[Endpoint] {
	iter, err := fsm.txn.LowerBound(`endpoint`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Endpoint](iter)
}

func (fsm *State) endpointPut(s Endpoint) {
	err := fsm.txn.Insert(`endpoint`, s)
	if err != nil {
		panic(err)
	}
}

func (fsm *State) endpointDelete(s Endpoint) {
	err := fsm.txn.Delete(`endpoint`, s)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func (fsm *State) endpointTouch(id, index uint64) {
	if s, ok := fsm.Endpoint(id); ok {
		s.Updated = index
		fsm.endpointPut(s)
	}
}

func endpointIter(iter memdb.ResultIterator, err error) iter.Seq[Endpoint] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Endpoint](iter)
}
