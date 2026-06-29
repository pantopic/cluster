package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// ServiceFindByName returns the service with the specified name or ok false if not found.
func (fsm *State) ServiceFindByName(namespace, resource, cluster, name string) (s *pb.Service, ok bool) {
	res, err := fsm.txn.First(`service`, `id`, namespace, resource, cluster, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(*pb.Service)
		ok = true
	}
	return
}

// ServiceIterAll returns an iterator for every service ordered by service id ascending.
func (fsm *State) ServiceIterAll() iter.Seq[*pb.Service] {
	return serviceIter(fsm.txn.LowerBound(`service`, `id`, "", "", "", ""))
}

// ServiceIter returns an iterator enumerating every service in a resource.
func (fsm *State) ServiceIter(namespace, resource string) iter.Seq[*pb.Service] {
	return serviceIter(fsm.txn.Get(`service`, `Resource`, namespace, resource))
}

// ServiceIterByUri returns an iterator enumerating every service of a specific Uri.
func (fsm *State) ServiceIterByUri(uri string) iter.Seq[*pb.Service] {
	return serviceIter(fsm.txn.Get(`service`, `Uri`, uri))
}

// ServiceIterByTag returns an iterator enumerating every service matching the specified tag,
// ordered by service id ascending.
func (fsm *State) ServiceIterByTag(tag string) iter.Seq[*pb.Service] {
	return serviceIter(fsm.txn.Get(`service`, `Tags`, tag))
}

// ServiceIterateUpdatedAfter returns an iterator enumerating every service having an updated index
// greater than the supplied index
func (fsm *State) ServiceIterUpdatedAfter(index uint64) iter.Seq[*pb.Service] {
	iter, err := fsm.txn.LowerBound(`service`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Service](iter)
}

func (fsm *State) servicePut(s *pb.Service, create bool) {
	err := fsm.txn.Insert(`service`, s)
	if err != nil {
		panic(err)
	}
	if create {
		fsm.metaIncrVal(`services`)
	}
}

func (fsm *State) serviceDelete(s *pb.Service) {
	err := fsm.txn.Delete(`service`, s)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func serviceIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Service] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Service](iter)
}
