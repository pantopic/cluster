package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// ServiceFindByName returns the service with the specified name or ok false if not found.
func (s *State) ServiceFindByName(namespace, resource, name string) (svc *pb.Service, ok bool) {
	res, err := s.txn.First(`service`, `id`, namespace, resource, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		svc = res.(*pb.Service)
		ok = true
	}
	return
}

// ServiceIterAll returns an iterator for every service ordered by service id ascending.
func (s *State) ServiceIterAll() iter.Seq[*pb.Service] {
	return serviceIter(s.txn.LowerBound(`service`, `id`, "", "", ""))
}

// ServiceIter returns an iterator enumerating every service in a resource.
func (s *State) ServiceIter(namespace, resource string) iter.Seq[*pb.Service] {
	return serviceIter(s.txn.Get(`service`, `Resource`, namespace, resource))
}

// ServiceIterByUri returns an iterator enumerating every service of a specific Uri.
func (s *State) ServiceIterByUri(uri string) iter.Seq[*pb.Service] {
	return serviceIter(s.txn.Get(`service`, `Uri`, uri))
}

// ServiceIterByTag returns an iterator enumerating every service matching the specified tag,
// ordered by service id ascending.
func (s *State) ServiceIterByTag(tag string) iter.Seq[*pb.Service] {
	return serviceIter(s.txn.Get(`service`, `Tags`, tag))
}

// ServiceIterateUpdatedAfter returns an iterator enumerating every service having an updated index
// greater than the supplied index
func (s *State) ServiceIterUpdatedAfter(index uint64) iter.Seq[*pb.Service] {
	iter, err := s.txn.LowerBound(`service`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Service](iter)
}

func (s *State) servicePut(svc *pb.Service, create bool) {
	err := s.txn.Insert(`service`, svc)
	if err != nil {
		panic(err)
	}
	if create {
		s.metaIncrVal(`services`)
	}
}

func (s *State) serviceDelete(item *pb.Service) uint64 {
	return s.itemDelete(`service`, item)
}

func serviceIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Service] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Service](iter)
}
