package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// BankFindByName returns the bank with the specified name or ok false if not found.
func (s *State) BankFindByName(namespace, resource, cluster, name string) (c *pb.Bank, ok bool) {
	res, err := s.txn.First(`bank`, `id`, namespace, resource, cluster, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		c = res.(*pb.Bank)
		ok = true
	}
	return
}

// BankIterAll returns an iterator for every bank ordered by bank id ascending.
func (s *State) BankIterAll() iter.Seq[*pb.Bank] {
	return bankIter(s.txn.LowerBound(`bank`, `id`, "", "", "", ""))
}

// ServiceIter returns an iterator enumerating every service in a resource.
func (s *State) BankIter(namespace, resource string) iter.Seq[*pb.Bank] {
	return bankIter(s.txn.Get(`bank`, `Resource`, namespace, resource))
}

// BankIterByUri returns an iterator enumerating every bank of a specific Uri.
func (s *State) BankIterByUri(uri string) iter.Seq[*pb.Bank] {
	return bankIter(s.txn.Get(`bank`, `Uri`, uri))
}

// BankIterByTag returns an iterator enumerating every bank matching the specified tag,
// ordered by bank id ascending.
func (s *State) BankIterByTag(tag string) iter.Seq[*pb.Bank] {
	return bankIter(s.txn.Get(`bank`, `Tags`, tag))
}

// BankIterateUpdatedAfter returns an iterator enumerating every bank having an updated index
// greater than the supplied index
func (s *State) BankIterUpdatedAfter(index uint64) iter.Seq[*pb.Bank] {
	iter, err := s.txn.LowerBound(`bank`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Bank](iter)
}

func (s *State) bankPut(c *pb.Bank, create bool) {
	err := s.txn.Insert(`bank`, c)
	if err != nil {
		panic(err)
	}
	if create {
		s.metaIncrVal(`banks`)
	}
}

func (s *State) bankDelete(item *pb.Bank) uint64 {
	err := s.txn.Delete(`bank`, item)
	if err == nil {
		return ResultSuccess
	}
	if err == memdb.ErrNotFound {
		return ResultErrNotFound
	}
	panic(err)
}

func bankIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Bank] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Bank](iter)
}
