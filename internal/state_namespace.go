package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// NamespaceFindByName returns the namespace with the specified name or ok false if not found.
func (s *State) NamespaceFindByName(name string) (ns *pb.Namespace, ok bool) {
	res, err := s.txn.First(`namespace`, `id`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		ns = res.(*pb.Namespace)
		ok = true
	}
	return
}

// NamespaceIter returns an iterator enumerating every namespace ordered by id ascending.
func (s *State) NamespaceIter() iter.Seq[*pb.Namespace] {
	return namespaceIter(s.txn.LowerBound(`namespace`, `id`, ""))
}

// NamespaceIterByTag returns an iterator enumerating every namespace in the namespace matching the specified tag,
// ordered by namespace id ascending.
func (s *State) NamespaceIterByTag(tag string) iter.Seq[*pb.Namespace] {
	return namespaceIter(s.txn.Get(`namespace`, `Tags`, tag))
}

// NamespaceIterUpdatedAfter returns an iterator enumerating every namespace in the namespace having an updated index
// greater than the supplied index
func (s *State) NamespaceIterUpdatedAfter(index uint64) iter.Seq[*pb.Namespace] {
	return namespaceIter(s.txn.LowerBound(`namespace`, `Updated`, index+1))
}

func (s *State) namespacePut(ns *pb.Namespace, create bool) {
	err := s.txn.Insert(`namespace`, ns)
	if err != nil {
		panic(err)
	}
	if create {
		s.metaIncrVal(`namespaces`)
	}
}

func (s *State) namespaceDelete(item *pb.Namespace) uint64 {
	return s.itemDelete(`namespace`, item)
}

func namespaceIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Namespace] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Namespace](iter)
}
