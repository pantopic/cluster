package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
)

// NamespaceFindByName returns the namespace with the specified name or ok false if not found.
func (fsm *State) NamespaceFindByName(name string) (s *pb.Namespace, ok bool) {
	res, err := fsm.txn.First(`namespace`, `id`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(*pb.Namespace)
		ok = true
	}
	return
}

// NamespaceIter returns an iterator enumerating every namespace ordered by id ascending.
func (fsm *State) NamespaceIter() iter.Seq[*pb.Namespace] {
	return namespaceIter(fsm.txn.LowerBound(`namespace`, `id`, ""))
}

// NamespaceIterByTag returns an iterator enumerating every namespace in the namespace matching the specified tag,
// ordered by namespace id ascending.
func (fsm *State) NamespaceIterByTag(tag string) iter.Seq[*pb.Namespace] {
	return namespaceIter(fsm.txn.Get(`namespace`, `Tags`, tag))
}

// NamespaceIterUpdatedAfter returns an iterator enumerating every namespace in the namespace having an updated index
// greater than the supplied index
func (fsm *State) NamespaceIterUpdatedAfter(index uint64) iter.Seq[*pb.Namespace] {
	return namespaceIter(fsm.txn.LowerBound(`namespace`, `Updated`, index+1))
}

func (fsm *State) namespacePut(s *pb.Namespace, create bool) {
	err := fsm.txn.Insert(`namespace`, s)
	if err != nil {
		panic(err)
	}
	if create {
		fsm.metaIncrVal(`namespaces`)
	}
}

func (fsm *State) namespaceDelete(s *pb.Namespace) {
	err := fsm.txn.Delete(`namespace`, s)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func namespaceIter(iter memdb.ResultIterator, err error) iter.Seq[*pb.Namespace] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[*pb.Namespace](iter)
}
