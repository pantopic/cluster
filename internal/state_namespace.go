package internal

import (
	"iter"

	"github.com/hashicorp/go-memdb"
)

// Namespace returns the namespace with the specified id or ok false if not found.
func (fsm *State) Namespace(id uint64) (s Namespace, ok bool) {
	res, err := fsm.txn.First(`namespace`, `id`, id)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Namespace)
		ok = true
	}
	return
}

// NamespaceFindByName returns the namespace with the specified name or ok false if not found.
func (fsm *State) NamespaceFindByName(name string) (s Namespace, ok bool) {
	res, err := fsm.txn.First(`namespace`, `Name`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Namespace)
		ok = true
	}
	return
}

// NamespaceIter returns an iterator enumerating every namespace ordered by id ascending.
func (fsm *State) NamespaceIter() iter.Seq[Namespace] {
	return namespaceIter(fsm.txn.LowerBound(`namespace`, `id`, uint64(0)))
}

// NamespaceIterByTag returns an iterator enumerating every namespace in the namespace matching the specified tag,
// ordered by namespace id ascending.
func (fsm *State) NamespaceIterByTag(tag string) iter.Seq[Namespace] {
	return namespaceIter(fsm.txn.Get(`namespace`, `Tags`, tag))
}

// NamespaceIterUpdatedAfter returns an iterator enumerating every namespace in the namespace having an updated index
// greater than the supplied index
func (fsm *State) NamespaceIterUpdatedAfter(index uint64) iter.Seq[Namespace] {
	return namespaceIter(fsm.txn.LowerBound(`namespace`, `Updated`, index+1))
}

func (fsm *State) namespacePut(s Namespace) {
	err := fsm.txn.Insert(`namespace`, s)
	if err != nil {
		panic(err)
	}
}

func (fsm *State) namespaceDelete(s Namespace) {
	err := fsm.txn.Delete(`namespace`, s)
	if err != nil && err != memdb.ErrNotFound {
		panic(err)
	}
}

func (fsm *State) namespaceTouch(id, index uint64) {
	if s, ok := fsm.Namespace(id); ok {
		s.Updated = index
		fsm.namespacePut(s)
	}
}

func namespaceIter(iter memdb.ResultIterator, err error) iter.Seq[Namespace] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Namespace](iter)
}
