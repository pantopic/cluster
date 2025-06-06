package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"sync"

	"github.com/hashicorp/go-memdb"
)

// State represents internal state. All public methods are read only.
type State struct {
	db  *memdb.MemDB
	txn *memdb.Txn
}

type metaValue struct {
	Key  string
	Val  uint64
	Data []byte
}

type fsmStateMetaHeader struct {
	Index       uint64 `json:"index"`
	NamespaceID uint64 `json:"namespaceID"`
	Namespaces  uint64 `json:"namespaces"`
	ResourceID  uint64 `json:"resourceID"`
	Resources   uint64 `json:"resources"`
	ComponentID uint64 `json:"componentID"`
	Components  uint64 `json:"components"`
	InterfaceID uint64 `json:"interfaceID"`
	Interfaces  uint64 `json:"interfaces"`
}

var statePool = sync.Pool{New: func() any {
	return &State{}
}}

func newState() *State {
	metaSchema := &memdb.TableSchema{
		Name: "meta",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.StringFieldIndex{Field: "Key"},
			},
		},
	}
	namespaceSchema := &memdb.TableSchema{
		Name: "namespace",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.UintFieldIndex{Field: "ID"},
			},
			"Name": {
				Name:    "Name",
				Unique:  true,
				Indexer: &memdb.StringFieldIndex{Field: "Name"},
			},
			"Tags": {
				Name:         "Tags",
				AllowMissing: true,
				Indexer:      &memdb.StringMapFieldIndex{Field: "Tags"},
			},
			"Created": {
				Name:    "Created",
				Indexer: &memdb.UintFieldIndex{Field: "Created"},
			},
			"Updated": {
				Name:    "Updated",
				Indexer: &memdb.UintFieldIndex{Field: "Updated"},
			},
		},
	}
	resourceSchema := &memdb.TableSchema{
		Name: "resource",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.UintFieldIndex{Field: "ID"},
			},
			"NamespaceID": {
				Name:    "NamespaceID",
				Indexer: &memdb.UintFieldIndex{Field: "NamespaceID"},
			},
			"Name": {
				Name:   "Name",
				Unique: true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.UintFieldIndex{Field: "NamespaceID"},
						&memdb.StringFieldIndex{Field: "Name"},
					},
				},
			},
			"Tags": {
				Name:         "Tags",
				AllowMissing: true,
				Indexer:      &memdb.StringMapFieldIndex{Field: "Tags"},
			},
			"Uri": {
				Name:    "Uri",
				Indexer: &memdb.StringFieldIndex{Field: "Uri"},
			},
			"Created": {
				Name:    "Created",
				Indexer: &memdb.UintFieldIndex{Field: "Created"},
			},
			"Updated": {
				Name:    "Updated",
				Indexer: &memdb.UintFieldIndex{Field: "Updated"},
			},
		},
	}
	componentSchema := &memdb.TableSchema{
		Name: "component",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.UintFieldIndex{Field: "ID"},
			},
			"ResourceID": {
				Name:    "ResourceID",
				Indexer: &memdb.UintFieldIndex{Field: "ResourceID"},
			},
			"Name": {
				Name:   "Name",
				Unique: true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.UintFieldIndex{Field: "ResourceID"},
						&memdb.StringFieldIndex{Field: "Name"},
					},
				},
			},
			"Tags": {
				Name:         "Tags",
				AllowMissing: true,
				Indexer:      &memdb.StringMapFieldIndex{Field: "Tags"},
			},
			"Uri": {
				Name:    "Uri",
				Indexer: &memdb.StringFieldIndex{Field: "Uri"},
			},
			"Created": {
				Name:    "Created",
				Indexer: &memdb.UintFieldIndex{Field: "Created"},
			},
			"Updated": {
				Name:    "Updated",
				Indexer: &memdb.UintFieldIndex{Field: "Updated"},
			},
		},
	}
	db, err := memdb.NewMemDB(&memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"meta":      metaSchema,
			"namespace": namespaceSchema,
			"resource":  resourceSchema,
			"component": componentSchema,
		},
	})
	if err != nil {
		panic(err)
	}
	return &State{
		db: db,
	}
}

func (fsm *State) withTxn(write bool) *State {
	s := statePool.Get().(*State)
	s.db = fsm.db
	s.txn = fsm.db.Txn(write)
	return s
}

func (fsm *State) commit() {
	fsm.txn.Commit()
	statePool.Put(fsm)
}

func (fsm *State) abort() {
	fsm.txn.Abort()
	statePool.Put(fsm)
}

func (fsm *State) metaSet(key string, val uint64, data []byte) {
	err := fsm.txn.Insert(`meta`, metaValue{key, val, data})
	if err != nil {
		panic(err)
	}
}

func (fsm *State) metaSetIndex(val uint64) {
	err := fsm.txn.Insert(`meta`, metaValue{`index`, val, nil})
	if err != nil {
		panic(err)
	}
}

func (fsm *State) metaGet(key string) (val uint64, data []byte) {
	res, _ := fsm.txn.First(`meta`, `id`, key)
	if res != nil {
		val = res.(metaValue).Val
		data = res.(metaValue).Data
	}
	return
}

func (fsm *State) metaGetVal(key string) (val uint64) {
	val, _ = fsm.metaGet(key)
	return
}

func (fsm *State) metaGetDataString(key string) string {
	_, b := fsm.metaGet(key)
	return string(b)
}

func (fsm *State) Index() (val uint64) {
	return fsm.metaGetVal(`index`)
}

func (fsm *State) namespaceIncr() uint64 {
	id := fsm.metaGetVal(`namespaceID`)
	fsm.metaSet(`namespaceID`, id+1, nil)
	return id
}

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

// Namespace returns the namespace with the specified name or ok false if not found.
func (fsm *State) NamespaceByName(name string) (s Namespace, ok bool) {
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

func resultIterSeq[V any](iter memdb.ResultIterator) func(yield func(V) bool) {
	return func(yield func(V) bool) {
		for {
			if !yield(iter.Next().(V)) {
				break
			}
		}
	}
}

// NamespaceIter executes a callback for every namespace ordered by id ascending.
// Return true to continue iterating, false to stop.
func (fsm *State) NamespaceIter() iter.Seq[Namespace] {
	return namespaceIter(fsm.txn.LowerBound(`namespace`, `id`, uint64(0)))
}

// NamespaceIterByTag executes a callback for every namespace in the namespace matching the specified tag,
// ordered by namespace id ascending. Return true to continue iterating, false to stop.
func (fsm *State) NamespaceIterByTag(tag string) iter.Seq[Namespace] {
	return namespaceIter(fsm.txn.Get(`namespace`, `Tags`, tag))
}

// NamespaceIterUpdatedAfter executes a callback for every namespace in the namespace having an updated index
// greater than the supplied index
func (fsm *State) NamespaceIterUpdatedAfter(index uint64) iter.Seq[Namespace] {
	return namespaceIter(fsm.txn.LowerBound(`namespace`, `Updated`, index+1))
}

func namespaceIter(iter memdb.ResultIterator, err error) iter.Seq[Namespace] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Namespace](iter)
}

func (fsm *State) componentIncr() uint64 {
	id := fsm.metaGetVal(`componentID`)
	id++
	fsm.metaSet(`componentID`, id, nil)
	return id
}

// ComponentID returns the ID of the last created Component
func (fsm *State) ComponentID() uint64 {
	return fsm.metaGetVal(`componentID`)
}

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

// ComponentIter executes a callback for every component in the component ordered by component id ascending.
// Return true to continue iterating, false to stop.
func (fsm *State) ComponentIter() iter.Seq[Component] {
	return componentIter(fsm.txn.LowerBound(`component`, `id`, uint64(0)))
}

// ComponentIterByResourceID executes a callback for every child of a component.
func (fsm *State) ComponentIterByResourceID(resourceID uint64) iter.Seq[Component] {
	return componentIter(fsm.txn.Get(`component`, `ResourceID`, resourceID))
}

// ComponentIterByNamespaceID executes a callback for every component in the component in the namespace.
func (fsm *State) ComponentIterByNamespaceID(namespaceID string) iter.Seq[Component] {
	return componentIter(fsm.txn.Get(`component`, `NamespaceID`, namespaceID))
}

// ComponentIterByUri executes a callback for every component of a specific Uri.
func (fsm *State) ComponentIterByUri(uri string) iter.Seq[Component] {
	return componentIter(fsm.txn.Get(`component`, `Uri`, uri))
}

// ComponentIterByTag executes a callback for every component in the component matching the specified tag,
// ordered by component id ascending. Return true to continue iterating, false to stop.
func (fsm *State) ComponentIterByTag(tag string) iter.Seq[Component] {
	return componentIter(fsm.txn.Get(`component`, `Tags`, tag))
}

func componentIter(iter memdb.ResultIterator, err error) iter.Seq[Component] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Component](iter)
}

// ComponentIterateUpdatedAfter executes a callback for every component in the component having an updated index
// greater than the supplied index
func (fsm *State) ComponentIterUpdatedAfter(index uint64) iter.Seq[Component] {
	iter, err := fsm.txn.LowerBound(`component`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Component](iter)
}

func (fsm *State) resourceIncr() uint64 {
	id := fsm.metaGetVal(`resourceID`)
	id++
	fsm.metaSet(`resourceID`, id, nil)
	return id
}

// ResourceID returns the ID of the last created Resource
func (fsm *State) ResourceID() uint64 {
	return fsm.metaGetVal(`resourceID`)
}

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

func resourceIter(iter memdb.ResultIterator, err error) iter.Seq[Resource] {
	if err != nil {
		panic(err)
	}
	return resultIterSeq[Resource](iter)
}

func (fsm *State) resourceIter(iter memdb.ResultIterator, fn func(r Resource) bool) {
	for {
		res := iter.Next()
		if res == nil || !fn(res.(Resource)) {
			break
		}
	}
}

func (fsm *State) Save(w io.Writer) error {
	f := fsm.withTxn(false)
	header := fsmStateMetaHeader{
		Index:       f.metaGetVal(`index`),
		NamespaceID: f.metaGetVal(`namespaceID`),
		ResourceID:  f.metaGetVal(`resourceID`),
		ComponentID: f.metaGetVal(`componentID`),
	}
	for range f.ResourceIter() {
		header.Resources++
	}
	b, _ := json.Marshal(header)
	w.Write(append(b, '\n'))
	for item := range f.ResourceIter() {
		b, _ := json.Marshal(item)
		w.Write(append(b, '\n'))
	}
	return nil
}

func (fsm *State) recover(r io.Reader) (err error) {
	f := fsm.withTxn(true)
	defer f.commit()
	decoder := json.NewDecoder(r)
	var header fsmStateMetaHeader
	if err := decoder.Decode(&header); err != nil {
		return err
	}
	f.metaSet(`index`, header.Index, nil)
	f.metaSet(`resourceID`, header.ResourceID, nil)
	var i uint64
	for i = 0; i < header.Resources; i++ {
		var s Resource
		if err = decoder.Decode(&s); err != nil {
			return fmt.Errorf("parse resource: %w", err)
		}
		f.resourcePut(s)
	}
	return nil
}
