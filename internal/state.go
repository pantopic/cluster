package internal

import (
	"encoding/json"
	"fmt"
	"io"
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
	namspaceSchema := &memdb.TableSchema{
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
			"Type": {
				Name:    "Type",
				Indexer: &memdb.StringFieldIndex{Field: "Type"},
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
			"Type": {
				Name:    "Type",
				Indexer: &memdb.StringFieldIndex{Field: "Type"},
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
			"namespace": namspaceSchema,
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

// Namespace returns the namespace with the specified id or ok false if not found.
func (fsm *State) Namespace(id string) (s Namespace, ok bool) {
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

func (fsm *State) namespaceTouch(id string, index uint64) {
	if s, ok := fsm.Namespace(id); ok {
		s.Updated = index
		fsm.namespacePut(s)
	}
}

// NamespaceIterate executes a callback for every namespace in the namespace ordered by namespace id ascending.
// Return true to continue iterating, false to stop.
func (fsm *State) NamespaceIterate(fn func(s Namespace) bool) {
	iter, err := fsm.txn.LowerBound(`namespace`, `id`, uint64(0))
	if err != nil {
		panic(err)
	}
	for {
		res := iter.Next()
		if res == nil || !fn(res.(Namespace)) {
			break
		}
	}
}

// NamespaceIterateByTag executes a callback for every namespace in the namespace matching the specified tag,
// ordered by namespace id ascending. Return true to continue iterating, false to stop.
func (fsm *State) NamespaceIterateByTag(tag string, fn func(r Namespace) bool) {
	iter, err := fsm.txn.Get(`namespace`, `Tags`, tag)
	if err != nil {
		panic(err)
	}
	for {
		res := iter.Next()
		if res == nil || !fn(res.(Namespace)) {
			break
		}
	}
}

// NamespaceIterateUpdatedAfter executes a callback for every namespace in the namespace having an updated index
// greater than the supplied index
func (fsm *State) NamespaceIterateUpdatedAfter(index uint64, fn func(r Namespace) bool) {
	iter, err := fsm.txn.LowerBound(`namespace`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	for {
		res := iter.Next()
		if res == nil || !fn(res.(Namespace)) {
			break
		}
	}
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

// ComponentIterate executes a callback for every component in the component ordered by component id ascending.
// Return true to continue iterating, false to stop.
func (fsm *State) ComponentIterate(fn func(s Component) bool) {
	iter, err := fsm.txn.LowerBound(`component`, `id`, uint64(0))
	if err != nil {
		panic(err)
	}
	fsm.componentIter(iter, fn)
}

// ComponentIterateByResourceID executes a callback for every child of a component.
func (fsm *State) ComponentIterateByResourceID(resourceID uint64, fn func(r Component) bool) {
	iter, err := fsm.txn.Get(`component`, `ResourceID`, resourceID)
	if err != nil {
		panic(err)
	}
	fsm.componentIter(iter, fn)
}

// ComponentIterateByNamespaceID executes a callback for every component in the component in the namespace.
func (fsm *State) ComponentIterateByNamespaceID(namespaceID string, fn func(r Component) bool) {
	iter, err := fsm.txn.Get(`component`, `NamespaceID`, namespaceID)
	if err != nil {
		panic(err)
	}
	fsm.componentIter(iter, fn)
}

// ComponentIterateByType executes a callback for every component of a specific type.
func (fsm *State) ComponentIterateByType(uri string, fn func(r Component) bool) {
	iter, err := fsm.txn.Get(`component`, `Type`, uri)
	if err != nil {
		panic(err)
	}
	fsm.componentIter(iter, fn)
}

// ComponentIterateByTag executes a callback for every component in the component matching the specified tag,
// ordered by component id ascending. Return true to continue iterating, false to stop.
func (fsm *State) ComponentIterateByTag(tag string, fn func(r Component) bool) {
	iter, err := fsm.txn.Get(`component`, `Tags`, tag)
	if err != nil {
		panic(err)
	}
	fsm.componentIter(iter, fn)
}

// ComponentIterateUpdatedAfter executes a callback for every component in the component having an updated index
// greater than the supplied index
func (fsm *State) ComponentIterateUpdatedAfter(index uint64, fn func(c Component) bool) {
	iter, err := fsm.txn.LowerBound(`component`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	fsm.componentIter(iter, fn)
}

func (fsm *State) componentIter(iter memdb.ResultIterator, fn func(c Component) bool) {
	for {
		item := iter.Next()
		if item == nil || !fn(item.(Component)) {
			break
		}
	}
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
func (fsm *State) ResourceFindByName(name string) (s Resource, ok bool) {
	res, err := fsm.txn.First(`resource`, `Name`, name)
	if err != nil {
		panic(err)
	}
	if res != nil {
		s = res.(Resource)
		ok = true
	}
	return
}

func (fsm *State) resourcePut(s Resource) {
	err := fsm.txn.Insert(`resource`, s)
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

// ResourceIterate executes a callback for every resource in the resource ordered by resource id ascending.
// Return true to continue iterating, false to stop.
func (fsm *State) ResourceIterate(fn func(s Resource) bool) {
	iter, err := fsm.txn.LowerBound(`resource`, `id`, uint64(0))
	if err != nil {
		panic(err)
	}
	fsm.resourceIter(iter, fn)
}

// ResourceIterateByParentID executes a callback for every child of a resource.
func (fsm *State) ResourceIterateByParentID(parentID uint64, fn func(r Resource) bool) {
	iter, err := fsm.txn.Get(`resource`, `ParentID`, parentID)
	if err != nil {
		panic(err)
	}
	fsm.resourceIter(iter, fn)
}

// ResourceIterateByType executes a callback for every resource of a specific type.
func (fsm *State) ResourceIterateByType(uri string, fn func(r Resource) bool) {
	iter, err := fsm.txn.Get(`resource`, `Type`, uri)
	if err != nil {
		panic(err)
	}
	fsm.resourceIter(iter, fn)
}

// ResourceIterateByTag executes a callback for every resource in the resource matching the specified tag,
// ordered by resource id ascending. Return true to continue iterating, false to stop.
func (fsm *State) ResourceIterateByTag(tag string, fn func(r Resource) bool) {
	iter, err := fsm.txn.Get(`resource`, `Tags`, tag)
	if err != nil {
		panic(err)
	}
	fsm.resourceIter(iter, fn)
}

// ResourceIterateUpdatedAfter executes a callback for every resource in the resource having an updated index
// greater than the supplied index
func (fsm *State) ResourceIterateUpdatedAfter(index uint64, fn func(r Resource) bool) {
	iter, err := fsm.txn.LowerBound(`resource`, `Updated`, index+1)
	if err != nil {
		panic(err)
	}
	fsm.resourceIter(iter, fn)
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
		Index:      f.metaGetVal(`index`),
		ResourceID: f.metaGetVal(`resourceID`),
	}
	f.ResourceIterate(func(Resource) bool {
		header.Resources++
		return true
	})
	b, _ := json.Marshal(header)
	w.Write(append(b, '\n'))
	f.ResourceIterate(func(item Resource) bool {
		b, _ := json.Marshal(item)
		w.Write(append(b, '\n'))
		return true
	})
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
