package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/go-memdb"

	"github.com/pantopic/cluster/pb"
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
	Clusters   uint64 `json:"clusters"`
	Components uint64 `json:"components"`
	Endpoints  uint64 `json:"endpoints"`
	Index      uint64 `json:"index"`
	Namespaces uint64 `json:"namespaces"`
	Resources  uint64 `json:"resources"`
	Services   uint64 `json:"services"`
}

var statePool = sync.Pool{New: func() any {
	return &State{}
}}

func newState() *State {
	schema := &memdb.DBSchema{
		Tables: make(map[string]*memdb.TableSchema),
	}
	schema.Tables[`meta`] = &memdb.TableSchema{
		Name: "meta",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.StringFieldIndex{Field: "Key"},
			},
		},
	}
	schema.Tables[`cluster`] = &memdb.TableSchema{
		Name: "cluster",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
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
	schema.Tables[`namespace`] = &memdb.TableSchema{
		Name: "namespace",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
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
	schema.Tables[`resource`] = &memdb.TableSchema{
		Name: "resource",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:   "id",
				Unique: true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{Field: "Namespace"},
						&memdb.StringFieldIndex{Field: "Name"},
					},
				},
			},
			"Namespace": {
				Name:    "Namespace",
				Indexer: &memdb.StringFieldIndex{Field: "Namespace"},
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
	schema.Tables[`component`] = &memdb.TableSchema{
		Name: "component",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:   "id",
				Unique: true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{Field: "Namespace"},
						&memdb.StringFieldIndex{Field: "Resource"},
						&memdb.StringFieldIndex{Field: "Name"},
					},
				},
			},
			"Namespace": {
				Name:    "Namespace",
				Indexer: &memdb.StringFieldIndex{Field: "Namespace"},
			},
			"Resource": {
				Name: "Resource",
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{Field: "Namespace"},
						&memdb.StringFieldIndex{Field: "Resource"},
					},
				},
			},
			"Cluster": {
				Name:    "Cluster",
				Indexer: &memdb.StringFieldIndex{Field: "Cluster"},
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
	schema.Tables[`service`] = &memdb.TableSchema{
		Name: "service",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:   "id",
				Unique: true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{Field: "Namespace"},
						&memdb.StringFieldIndex{Field: "Resource"},
						&memdb.StringFieldIndex{Field: "Name"},
					},
				},
			},
			"Namespace": {
				Name:    "Namespace",
				Indexer: &memdb.StringFieldIndex{Field: "Namespace"},
			},
			"Resource": {
				Name: "Resource",
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{Field: "Namespace"},
						&memdb.StringFieldIndex{Field: "Resource"},
					},
				},
			},
			"Cluster": {
				Name:    "Cluster",
				Indexer: &memdb.StringFieldIndex{Field: "Cluster"},
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
	db, err := memdb.NewMemDB(schema)
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

func resultIterSeq[V any](iter memdb.ResultIterator) func(yield func(V) bool) {
	return func(yield func(V) bool) {
		for {
			if !yield(iter.Next().(V)) {
				break
			}
		}
	}
}

func (fsm *State) Save(w io.Writer) error {
	f := fsm.withTxn(false)
	header := fsmStateMetaHeader{
		Clusters:   f.metaGetVal(`clusters`),
		Components: f.metaGetVal(`components`),
		Index:      f.metaGetVal(`index`),
		Namespaces: f.metaGetVal(`namespaces`),
		Resources:  f.metaGetVal(`resources`),
		Services:   f.metaGetVal(`services`),
	}
	b, _ := json.Marshal(header)
	w.Write(append(b, '\n'))
	for item := range f.ClusterIter() {
		b, _ := json.Marshal(item)
		w.Write(append(b, '\n'))
	}
	for item := range f.NamespaceIter() {
		b, _ := json.Marshal(item)
		w.Write(append(b, '\n'))
	}
	for item := range f.ResourceIterAll() {
		b, _ := json.Marshal(item)
		w.Write(append(b, '\n'))
	}
	for item := range f.ComponentIterAll() {
		b, _ := json.Marshal(item)
		w.Write(append(b, '\n'))
	}
	for item := range f.ServiceIterAll() {
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
	f.metaSet(`clusters`, header.Clusters, nil)
	f.metaSet(`components`, header.Components, nil)
	f.metaSet(`index`, header.Index, nil)
	f.metaSet(`namespaces`, header.Namespaces, nil)
	f.metaSet(`resources`, header.Resources, nil)
	f.metaSet(`services`, header.Services, nil)
	var i uint64
	for i = 0; i < header.Clusters; i++ {
		var s = &pb.Cluster{}
		if err = decoder.Decode(&s); err != nil {
			return fmt.Errorf("parse cluster: %w", err)
		}
		f.clusterPut(s, false)
	}
	for i = 0; i < header.Namespaces; i++ {
		var s = &pb.Namespace{}
		if err = decoder.Decode(&s); err != nil {
			return fmt.Errorf("parse namespace: %w", err)
		}
		f.namespacePut(s, false)
	}
	for i = 0; i < header.Resources; i++ {
		var s = &pb.Resource{}
		if err = decoder.Decode(&s); err != nil {
			return fmt.Errorf("parse resource: %w", err)
		}
		f.resourcePut(s, false)
	}
	for i = 0; i < header.Components; i++ {
		var s = &pb.Component{}
		if err = decoder.Decode(&s); err != nil {
			return fmt.Errorf("parse component: %w", err)
		}
		f.componentPut(s, false)
	}
	for i = 0; i < header.Services; i++ {
		var s = &pb.Service{}
		if err = decoder.Decode(&s); err != nil {
			return fmt.Errorf("parse service: %w", err)
		}
		f.servicePut(s, false)
	}
	return nil
}
