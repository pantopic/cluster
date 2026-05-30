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
	ClusterID   uint64 `json:"clusterID"`
	Clusters    uint64 `json:"clusters"`
	ComponentID uint64 `json:"componentID"`
	Components  uint64 `json:"components"`
	EndpointID  uint64 `json:"endpointID"`
	Endpoints   uint64 `json:"endpoints"`
	Index       uint64 `json:"index"`
	NamespaceID uint64 `json:"namespaceID"`
	Namespaces  uint64 `json:"namespaces"`
	ResourceID  uint64 `json:"resourceID"`
	Resources   uint64 `json:"resources"`
	ServiceID   uint64 `json:"serviceID"`
	Services    uint64 `json:"services"`
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
	clusterSchema := &memdb.TableSchema{
		Name: "cluster",
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
			"ClusterID": {
				Name:    "ClusterID",
				Indexer: &memdb.UintFieldIndex{Field: "ClusterID"},
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
	serviceSchema := &memdb.TableSchema{
		Name: "service",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.UintFieldIndex{Field: "ID"},
			},
			"ClusterID": {
				Name:    "ClusterID",
				Indexer: &memdb.UintFieldIndex{Field: "ClusterID"},
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
	endpointSchema := &memdb.TableSchema{
		Name: "endpoint",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.UintFieldIndex{Field: "ID"},
			},
			"ServiceID": {
				Name:    "ServiceID",
				Indexer: &memdb.UintFieldIndex{Field: "ServiceID"},
			},
			"HostID": {
				Name:    "HostID",
				Indexer: &memdb.UintFieldIndex{Field: "HostID"},
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
			"cluster":   clusterSchema,
			"component": componentSchema,
			"endpoint":  endpointSchema,
			"meta":      metaSchema,
			"namespace": namespaceSchema,
			"resource":  resourceSchema,
			"service":   serviceSchema,
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
		ClusterID:   f.metaGetVal(`clusterID`),
		ComponentID: f.metaGetVal(`componentID`),
		EndpointID:  f.metaGetVal(`endpointID`),
		Index:       f.metaGetVal(`index`),
		NamespaceID: f.metaGetVal(`namespaceID`),
		ResourceID:  f.metaGetVal(`resourceID`),
		ServiceID:   f.metaGetVal(`serviceID`),
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
