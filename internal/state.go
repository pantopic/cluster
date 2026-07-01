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
	schema := newSchema()
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		panic(err)
	}
	return &State{db: db}
}

func (s *State) withTxn(write bool) *State {
	s2 := statePool.Get().(*State)
	s2.db = s.db
	s2.txn = s.db.Txn(write)
	return s2
}

func (s *State) commit() {
	s.txn.Commit()
	statePool.Put(s)
}

func (s *State) abort() {
	s.txn.Abort()
	statePool.Put(s)
}

func (s *State) itemDelete(name string, item any) uint64 {
	err := s.txn.Delete(name, item)
	if err == nil {
		return ResultSuccess
	}
	if err == memdb.ErrNotFound {
		return ResultErrNotFound
	}
	panic(err)
}

func resultIterSeq[V any](iter memdb.ResultIterator) func(yield func(V) bool) {
	return func(yield func(V) bool) {
		for {
			val := iter.Next()
			if val == nil {
				break
			}
			if !yield(val.(V)) {
				break
			}
		}
	}
}

func (s *State) Save(w io.Writer) error {
	f := s.withTxn(false)
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
func (s *State) recover(r io.Reader) (err error) {
	f := s.withTxn(true)
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
