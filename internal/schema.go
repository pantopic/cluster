package internal

import (
	"github.com/hashicorp/go-memdb"
)

func newSchema() *memdb.DBSchema {
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
	schema.Tables[`bank`] = &memdb.TableSchema{
		Name: "bank",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:   "id",
				Unique: true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{Field: "Namespace"},
						&memdb.StringFieldIndex{Field: "Resource"},
						&memdb.StringFieldIndex{Field: "Cluster"},
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
	return schema
}
