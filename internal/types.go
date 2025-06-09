package internal

import (
	"fmt"

	"github.com/logbn/zongzi"
)

const (
	ShardID = 1
)

type (
	Status string

	Entry  = zongzi.Entry
	Logger = zongzi.Logger
	Result = zongzi.Result
	Shard  = zongzi.Shard
	Agent  = zongzi.Agent

	LeaderInfo  = zongzi.LeaderInfo
	ShardOption = zongzi.ShardOption
)

var (
	Status_Pending     = Status("pending")
	Status_Active      = Status("active")
	Status_Unavailable = Status("unavailable")
	Status_Gone        = Status("gone")

	Type_Namespace = "namespace"
	Type_Resource  = "resource"
	Type_Component = "component"
	Type_Interface = "interface"
	Type_Endpoint  = "endpoint"
)

var (
	ErrCommandEmpty        = fmt.Errorf("Empty command")
	ErrCommandUnrecognized = fmt.Errorf("Unrecognized command")
	ErrCommandInvalid      = fmt.Errorf("Invalid command")
	ErrIndexNotMatch       = fmt.Errorf("Index does not match")
	ErrNotFound            = fmt.Errorf("Not Found")
)

type Item interface {
	GetID() uint64
	TagsAdd(map[string]string)
	TagsGet() map[string]string
	Touch(t uint64)
	Type() string
}

type Namespace struct {
	Tags    `json:"tags"`
	Created uint64 `json:"created"`
	Deleted uint64 `json:"deleted"`
	ID      uint64 `json:"id"`
	Updated uint64 `json:"updated"`

	Name string `json:"name" validate:"required"`
}

func (item Namespace) Type() string   { return Type_Namespace }
func (item Namespace) GetID() uint64  { return item.ID }
func (item Namespace) Touch(t uint64) { item.Updated = t }

type Resource struct {
	Tags    `json:"tags"`
	Config  []byte `json:"config"`
	Created uint64 `json:"created"`
	Deleted uint64 `json:"deleted"`
	ID      uint64 `json:"id"`
	Status  Status `json:"status"`
	Updated uint64 `json:"updated"`

	Name        string `json:"name" validate:"required"`
	NamespaceID uint64 `json:"namespaceID" validate:"required"`
	Uri         string `json:"uri" validate:"required"`
}

func (item Resource) Type() string   { return Type_Resource }
func (item Resource) GetID() uint64  { return item.ID }
func (item Resource) Touch(t uint64) { item.Updated = t }

type Component struct {
	Tags    `json:"tags"`
	Config  []byte `json:"config"`
	Created uint64 `json:"created"`
	Deleted uint64 `json:"deleted"`
	ID      uint64 `json:"id"`
	Status  Status `json:"status"`
	Updated uint64 `json:"updated"`

	Name       string `json:"name" validate:"required"`
	ResourceID uint64 `json:"resourceID" validate:"required"`
	Uri        string `json:"uri" validate:"required"`
}

func (item Component) Type() string   { return Type_Component }
func (item Component) GetID() uint64  { return item.ID }
func (item Component) Touch(t uint64) { item.Updated = t }

type Interface struct {
	Tags    `json:"tags"`
	Config  []byte `json:"config"`
	Created uint64 `json:"created"`
	Deleted uint64 `json:"deleted"`
	ID      uint64 `json:"id"`
	Status  Status `json:"status"`
	Updated uint64 `json:"updated"`

	Name       string `json:"name" validate:"required"`
	ResourceID uint64 `json:"resourceID" validate:"required"`
	Uri        string `json:"uri" validate:"required"`
}

func (item Interface) Type() string   { return Type_Interface }
func (item Interface) GetID() uint64  { return item.ID }
func (item Interface) Touch(t uint64) { item.Updated = t }

type Endpoint struct {
	Tags    `json:"tags"`
	Config  []byte `json:"config"`
	Created uint64 `json:"created"`
	Deleted uint64 `json:"deleted"`
	ID      uint64 `json:"id"`
	Status  Status `json:"status"`
	Updated uint64 `json:"updated"`

	HostID      string `json:"hostID" validate:"required"`
	InterfaceID uint64 `json:"interfaceID" validate:"required"`
	Port        string `json:"port"`
}

func (item Endpoint) Type() string   { return Type_Endpoint }
func (item Endpoint) GetID() uint64  { return item.ID }
func (item Endpoint) Touch(t uint64) { item.Updated = t }

type Tags map[string]string

func (t Tags) TagsGet() map[string]string {
	return t
}

func (t Tags) TagsAdd(tags map[string]string) {
	for k, v := range tags {
		t[k] = v
	}
}
