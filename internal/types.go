package internal

import (
	"fmt"
	"maps"

	"github.com/logbn/zongzi"
)

const (
	ShardID = 1

	NAMESPACE_DEFAULT  = `default`
	NAMESPACE_INTERNAL = `internal`
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

	Type_Cluster   = "cluster"
	Type_Component = "component"
	Type_Endpoint  = "endpoint"
	Type_Namespace = "namespace"
	Type_Resource  = "resource"
	Type_Service   = "service"
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
	Type() string
}

type Cluster struct {
	Tags    `json:"tags"`
	Created uint64 `json:"created"`
	Deleted uint64 `json:"deleted"`
	ID      uint64 `json:"id"`
	Updated uint64 `json:"updated"`

	Name string `json:"name" validate:"required"`
}

func (item Cluster) Type() string  { return Type_Cluster }
func (item Cluster) GetID() uint64 { return item.ID }

type Namespace struct {
	Tags    `json:"tags"`
	Created uint64 `json:"created"`
	Deleted uint64 `json:"deleted"`
	ID      uint64 `json:"id"`
	Updated uint64 `json:"updated"`

	Name string `json:"name" validate:"required"`
}

func (item Namespace) Type() string  { return Type_Namespace }
func (item Namespace) GetID() uint64 { return item.ID }

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

func (item Resource) Type() string  { return Type_Resource }
func (item Resource) GetID() uint64 { return item.ID }

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

func (item Component) Type() string  { return Type_Component }
func (item Component) GetID() uint64 { return item.ID }

type Service struct {
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

func (item Service) Type() string  { return Type_Service }
func (item Service) GetID() uint64 { return item.ID }

type Endpoint struct {
	Tags    `json:"tags"`
	Config  []byte `json:"config"`
	Created uint64 `json:"created"`
	Deleted uint64 `json:"deleted"`
	ID      uint64 `json:"id"`
	Status  Status `json:"status"`
	Updated uint64 `json:"updated"`

	HostID    string `json:"hostID" validate:"required"`
	ServiceID uint64 `json:"serviceID" validate:"required"`
	Port      string `json:"port"`
}

func (item Endpoint) Type() string  { return Type_Endpoint }
func (item Endpoint) GetID() uint64 { return item.ID }

type Tags map[string]string

func (t Tags) TagsGet() map[string]string {
	return t
}

func (t Tags) TagsAdd(tags map[string]string) {
	if t == nil {
		t = map[string]string{}
	}
	maps.Copy(t, tags)
}
