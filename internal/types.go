package internal

import (
	"fmt"

	"github.com/logbn/zongzi"
)

type (
	NamespaceStatus string
	ComponentStatus string
	ResourceStatus  string

	Entry  = zongzi.Entry
	Logger = zongzi.Logger
	Result = zongzi.Result
	Shard  = zongzi.Shard

	ShardOption = zongzi.ShardOption
)

var (
	NamespaceStatus_Pending     = NamespaceStatus("pending")
	NamespaceStatus_Active      = NamespaceStatus("active")
	NamespaceStatus_Unavailable = NamespaceStatus("unavailable")
	NamespaceStatus_Gone        = NamespaceStatus("gone")

	ResourceStatus_Pending     = ResourceStatus("pending")
	ResourceStatus_Active      = ResourceStatus("active")
	ResourceStatus_Unavailable = ResourceStatus("unavailable")
	ResourceStatus_Gone        = ResourceStatus("gone")

	ComponentStatus_Pending     = ComponentStatus("pending")
	ComponentStatus_Active      = ComponentStatus("active")
	ComponentStatus_Unavailable = ComponentStatus("unavailable")
	ComponentStatus_Gone        = ComponentStatus("gone")
)

var (
	ErrCommandEmpty        = fmt.Errorf("Empty command")
	ErrCommandUnrecognized = fmt.Errorf("Unrecognized command")
	ErrCommandInvalid      = fmt.Errorf("Invalid command")
	ErrIndexNotMatch       = fmt.Errorf("Index does not match")
	ErrNotFound            = fmt.Errorf("Not Found")
)

type Namespace struct {
	Name    string            `json:"name"`
	Created uint64            `json:"created"`
	Deleted uint64            `json:"deleted"`
	ID      string            `json:"id"`
	Status  NamespaceStatus   `json:"status"`
	Tags    map[string]string `json:"tags"`
	Updated uint64            `json:"updated"`
}

type Resource struct {
	Config      []byte            `json:"config"`
	Created     uint64            `json:"created"`
	Deleted     uint64            `json:"deleted"`
	ID          uint64            `json:"id"`
	Name        string            `json:"name" validate:"required"`
	NamespaceID uint64            `json:"namespaceID" validate:"required"`
	Status      ResourceStatus    `json:"status"`
	Tags        map[string]string `json:"tags"`
	Type        string            `json:"type" validate:"required"`
	Updated     uint64            `json:"updated"`
}

type Component struct {
	Config     []byte            `json:"config"`
	Created    uint64            `json:"created"`
	Deleted    uint64            `json:"deleted"`
	ID         uint64            `json:"id"`
	Name       string            `json:"name" validate:"required"`
	ResourceID uint64            `json:"resourceID" validate:"required"`
	Status     ComponentStatus   `json:"status"`
	Tags       map[string]string `json:"tags"`
	Type       string            `json:"type" validate:"required"`
	Updated    uint64            `json:"updated"`
}
