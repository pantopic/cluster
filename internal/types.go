package internal

import (
	"fmt"

	"github.com/logbn/zongzi"
)

const (
	ShardID = 1
)

type (
	NamespaceStatus string
	ComponentStatus string
	ResourceStatus  string

	Entry  = zongzi.Entry
	Logger = zongzi.Logger
	Result = zongzi.Result
	Shard  = zongzi.Shard
	Agent  = zongzi.Agent

	LeaderInfo  = zongzi.LeaderInfo
	ShardOption = zongzi.ShardOption
)

var (
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
	Name    string            `json:"name" validate:"required"`
	Created uint64            `json:"created"`
	Deleted uint64            `json:"deleted"`
	ID      uint64            `json:"id"`
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
