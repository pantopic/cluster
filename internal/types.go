package internal

import (
	"fmt"

	"github.com/logbn/zongzi"
)

const (
	ShardID = 1

	NAMESPACE_DEFAULT  = `default`
	NAMESPACE_INTERNAL = `internal`
)

const (
	ResultSuccess uint64 = iota
	ResultErrSyntactic
	ResultErrSemantic
	ResultErrAuthentication
	ResultErrAuthorization
	ResultErrNotFound
	ResultErrConflict
	ResultErrInternal
	ResultErrExists
)

const (
	CMD_UNKNOWN byte = iota
	CMD_NAMESPACE_DELETE
	CMD_NAMESPACE_PUT
	CMD_CLUSTER_DELETE
	CMD_CLUSTER_PUT
	CMD_SERVICE_DELETE
	CMD_SERVICE_PUT
	CMD_RESOURCE_DELETE
	CMD_RESOURCE_PUT
	CMD_COMPONENT_DELETE
	CMD_COMPONENT_PUT
)

const (
	QUERY_UNKNOWN byte = iota
	QUERY_NAMESPACE_GET
	QUERY_NAMESPACE_LIST
	QUERY_CLUSTER_GET
	QUERY_CLUSTER_LIST
	QUERY_SERVICE_GET
	QUERY_SERVICE_LIST
	QUERY_RESOURCE_GET
	QUERY_RESOURCE_LIST
	QUERY_COMPONENT_GET
	QUERY_COMPONENT_LIST
)

type (
	Status = string

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
)

var (
	ErrCommandEmpty        = fmt.Errorf("Empty command")
	ErrCommandUnrecognized = fmt.Errorf("Unrecognized command")
	ErrCommandInvalid      = fmt.Errorf("Invalid command")
	ErrIndexNotMatch       = fmt.Errorf("Index does not match")
	ErrNotFound            = fmt.Errorf("Not Found")
)
