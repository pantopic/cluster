package internal

import (
	"context"
	"fmt"
	"net"

	"github.com/logbn/zongzi"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	"github.com/pantopic/wazero-pool"
)

const (
	ShardID = 1

	NAMESPACE_DEFAULT  = `default`
	NAMESPACE_INTERNAL = `internal`
)

const (
	ResultSuccess uint64 = iota
	ResultErrAuthentication
	ResultErrAuthorization
	ResultErrConflict
	ResultErrExists
	ResultErrInternal
	ResultErrNotFound
	ResultErrSemantic
	ResultErrSyntactic
)

const (
	CMD_UNKNOWN byte = iota
	CMD_CLUSTER_DELETE
	CMD_CLUSTER_PUT
	CMD_COMPONENT_DELETE
	CMD_COMPONENT_PUT
	CMD_NAMESPACE_DELETE
	CMD_NAMESPACE_PUT
	CMD_RESOURCE_DELETE
	CMD_RESOURCE_PUT
	CMD_SERVICE_DELETE
	CMD_SERVICE_PUT
)

const (
	QUERY_UNKNOWN byte = iota
	QUERY_CLUSTER_GET
	QUERY_CLUSTER_LIST
	QUERY_COMPONENT_GET
	QUERY_COMPONENT_LIST
	QUERY_NAMESPACE_GET
	QUERY_NAMESPACE_LIST
	QUERY_RESOURCE_GET
	QUERY_RESOURCE_LIST
	QUERY_SERVICE_GET
	QUERY_SERVICE_LIST
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
	Status_Stopped     = Status("stopped")
	Status_Gone        = Status("gone")
)

var (
	ErrCommandEmpty        = fmt.Errorf("Empty command")
	ErrCommandUnrecognized = fmt.Errorf("Unrecognized command")
	ErrCommandInvalid      = fmt.Errorf("Invalid command")
	ErrIndexNotMatch       = fmt.Errorf("Index does not match")
	ErrNotFound            = fmt.Errorf("Not Found")
)

type ContextCopy = func(dst, src context.Context) context.Context

type Extension interface {
	Name() string
	Register(context.Context, wazero.Runtime) error
	InitContext(context.Context, api.Module) (context.Context, error)
	ContextCopy(dst, src context.Context) context.Context
}

type ServiceProvider interface {
	ServiceType() string
	ServerStart(ctx context.Context, lis net.Listener, tlsCrt, tlsKey string, pool wazeropool.Instance, ctxCopy ...ContextCopy) (err error)
	ServerStop(ctx context.Context, lis net.Listener) (err error)
}
