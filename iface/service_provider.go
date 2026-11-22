package pantopic

import (
	"context"
	"net"

	"github.com/pantopic/wazero-pool"
)

type ContextCopy = func(dst, src context.Context) context.Context

type ServiceProvider interface {
	ServiceType() string
	ServerStart(ctx context.Context, lis net.Listener, tlsCrt, tlsKey string, pool wazeropool.Instance, ctxCopy ...ContextCopy) (err error)
	ServerStop(ctx context.Context, lis net.Listener) (err error)
	ServerUpdate(ctx context.Context, pool wazeropool.Instance, ctxCopy ...ContextCopy) (err error)
}
