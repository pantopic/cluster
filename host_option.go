package cluster

import (
	"github.com/pantopic/ext-global/host-wazero"
)

type HostOption func(*Host)

func WithGlobalOverlay(env wazero_global.Lookup) HostOption {
	return func(h *Host) {
		h.extGlobal.LayerAdd(env)
	}
}
