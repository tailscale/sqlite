//go:build cgo

package sqlite

import (
	"github.com/tailscale/sqlite/cgosqlite"
)

func init() {
	Open = cgosqlite.Open
}

// SetLogCallback sets the global SQLite log callback.
// If callback is nil, logs are discarded.
func SetLogCallback(callback LogCallback) error {
	return cgosqlite.SetLogCallback(cgosqlite.LogCallback(callback))
}
