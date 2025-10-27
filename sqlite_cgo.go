//go:build cgo
// +build cgo

package sqlite

import (
	"github.com/tailscale/sqlite/cgosqlite"
	"github.com/tailscale/sqlite/sqliteh"
)

func init() {
	Open = cgosqlite.Open
}

// LogCallback receives SQLite log messages.
type LogCallback func(code sqliteh.Code, msg string)

// SetLogCallback sets the global SQLite log callback.
// If callback is nil, logs are discarded.
func SetLogCallback(callback LogCallback) error {
	return cgosqlite.SetLogCallback(cgosqlite.LogCallback(callback))
}
