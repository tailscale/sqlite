//go:build cgo
// +build cgo

package sqlite

import "github.com/tailscale/sqlite/cgosqlite"

func init() {
	Open = cgosqlite.Open
}
