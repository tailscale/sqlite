//go:build cgo
// +build cgo

package sqlite

import (
	"sync"
	"testing"

	"github.com/tailscale/sqlite/cgosqlite"
	"github.com/tailscale/sqlite/sqliteh"
)

// ensure LogCallback is convertible to cgosqlite.LogCallback
var _ cgosqlite.LogCallback = cgosqlite.LogCallback(LogCallback(func(code sqliteh.Code, msg string) {}))

func TestSetLogCallback(t *testing.T) {
	var mu sync.Mutex
	var logs []string

	err := SetLogCallback(func(code sqliteh.Code, msg string) {
		mu.Lock()
		defer mu.Unlock()
		logs = append(logs, msg)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer SetLogCallback(nil)

	db := openTestDB(t)

	_, err = db.Exec("SELECT * FROM nonexistent_table")
	if err == nil {
		t.Fatal("expected error from invalid SQL")
	}

	mu.Lock()
	gotLogs := len(logs) > 0
	mu.Unlock()

	if !gotLogs {
		t.Fatal("expected to receive log messages")
	}
}
