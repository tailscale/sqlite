//go:build sqlite_enable_multithreaded_checks

package cgosqlite

import (
	"testing"
)

func TestMultithreadedChecksEnabled(t *testing.T) {
	testMultithreadedChecks(t, true)
}
