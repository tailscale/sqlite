//go:build !sqlite_enable_multithreaded_checks

package cgosqlite

import (
	"testing"
)

func TestMultithreadedChecksDisabled(t *testing.T) {
	testMultithreadedChecks(t, false)
}
