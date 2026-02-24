//

package cgosqlite

import (
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tailscale/sqlite/sqliteh"
)

// testMultithreadedChecks provides a common function for testing SQLITE_ENABLE_MULTITHREADED_CHECKS.
func testMultithreadedChecks(t *testing.T, wantThreadingWarning bool) {
	if wantThreadingWarning && !MultithreadedChecksEnabled() {
		t.Fatal("Multithreaded checks are not enabled")
	} else if !wantThreadingWarning && MultithreadedChecksEnabled() {
		t.Fatal("Multithreaded checks are enabled")
	}

	var gotMisuseLog atomic.Bool
	err := SetLogCallback(func(code sqliteh.Code, msg string) {
		if code == sqliteh.SQLITE_MISUSE && msg == "illegal multi-threaded access to database connection" {
			gotMisuseLog.Store(true)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	// Lock this goroutine to a thread (preventing other goroutines from using that thread)
	runtime.LockOSThread()

	flags := sqliteh.SQLITE_OPEN_READWRITE |
		sqliteh.SQLITE_OPEN_CREATE |
		sqliteh.SQLITE_OPEN_WAL |
		sqliteh.SQLITE_OPEN_URI |
		sqliteh.SQLITE_OPEN_NOMUTEX
	db, err := Open(filepath.Join(t.TempDir(), "test.db"), flags, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	hitAPI := func() {
		for i := 0; i < 1000 && !gotMisuseLog.Load(); i++ {
			// Prepare a statement on this thread, mostly ignoring errors.
			stmt, _, err := db.Prepare("CREATE TABLE t(c INTEGER PRIMARY KEY)", 0)
			if err != nil {
				continue
			}
			if _, err := stmt.Step(nil); err != nil {
				continue
			}
			_ = stmt.Finalize()
		}
	}

	// Hit API on a separate goroutine as well as in this goroutine.
	// Because the original goroutine locked the OS thread, this new goroutine
	// will execute on a separate thread.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		hitAPI()
	}()
	hitAPI()
	wg.Wait()

	if wantThreadingWarning && !gotMisuseLog.Load() {
		t.Fatal("did not get SQLITE_MISUSE in LogCallback")
	}
	if !wantThreadingWarning && gotMisuseLog.Load() {
		t.Fatal("got SQLITE_MISUSE in LogCallback")
	}
}
