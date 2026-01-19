package cgosqlite

import (
	"bytes"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tailscale/sqlite/sqliteh"
)

func TestBindParameterIndexSearch(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "test.db"), sqliteh.OpenFlagsDefault, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		name   string
		query  string
		param  string
		wantOK bool
	}{
		{"colon", "SELECT :foo", "foo", true},
		{"at_sybol", "SELECT @foo", "foo", true},
		{"dollar", "SELECT $foo", "foo", true},
		{"question", "SELECT ?123", "123", true},
		{"not_found", "SELECT :bar", "foo", false},
		{"dollar_multiple_params", "SELECT $a, $b, $c", "b", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, _, err := db.Prepare(tt.query, 0)
			if err != nil {
				t.Fatalf("Prepare %q: %v", tt.query, err)
			}
			defer stmt.Finalize()

			idx := stmt.BindParameterIndexSearch(tt.param)
			gotOK := idx > 0
			if gotOK != tt.wantOK {
				t.Errorf("BindParameterIndexSearch(%q) = %d, wantOK=%v gotOK=%v",
					tt.param, idx, tt.wantOK, gotOK)
			}
		})
	}
}

func TestColumnBlob(t *testing.T) {
	// Run the test with and without the SetAlwaysCopyBlob flag enabled.
	cases := []struct {
		name string
		flag bool
	}{
		{"off", false},
		{"on", true},
	}
	for _, tt := range cases {
		t.Run("SetAlwaysCopyBlob="+tt.name, func(t *testing.T) {
			SetAlwaysCopyBlob(tt.flag)

			// Open a test database
			db, err := Open(filepath.Join(t.TempDir(), "test.db"), sqliteh.OpenFlagsDefault, "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			mustRun(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")
			mustRun(t, db, `INSERT INTO t (id, data) VALUES (1, 'HELLOHELLOHELLOHELLOHELLOHELLO99')`)
			mustRun(t, db, `INSERT INTO t (id, data) VALUES (2, '')`)
			mustRun(t, db, `INSERT INTO t (id, data) VALUES (3, NULL)`)

			t.Run("WithData", func(t *testing.T) {
				stmt := queryRow(t, db, "SELECT data FROM t WHERE id = 1")
				data := stmt.ColumnBlob(0)

				const want = "HELLOHELLOHELLOHELLOHELLOHELLO99"
				if !bytes.Equal(data, []byte(want)) {
					t.Fatalf("got %q, want %q", data, want)
				}
			})

			t.Run("EmptyBlob", func(t *testing.T) {
				stmt := queryRow(t, db, "SELECT data FROM t WHERE id = 2")
				data := stmt.ColumnBlob(0)
				if len(data) != 0 {
					t.Fatalf("got %d bytes, want 0 bytes", len(data))
				}

				// NOTE: it appears that this returns a nil
				// slice, not a non-nil empty slice; both are
				// valid representations of an empty blob, so
				// we're not going to assert on which we get.
			})

			t.Run("NullBlob", func(t *testing.T) {
				stmt := queryRow(t, db, "SELECT data FROM t WHERE id = 3")
				data := stmt.ColumnBlob(0)
				if data != nil {
					t.Fatalf("got %q, want nil", data)
				}
			})
		})
	}
}

func TestColumnBlobModifiedHook(t *testing.T) {
	// Disable the "always copy blob" option to test just the hook behavior
	SetAlwaysCopyBlob(false)

	// Write to this channel every time a cleanup function executes, so we
	// can ensure they've run.
	checkRun := make(chan struct{}, 10_000) // high enough to never block
	blobCheckHook = func() {
		checkRun <- struct{}{}
	}
	t.Cleanup(func() {
		blobCheckHook = nil
	})

	// waitForCleanup waits for one cleanup to run.
	waitForCleanup := func() {
		timedOut := time.After(10 * time.Second)
		for {
			runtime.GC()
			runtime.Gosched()

			select {
			case <-checkRun:
				return
			case <-t.Context().Done():
				t.Fatal("test context done while waiting for cleanup")
			case <-timedOut:
				t.Fatal("timeout waiting for cleanup")
			case <-time.After(10 * time.Millisecond):
				// retry
			}
		}
	}

	// Open a test database
	db, err := Open(filepath.Join(t.TempDir(), "test.db"), sqliteh.OpenFlagsDefault, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a table with some blob data
	mustRun(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")

	// Use a blob larger than 16 bytes to avoid tiny object optimization which
	// can prevent cleanups from running (as mentioned in the documentation
	// for [runtime.AddCleanup]).
	mustRun(t, db, "INSERT INTO t (id, data) VALUES (1, CAST('HELLOHELLOHELLOHELLOHELLOHELLO99' AS BLOB))")

	const testQuery = "SELECT data FROM t WHERE id = 1"

	t.Run("UnmodifiedSliceDoesNotCallHook", func(t *testing.T) {
		var hookCalls atomic.Int64
		SetColumnBlobModifiedHook(func(query string) {
			hookCalls.Add(1)
		})
		defer SetColumnBlobModifiedHook(nil)

		func() {
			stmt := queryRow(t, db, testQuery)
			data := stmt.ColumnBlob(0)
			if len(data) != 32 {
				t.Fatalf("got len %d, want 32", len(data))
			}

			// Don't modify data, just let it go out of scope
			runtime.KeepAlive(data)
		}()

		waitForCleanup()
		if got := hookCalls.Load(); got != 0 {
			t.Errorf("hook called %d times, want 0", got)
		}
	})

	t.Run("ModifiedSliceCallsHook", func(t *testing.T) {
		var (
			hookCalls     atomic.Int64
			receivedQuery atomic.Pointer[string]

			calledOnce sync.Once
			called     = make(chan struct{})
		)
		SetColumnBlobModifiedHook(func(query string) {
			hookCalls.Add(1)
			receivedQuery.Store(&query)
			calledOnce.Do(func() { close(called) })
		})
		defer SetColumnBlobModifiedHook(nil)

		func() {
			stmt := queryRow(t, db, testQuery)
			data := stmt.ColumnBlob(0)
			if len(data) != 32 {
				t.Fatalf("got len %d, want 32", len(data))
			}

			// Modify the data to trigger our hook.
			data[0] = byte((int(data[0]) + 1) % 256)

			runtime.KeepAlive(data)
		}()

		waitForCleanup()
		<-called // need to synchronize separately since it's in another goroutine

		if got := hookCalls.Load(); got != 1 {
			t.Errorf("hook called %d times, want 1", got)
		}
		if q := receivedQuery.Load(); q == nil || *q != testQuery {
			got := ""
			if q != nil {
				got = *q
			}
			t.Errorf("hook received query %q, want %q", got, testQuery)
		}
	})

	t.Run("NilHook", func(t *testing.T) {
		SetColumnBlobModifiedHook(nil)

		// Ensure we start with an empty channel.
	drain:
		for {
			select {
			case <-checkRun:
			default:
				break drain
			}
		}

		func() {
			stmt := queryRow(t, db, testQuery)
			data := stmt.ColumnBlob(0)
			if len(data) != 32 {
				t.Fatalf("got len %d, want 32", len(data))
			}

			data[0] = 'Y'

			runtime.KeepAlive(data)
		}()

		// Spin for a bit to try and trigger any cleanups to be executed.
		for i := 0; i < 10; i++ {
			runtime.GC()
			runtime.Gosched()
			time.Sleep(10 * time.Millisecond)
		}

		// We expect nothing in the channel, as no hook is set.
		select {
		case <-checkRun:
			t.Fatal("unexpected cleanup hook call")
		default:
		}
	})
}

func mustRun(t *testing.T, db sqliteh.DB, sql string) {
	t.Helper()
	stmt, _, err := db.Prepare(sql, 0)
	if err != nil {
		t.Fatalf("Prepare %q: %v", sql, err)
	}
	if _, err := stmt.Step(nil); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if err := stmt.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
}

// queryRow runs the given query and returns the *Stmt for the first row.
func queryRow(t *testing.T, db sqliteh.DB, sql string) sqliteh.Stmt {
	t.Helper()
	stmt, _, err := db.Prepare(sql, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stmt.Finalize()
	})
	row, err := stmt.Step(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !row {
		t.Fatal("expected a row")
	}
	return stmt
}
