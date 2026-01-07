package cgosqlite

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tailscale/sqlite/sqliteh"
)

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

			mustRun := func(sql string) {
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

			mustRun("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")
			mustRun(`INSERT INTO t (id, data) VALUES (1, 'HELLOHELLOHELLOHELLOHELLOHELLO99')`)
			mustRun(`INSERT INTO t (id, data) VALUES (2, '')`)
			mustRun(`INSERT INTO t (id, data) VALUES (3, NULL)`)

			// queryRow runs the given query and returns the *Stmt for the first row.
			queryRow := func(t *testing.T, sql string) sqliteh.Stmt {
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

			t.Run("WithData", func(t *testing.T) {
				stmt := queryRow(t, "SELECT data FROM t WHERE id = 1")
				data := stmt.ColumnBlob(0)

				const want = "HELLOHELLOHELLOHELLOHELLOHELLO99"
				if !bytes.Equal(data, []byte(want)) {
					t.Fatalf("got %q, want %q", data, want)
				}
			})

			t.Run("EmptyBlob", func(t *testing.T) {
				stmt := queryRow(t, "SELECT data FROM t WHERE id = 2")
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
				stmt := queryRow(t, "SELECT data FROM t WHERE id = 3")
				data := stmt.ColumnBlob(0)
				if data != nil {
					t.Fatalf("got %q, want nil", data)
				}
			})
		})
	}
}
