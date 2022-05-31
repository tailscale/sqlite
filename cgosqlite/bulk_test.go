package cgosqlite_test

import (
	"testing"

	"github.com/tailscale/sqlite/cgosqlite"
	"github.com/tailscale/sqlite/sqliteh"
	"tailscale.com/tstest"
)

func TestBulkExec(t *testing.T) {
	db, err := cgosqlite.Open("file:mem?mode=memory", sqliteh.OpenFlagsDefault, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := db.Close()
		if !t.Failed() {
			if err != nil {
				t.Error(err)
			}
		}
	}()

	stmt, _, err := db.Prepare("CREATE TABLE t (c0, c1, c2);", 0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := cgosqlite.NewBulkExec(stmt.(*cgosqlite.Stmt))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := b.Exec(); err != nil {
		t.Fatal(err)
	}
	if err := b.Finalize(); err != nil {
		t.Fatal(err)
	}

	stmt, _, err = db.Prepare("INSERT INTO t (c0, c1, c2) VALUES (?, ?, ?);", 0)
	if err != nil {
		t.Fatal(err)
	}
	b, err = cgosqlite.NewBulkExec(stmt.(*cgosqlite.Stmt))
	if err != nil {
		t.Fatal(err)
	}
	b.SetNull(0)
	b.SetInt64(1, 42)
	b.SetText(2, []byte("hello, world!"))
	if _, changes, _, err := b.Exec(); err != nil {
		t.Fatal(err)
	} else if changes != 1 {
		t.Errorf("changes=%d, want 1", changes)
	}
	msg := []byte("hello, world!")

	// TODO: this should be zero allocs, but for some reason it's 1?
	err = tstest.MinAllocsPerRun(t, 1, func() {
		b.SetNull(0)
		b.SetInt64(1, 43)
		b.SetText(2, msg)
		if _, changes, _, err := b.Exec(); err != nil {
			t.Fatal(err)
		} else if changes != 1 {
			t.Errorf("changes=%d, want 1", changes)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Finalize(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkBulkExec(b *testing.B) {
	dir := b.TempDir()
	db, err := cgosqlite.Open("file:"+dir+"/testbulkdexec", sqliteh.OpenFlagsDefault, "")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	stmt, _, err := db.Prepare("PRAGMA journal_mode=WAL;", 0)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := stmt.Step(); err != nil {
		b.Fatal(err)
	}
	stmt.Finalize()

	stmt, _, err = db.Prepare("PRAGMA synchronous=NORMAL;", 0)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := stmt.Step(); err != nil {
		b.Fatal(err)
	}
	stmt.Finalize()

	stmt, _, err = db.Prepare("CREATE TABLE t (c);", 0)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := stmt.Step(); err != nil {
		b.Fatal(err)
	}
	stmt.Finalize()

	stmt, _, err = db.Prepare("INSERT INTO t (c) VALUES (?);", 0)
	if err != nil {
		b.Fatal(err)
	}
	bstmt, err := cgosqlite.NewBulkExec(stmt.(*cgosqlite.Stmt))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		bstmt.SetInt64(0, 42)
		if _, _, _, err := bstmt.Exec(); err != nil {
			b.Fatal(err)
		}
	}

	if err := bstmt.Finalize(); err != nil {
		b.Fatal(err)
	}
}

func TestBulkQuery(t *testing.T) {
	db, err := cgosqlite.Open("file:mem?mode=memory", sqliteh.OpenFlagsDefault, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := db.Close()
		if !t.Failed() {
			if err != nil {
				t.Error(err)
			}
		}
	}()

	stmt, _, err := db.Prepare("CREATE TABLE t (c0, c1, c2);", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Step(); err != nil {
		t.Fatal(err)
	}
	stmt.Finalize()

	stmt, _, err = db.Prepare("INSERT INTO t (c0, c1, c2) VALUES (?, ?, ?);", 0)
	if err != nil {
		t.Fatal(err)
	}
	const totalRows = 128*3 + 3 // not a clean multiple of dataArrLen
	for i := 0; i < totalRows; i++ {
		stmt.Reset()
		stmt.BindInt64(1, int64(i))
		stmt.BindText64(2, "hello c1")
		stmt.BindText64(3, "bye c2")
		if _, err := stmt.Step(); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Finalize()

	stmt, _, err = db.Prepare("SELECT c0, c1, c2 FROM t;", 0)
	if err != nil {
		t.Fatal(err)
	}
	bstmt, err := cgosqlite.NewBulkQuery(stmt.(*cgosqlite.Stmt))
	if err != nil {
		t.Fatal(err)
	}
	bstmt.Query()
	rows := 0
	for bstmt.Next() {
		if got, want := bstmt.Int64(0), int64(rows); got != want {
			t.Errorf("row %d, col0=%d, want %d", rows, got, rows)
		}
		if got, want := string(bstmt.Text(1)), "hello c1"; got != want {
			t.Errorf("row %d, col1=%q, want %q", rows, got, want)
		}
		if got, want := string(bstmt.Text(2)), "bye c2"; got != want {
			t.Errorf("row %d, col2=%q, want %q", rows, got, want)
		}
		rows++
	}
	if err := bstmt.Error(); err != nil {
		t.Fatal(err)
	}
	if rows != totalRows {
		t.Fatalf("rows=%d, want totalRows %d", rows, totalRows)
	}
	if err := bstmt.Finalize(); err != nil {
		t.Fatal(err)
	}
}
