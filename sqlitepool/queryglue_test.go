//go:build cgo

package sqlitepool

import (
	"context"
	"database/sql"
	"testing"

	"github.com/tailscale/sqlite-exp/sqliteh"
	"github.com/tailscale/sqlite-exp/sqlstats"
)

func TestQueryGlue(t *testing.T) {
	ctx := context.Background()
	initFn := func(db sqliteh.DB) error { return ExecScript(db, "PRAGMA synchronous=OFF;") }
	tracer := &sqlstats.Tracer{}
	tempDir := t.TempDir()
	p, err := NewPool("file:"+tempDir+"/sqlitepool_queryglue_test", 2, initFn, tracer)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := p.BeginTx(ctx, "insert-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := Exec(tx.DB(), "INSERT INTO t VALUES (?, ?)", 10, "skip"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec("INSERT INTO t VALUES (?, ?)", 100, "a"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec("INSERT INTO t VALUES (?, ?)", 200, "b"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec("INSERT INTO t VALUES (?, ?)", 300, "c"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec("INSERT INTO t VALUES (?, ?)", 400, "d"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec("INSERT INTO t VALUES (?, ?)", 401, "skip"); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := tx.QueryRow("SELECT count(*) FROM t WHERE id >= ? AND id <= ?", 100, 400).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Fatalf("count=%d, want 4", count)
	}
	if err := tx.QueryRow("SELECT id FROM t WHERE id >= ?", 900).Scan(&count); err != sql.ErrNoRows {
		t.Fatalf("QueryRow err=%v, want ErrNoRows", err)
	}

	rows, err := tx.Query("SELECT * FROM t WHERE id >= ? AND id <= ?", 100, 400)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if !rows.Next() {
			t.Fatalf("pass %d: Next=false", i)
		}
		var id int64
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("pass %d: Scan: %v", i, err)
		}
		if want := int64(i+1) * 100; id != want {
			t.Fatalf("pass %d: id=%d, want %d", i, id, want)
		}
		if want := string([]byte{'a' + byte(i)}); val != want {
			t.Fatalf("pass %d: val=%q want %q", i, val, want)
		}
	}
	if rows.Next() {
		t.Fatal("too many rows")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	var concat sql.RawBytes
	if err := tx.QueryRow("SELECT val FROM t WHERE id = 401").Scan(&concat); err != nil {
		t.Fatal(err)
	}
	if got, want := string(concat), "skip"; got != want {
		t.Fatalf("concat=%q, want %q", got, want)
	}

	tx.Rollback()
	p.Close()
}
