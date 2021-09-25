// Copyright (c) 2021 Tailscale Inc & AUTHORS All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tailscale/sqlite/sqliteh"
)

func TestOpenDB(t *testing.T) {
	db := openTestDB(t)
	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if want := "wal"; journalMode != want {
		t.Errorf("journal_mode=%q, want %q", journalMode, want)
	}
	var synchronous string
	if err := db.QueryRow("PRAGMA synchronous;").Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if want := "0"; synchronous != want {
		t.Errorf("synchronous=%q, want %q", synchronous, want)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func configDB(t testing.TB, db *sql.DB) {
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA synchronous=OFF"); err != nil {
		t.Fatal(err)
	}
	numConns := runtime.GOMAXPROCS(0)
	db.SetMaxOpenConns(numConns)
	db.SetMaxIdleConns(numConns)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	t.Cleanup(func() { db.Close() })
}

func openTestDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+t.TempDir()+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	configDB(t, db)
	return db
}

func openTestDBTrace(t testing.TB, traceFunc TraceFunc) *sql.DB {
	t.Helper()
	db := sql.OpenDB(Connector("file:"+t.TempDir()+"/test.db", traceFunc))
	configDB(t, db)
	return db
}

// execContexter is an *sql.DB or an *sql.Tx.
type execContexter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func exec(t *testing.T, db execContexter, query string, args ...interface{}) sql.Result {
	t.Helper()
	ctx := context.Background()
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestTrailingTextError(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF;")
	if err == nil {
		t.Error("missing error from trailing command")
	}
	if !strings.Contains(err.Error(), "trailing text") {
		t.Errorf("error does not mention 'trailing text': %v", err)
	}
}

func TestInsertResults(t *testing.T) {
	db := openTestDB(t)
	exec(t, db, "CREATE TABLE t (c)")
	res := exec(t, db, "INSERT INTO t VALUES ('a')")
	if id, err := res.LastInsertId(); err != nil {
		t.Fatal(err)
	} else if id != 1 {
		t.Errorf("LastInsertId=%d, want 1", id)
	}
	if rows, err := res.RowsAffected(); err != nil {
		t.Fatal(err)
	} else if rows != 1 {
		t.Errorf("RowsAffected=%d, want 1", rows)
	}

	res = exec(t, db, "INSERT INTO t VALUES ('b')")
	if id, err := res.LastInsertId(); err != nil {
		t.Fatal(err)
	} else if id != 2 {
		t.Errorf("LastInsertId=%d, want 1", id)
	}

	exec(t, db, "INSERT INTO t VALUES ('c')")
	exec(t, db, "CREATE TABLE t2 (c)")
	res = exec(t, db, "INSERT INTO t2 SELECT c from t;")
	if id, err := res.LastInsertId(); err != nil {
		t.Fatal(err)
	} else if id != 3 {
		t.Errorf("LastInsertId=%d, want 1", id)
	}
	if rows, err := res.RowsAffected(); err != nil {
		t.Fatal(err)
	} else if rows != 3 {
		t.Errorf("RowsAffected=%d, want 1", rows)
	}
}

func TestExecAndScanSequence(t *testing.T) {
	db := openTestDB(t)
	exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, db, "INSERT INTO t VALUES (?, ?)", 10, "skip")
	exec(t, db, "INSERT INTO t VALUES (?, ?)", 100, "a")
	exec(t, db, "INSERT INTO t VALUES (?, ?)", 200, "b")
	exec(t, db, "INSERT INTO t VALUES (?, ?)", 300, "c")
	exec(t, db, "INSERT INTO t VALUES (?, ?)", 400, "d")
	exec(t, db, "INSERT INTO t VALUES (?, ?)", 401, "skip")

	rows, err := db.Query("SELECT * FROM t WHERE id >= ? AND id <= :max", 100, sql.Named("max", 400))
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
}

func TestTx(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, tx, "CREATE TABLE t (c);")
	exec(t, tx, "INSERT INTO t VALUES (1);")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err == nil {
		t.Fatal("rollback of committed Tx did not error")
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, tx, "INSERT INTO t VALUES (2);")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("count=%d, want 1", count)
	}
}

func TestValueConversion(t *testing.T) {
	db := openTestDB(t)
	var cInt int64
	var cFloat float64
	var cText string
	var cBlob []byte
	var cNull *string
	err := db.QueryRowContext(context.Background(), `SELECT
		CAST(4 AS INTEGER),
		CAST(4.0 AS FLOAT),
		CAST('txt' AS TEXT),
		CAST('txt' AS BLOB),
		NULL`).Scan(&cInt, &cFloat, &cText, &cBlob, &cNull)
	if err != nil {
		t.Fatal(err)
	}
	if cInt != 4 {
		t.Errorf("cInt=%d, want 4", cInt)
	}
	if cFloat != 4.0 {
		t.Errorf("cFloat=%v, want 4.0", cFloat)
	}
	if cText != "txt" {
		t.Errorf("cText=%v, want 'txt'", cText)
	}
	if string(cBlob) != "txt" {
		t.Errorf("cBlob=%v, want 'txt'", cBlob)
	}
	if cNull != nil {
		t.Errorf("cNull=%v, want nil", cNull)
	}
}

func TestTime(t *testing.T) {
	t1Str := "2021-06-08 11:36:52.444-0700"
	t1, err := time.Parse(TimeFormat, t1Str)
	if err != nil {
		t.Fatal(err)
	}
	var t2 time.Time

	db := openTestDB(t)
	exec(t, db, "CREATE TABLE t (c DATETIME)")
	exec(t, db, "INSERT INTO t VALUES (?)", t1)
	err = db.QueryRowContext(context.Background(), "SELECT c FROM t").Scan(&t2)
	if err != nil {
		t.Fatal(err)
	}
	var txt string
	err = db.QueryRowContext(context.Background(), "SELECT CAST(c AS TEXT) FROM t").Scan(&txt)
	if err != nil {
		t.Fatal(err)
	}
	if want := t1Str; txt != want {
		t.Errorf("time stored as %q, want %q", txt, want)
	}

	exec(t, db, "CREATE TABLE t2 (c FOOD)")
	exec(t, db, "INSERT INTO t2 VALUES (?)", t1)
	if err := db.QueryRowContext(context.Background(), "SELECT c FROM t2").Scan(&t2); err == nil {
		t.Fatal("expect an error trying to interpet FOOD as Time")
	}
}

func TestShortTimes(t *testing.T) {
	var tests = []time.Time{
		time.Date(2021, 6, 8, 11, 36, 52, 128*1e6, time.UTC),
		time.Date(2021, 6, 8, 11, 36, 52, 0, time.UTC),
		time.Date(2021, 6, 8, 11, 36, 0, 0, time.UTC),
	}

	for _, t0 := range tests {
		db := openTestDB(t)
		exec(t, db, "CREATE TABLE t (c DATETIME)")
		exec(t, db, "INSERT INTO t VALUES (?)", t0)
		var tOut time.Time
		err := db.QueryRowContext(context.Background(), "SELECT c FROM t").Scan(&tOut)
		if err != nil {
			t.Fatal(err)
		}
		if !tOut.Equal(t0) {
			t.Errorf("t0=%v, tOut=%v", t0, tOut)
		}

	}
}

func TestEmptyString(t *testing.T) {
	db := openTestDB(t)
	exec(t, db, "CREATE TABLE t (c)")
	exec(t, db, "INSERT INTO t VALUES (?)", "")
	exec(t, db, "INSERT INTO t VALUES (?)", "")
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count=%d, want 2", count)
	}
}

func TestExecScript(t *testing.T) {
	db := openTestDB(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = ExecScript(conn, `BEGIN;
		CREATE TABLE t (c);
		INSERT INTO t VALUES ('a');
		INSERT INTO t VALUES ('b');
		COMMIT;`)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count=%d, want 2", count)
	}
}

func TestWithPersist(t *testing.T) {
	db := openTestDB(t)
	exec(t, db, "CREATE TABLE t (c)")

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()

	ins := "INSERT INTO t VALUES (?)"
	if _, err := sqlConn.ExecContext(ctx, ins, 1); err != nil {
		t.Fatal(err)
	}

	err = sqlConn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*conn)
		if c.stmts[ins] != nil {
			return fmt.Errorf("query %q was persisted", ins)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sqlConn.ExecContext(WithPersist(ctx), ins, 2); err != nil {
		t.Fatal(err)
	}
	err = sqlConn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*conn)
		if c.stmts[ins] == nil {
			return fmt.Errorf("query %q was not persisted", ins)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestErrors(t *testing.T) {
	db := openTestDB(t)
	exec(t, db, "CREATE TABLE t (c)")
	exec(t, db, "INSERT INTO t (c) VALUES (1)")
	exec(t, db, "INSERT INTO t (c) VALUES (2)")

	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT c FROM t;")
	if err != nil {
		t.Fatal(err)
	}
	exec(t, db, "DROP TABLE t")
	if rows.Next() {
		t.Error("rows")
	}
	err = rows.Err()
	if err == nil {
		t.Fatal("no error")
	}
	// Test use of ErrMsg to elaborate on the error.
	if want := "no such table: t"; !strings.Contains(err.Error(), want) {
		t.Errorf("err=%v, want %q", err, want)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = ExecScript(conn, `BEGIN; NOT VALID SQL;`)
	if err == nil {
		t.Fatal("no error")
	}
	if want := `near "NOT": syntax error`; !strings.Contains(err.Error(), want) {
		t.Errorf("err=%v, want %q", err, want)
	}
	if err := ExecScript(conn, "ROLLBACK;"); err != nil { // TODO: make unnecessary?
		t.Fatal(err)
	}

	err = ExecScript(conn, `CREATE TABLE t (c INTEGER PRIMARY KEY);
		INSERT INTO t (c) VALUES (1);
		INSERT INTO t (c) VALUES (1);`)
	if err == nil {
		t.Fatal("no error")
	}
	if want := `UNIQUE constraint failed: t.c`; !strings.Contains(err.Error(), want) {
		t.Errorf("err=%v, want %q", err, want)
	}

	_, err = conn.ExecContext(ctx, "INSERT INTO t (c) VALUES (1);")
	if err == nil {
		t.Fatal("no error")
	}
	if want := `Stmt.Exec: SQLITE_CONSTRAINT: UNIQUE constraint failed: t.c`; !strings.Contains(err.Error(), want) {
		t.Errorf("err=%v, want %q", err, want)
	}
}

func TestCheckpoint(t *testing.T) {
	dbFile := t.TempDir() + "/test.db"
	db, err := sql.Open("sqlite3", "file:"+dbFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = ExecScript(conn, `CREATE TABLE t (c);
		INSERT INTO t (c) VALUES (1);
		INSERT INTO t (c) VALUES (1);`)
	if err != nil {
		t.Fatal(err)
	}

	if fi, err := os.Stat(dbFile + "-wal"); err != nil {
		t.Fatal(err)
	} else if fi.Size() == 0 {
		t.Fatal("WAL is empty")
	} else {
		t.Logf("WAL is %d bytes", fi.Size())
	}

	if _, _, err := Checkpoint(conn, "", sqliteh.SQLITE_CHECKPOINT_TRUNCATE); err != nil {
		t.Fatal(err)
	}

	if fi, err := os.Stat(dbFile + "-wal"); err != nil {
		t.Fatal(err)
	} else if fi.Size() != 0 {
		t.Fatal("WAL is not empty")
	}
}

func TestTrace(t *testing.T) {
	type traceEvent struct {
		prepCtx  context.Context
		query    string
		duration time.Duration
		err      error
	}
	evCh := make(chan traceEvent, 16)
	fn := func(prepCtx context.Context, query string, duration time.Duration, err error) {
		evCh <- traceEvent{prepCtx, query, duration, err}
	}
	type ctxKey struct{}
	expectEv := func(srcCtx context.Context, query string, errSubstr string) {
		t.Helper()
		ev, ok := <-evCh
		if !ok {
			t.Fatal("trace: no event")
		}
		if ev.prepCtx == nil {
			t.Errorf("trace: prepCtx==nil")
		} else if want, got := srcCtx.Value(ctxKey{}), ev.prepCtx.Value(ctxKey{}); want != got {
			t.Errorf("trace: prepCtx value=%v, want %v", got, want)
		}
		if ev.query != query {
			t.Errorf("trace: query=%q, want %q", ev.query, query)
		}
		switch {
		case ev.err == nil && errSubstr != "":
			t.Errorf("trace: err=nil, want %q", errSubstr)
		case ev.err != nil && errSubstr == "":
			t.Errorf("trace: err=%v, want nil", ev.err)
		case ev.err != nil && !strings.Contains(ev.err.Error(), errSubstr):
			t.Errorf("trace: err=%v, want %v", ev.err, errSubstr)
		}
		if ev.duration <= 0 || ev.duration > 10*time.Minute {
			t.Errorf("trace: improbable duration: %v", ev.duration)
		}
	}
	db := openTestDBTrace(t, fn)
	noErr := ""
	expectEv(context.Background(), "PRAGMA journal_mode=WAL", noErr) // from configDB
	expectEv(context.Background(), "PRAGMA synchronous=OFF", noErr)

	execCtx := func(ctx context.Context, query string, args ...interface{}) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
		expectEv(ctx, query, noErr)
	}
	ctx := WithPersist(context.Background())
	ctx = context.WithValue(ctx, ctxKey{}, 7)
	execCtx(ctx, "CREATE TABLE t (c)")

	ins := "INSERT INTO t VALUES (?)"
	execCtx(ctx, ins, 1)
	execCtx(WithPersist(ctx), ins, 2)

	execCtx(ctx, "SELECT null LIMIT 0;")

	rows, err := db.QueryContext(ctx, "SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var val int64
		if err := rows.Scan(&val); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	expectEv(ctx, "SELECT * FROM t", noErr)

	_, err = db.ExecContext(ctx, "DELETOR")
	if err == nil {
		t.Fatal(err)
	}
	expectEv(ctx, "DELETOR", err.Error())

	execCtx(context.WithValue(ctx, ctxKey{}, 9), "CREATE TABLE t2 (c INTEGER PRIMARY KEY)")
	execCtx(ctx, "INSERT INTO t2 (c) VALUES (1)")
	_, err = db.ExecContext(ctx, "INSERT INTO t2 (c) VALUES (1)")
	if err == nil {
		t.Fatal(err)
	}
	expectEv(ctx, "INSERT INTO t2 (c) VALUES (1)", "UNIQUE constraint failed")
}

func BenchmarkPersist(b *testing.B) {
	ctx := context.Background()
	db := openTestDB(b)
	conn, err := db.Conn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	err = ExecScript(conn, `BEGIN;
		CREATE TABLE t (c);
		INSERT INTO t VALUES ('a');
		INSERT INTO t VALUES ('b');
		COMMIT;`)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		var str string
		if err := db.QueryRowContext(WithPersist(ctx), "SELECT c FROM t LIMIT 1").Scan(&str); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEmptyExec(b *testing.B) {
	ctx := context.Background()
	db := openTestDB(b)
	ctx = WithPersist(ctx)
	for i := 0; i < b.N; i++ {
		if _, err := db.ExecContext(ctx, "SELECT null LIMIT 0;"); err != nil {
			b.Fatal(err)
		}
	}
}

// TODO(crawshaw): test TextMarshaler
// TODO(crawshaw): test named types
// TODO(crawshaw): check coverage
