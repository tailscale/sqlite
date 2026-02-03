// Copyright (c) 2021 Tailscale Inc & AUTHORS All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"expvar"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tailscale/sqlite-exp/sqliteh"
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

func getUsesAfterClose() (ret int64) {
	UsesAfterClose.Do(func(kv expvar.KeyValue) {
		ret += kv.Value.(*expvar.Int).Value()
	})
	return ret
}

func checkBadUsageDB(t testing.TB, db *sql.DB) {
	initial := getUsesAfterClose()
	t.Cleanup(func() {
		final := getUsesAfterClose()
		if initial != final {
			t.Errorf("%d uses after finalization != %d final value", initial, final)
		}
	})
}

func openTestDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+t.TempDir()+"/test.db")
	if err != nil {
		t.Fatal(err)
	}
	configDB(t, db)
	checkBadUsageDB(t, db)
	return db
}

func openTestDBTrace(t testing.TB, tracer sqliteh.Tracer) *sql.DB {
	t.Helper()
	db := sql.OpenDB(Connector("file:"+t.TempDir()+"/test.db", nil, tracer))
	configDB(t, db)
	return db
}

// execContexter is an *sql.DB or an *sql.Tx.
type execContexter interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func exec(t *testing.T, db execContexter, query string, args ...any) sql.Result {
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
	var tests = []struct {
		in   string
		want time.Time
	}{
		{in: "2021-06-08 11:36:52.128+0000", want: time.Date(2021, 6, 8, 11, 36, 52, 128*1e6, time.UTC)},
		{in: "2021-06-08 11:36:52", want: time.Date(2021, 6, 8, 11, 36, 52, 0, time.UTC)},
		{in: "2021-06-08 11:36", want: time.Date(2021, 6, 8, 11, 36, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		db := openTestDB(t)
		exec(t, db, "CREATE TABLE t (c DATETIME)")
		exec(t, db, "INSERT INTO t VALUES (?)", tt.in)
		var got time.Time
		if err := db.QueryRowContext(context.Background(), "SELECT c FROM t").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if !got.Equal(tt.want) {
			t.Errorf("in=%v, want=%v, got=%v", tt.in, tt.want, got)
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

// TestEmptyStringNotNull verifies that binding an empty string results in an
// empty string value, not NULL. This is a regression test for a bug where
// unsafe.StringData("") could return nil, causing sqlite3_bind_text64 to
// receive a NULL pointer and treat the value as NULL instead of "".
func TestEmptyStringNotNull(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, db, "INSERT INTO t (id, val) VALUES (?, ?)", 1, "")
	exec(t, db, "INSERT INTO t (id, val) VALUES (?, ?)", 2, nil)
	exec(t, db, "INSERT INTO t (id, val) VALUES (?, ?)", 3, "ok")

	var val sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT val FROM t WHERE id = 1").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if !val.Valid {
		t.Fatal("empty string was stored as NULL")
	}
	if val.String != "" {
		t.Fatalf("val=%q, want empty string", val.String)
	}

	if err := db.QueryRowContext(ctx, "SELECT val FROM t WHERE id = 2").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if val.Valid {
		t.Fatalf("NULL was stored as %q", val.String)
	}

	var countEmpty, countNull int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t WHERE val = ''").Scan(&countEmpty); err != nil {
		t.Fatal(err)
	}
	if countEmpty != 1 {
		t.Fatalf("countEmpty=%d, want 1", countEmpty)
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t WHERE val IS NULL").Scan(&countNull); err != nil {
		t.Fatal(err)
	}
	if countNull != 1 {
		t.Fatalf("countNull=%d, want 1", countNull)
	}

	var length sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT length(val) FROM t WHERE id = 1").Scan(&length); err != nil {
		t.Fatal(err)
	}
	if !length.Valid {
		t.Fatal("length of empty string returned NULL")
	}
	if length.Int64 != 0 {
		t.Fatalf("length=%d, want 0", length.Int64)
	}
}

func TestExecScript(t *testing.T) {
	db := openTestDB(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
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

	err = sqlConn.Raw(func(driverConn any) error {
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
	err = sqlConn.Raw(func(driverConn any) error {
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

func TestWithQueryCancel_Timeout(t *testing.T) {
	// This test query runs forever until interrupted.
	const testQuery = `WITH RECURSIVE inf(n) AS (
    SELECT 1
    UNION ALL
    SELECT n+1 FROM inf
) SELECT * FROM inf WHERE n = 0`

	db := openTestDB(t)

	t.Run("QueryContext", func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			defer close(done)

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			rows, err := db.QueryContext(WithQueryCancel(ctx), testQuery)
			if err != nil {
				t.Errorf("QueryContext: unexpected error: %v", err)
				return
			}
			for rows.Next() {
				t.Error("Next result available before timeout")
			}
			if err := rows.Err(); err == nil {
				t.Error("Rows did not report an error")
			} else if !strings.Contains(err.Error(), "SQLITE_INTERRUPT") {
				t.Errorf("Rows err=%v, want SQLITE_INTERRUPT", err)
			}
		}()

		select {
		case <-done:
			// OK
		case <-time.After(30 * time.Second):
			t.Fatal("Timeout waiting for query to end")
		}
	})
	t.Run("ExecContext", func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			defer close(done)

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			res, err := db.ExecContext(WithQueryCancel(ctx), testQuery)
			if err == nil {
				t.Errorf("ExecContext: got %v, want error", res)
			} else if !strings.Contains(err.Error(), "SQLITE_INTERRUPT") {
				t.Errorf("ExecContext err=%v, want SQLITE_INTERRUPT", err)
			}
		}()

		select {
		case <-done:
			// OK
		case <-time.After(30 * time.Second):
			t.Fatal("Timeout waiting for query to end")
		}
	})
}

func TestWithQueryCancel_OK(t *testing.T) {
	db := openTestDB(t)

	t.Run("QueryContext", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			t.Run(strconv.Itoa(i+1), func(t *testing.T) {
				// Set a timeout that is much longer than the expected runtime of the query.
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				rows, err := db.QueryContext(WithQueryCancel(ctx), `select 1`)
				if err != nil {
					t.Fatalf("QueryContext: unexpected error: %v", err)
				}
				for rows.Next() {
					var z int
					if err := rows.Scan(&z); err != nil {
						t.Fatalf("Scan: %v", err)
					} else if z != 1 {
						t.Errorf("Scan: got %d, want 1", z)
					}
				}
				if err := rows.Err(); err != nil {
					t.Errorf("Err reported %v", err)
				}
				if err := rows.Close(); err != nil {
					t.Errorf("Close reported %v", err)
				}
			})
		}
	})
	t.Run("ExecContext", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			t.Run(strconv.Itoa(i+1), func(t *testing.T) {
				// Set a timeout that is much longer than the expected runtime of the query.
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				_, err := db.ExecContext(WithQueryCancel(ctx), `select 1`)
				if err != nil && !strings.Contains(err.Error(), "SQLITE_INTERRUPT") {
					t.Errorf("ExecContext: unexpected error: %v", err)
				}
			})
		}
	})
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
	defer conn.Close()
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
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
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

type queryTraceEvent struct {
	prepCtx  context.Context
	query    string
	duration time.Duration
	err      error
}

type queryTracer struct {
	evCh chan queryTraceEvent
}

func (t *queryTracer) Query(prepCtx context.Context, id sqliteh.TraceConnID, query string, duration time.Duration, err error) {
	t.evCh <- queryTraceEvent{prepCtx, query, duration, err}
}
func (t *queryTracer) BeginTx(_ context.Context, _ sqliteh.TraceConnID, _ string, _ bool, _ error) {}
func (t *queryTracer) Commit(_ sqliteh.TraceConnID, _ error) {
}
func (t *queryTracer) Rollback(_ sqliteh.TraceConnID, _ error) {
}
func (t *queryTracer) Close(_ sqliteh.TraceConnID, _ error) {}

func TestTraceQuery(t *testing.T) {
	tracer := &queryTracer{
		evCh: make(chan queryTraceEvent, 16),
	}
	type ctxKey struct{}
	expectEv := func(srcCtx context.Context, query string, errSubstr string) {
		t.Helper()
		ev, ok := <-tracer.evCh
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
			// The macOS clock appears to low resolution and so
			// it's common to get a duration of exactly 0s.
			if runtime.GOOS != "darwin" || ev.duration != 0 {
				t.Errorf("trace: improbable duration: %v", ev.duration)
			}
		}
	}
	db := openTestDBTrace(t, tracer)
	noErr := ""
	expectEv(context.Background(), "PRAGMA journal_mode=WAL", noErr) // from configDB
	expectEv(context.Background(), "PRAGMA synchronous=OFF", noErr)

	execCtx := func(ctx context.Context, query string, args ...any) {
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

func TestTxnState(t *testing.T) {
	dbFile := t.TempDir() + "/test.db"
	db, err := sql.Open("sqlite3", "file:"+dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()
	if state, err := TxnState(sqlConn, ""); err != nil {
		t.Fatal(err)
	} else if state != sqliteh.SQLITE_TXN_NONE {
		t.Errorf("state=%v, want SQLITE_TXN_NONE", state)
	}
	if err := ExecScript(sqlConn, "BEGIN; CREATE TABLE t (c);"); err != nil {
		t.Fatal(err)
	}
	if state, err := TxnState(sqlConn, ""); err != nil {
		t.Fatal(err)
	} else if state != sqliteh.SQLITE_TXN_WRITE {
		t.Errorf("state=%v, want SQLITE_TXN_WRITE", state)
	}
	if err := ExecScript(sqlConn, "COMMIT; BEGIN; SELECT * FROM (t);"); err != nil {
		t.Fatal(err)
	}
	if state, err := TxnState(sqlConn, ""); err != nil {
		t.Fatal(err)
	} else if state != sqliteh.SQLITE_TXN_READ {
		t.Errorf("state=%v, want SQLITE_TXN_READ", state)
	}
}

func TestConnInit(t *testing.T) {
	called := 0
	uri := "file:" + t.TempDir() + "/test.db"
	connInitFunc := func(ctx context.Context, conn driver.ConnPrepareContext) error {
		called++
		return ExecScript(conn.(SQLConn), "PRAGMA journal_mode=WAL;")
	}
	db := sql.OpenDB(Connector(uri, connInitFunc, nil))
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if called == 0 {
		t.Fatal("called=0, want non-zero")
	}
	conn.Close()
	db.Close()
}

func TestTxReadOnly(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("create table t (c)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("insert into t (c) values (1)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow("select count(*) from t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("count=%d, want 1", count)
	}
	if _, err := tx.Exec("insert into t (c) values (1)"); err == nil {
		t.Fatal("no error on read-only insert")
	} else if !strings.Contains(err.Error(), "SQLITE_READONLY") {
		t.Errorf("err does not reference SQLITE_READONLY: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("insert into t (c) values (1)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestAttachOrderingDeadlock fails if transactions use SQLite's default
// BEGIN DEFERRED semantics, as the two databases locks are acquired in
// the wrong order. This tests that BEGIN IMMEDIATE resolves this.
func TestAttachOrderingDeadlock(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	db := sql.OpenDB(Connector("file:"+tmpdir+"/test.db", func(ctx context.Context, conn driver.ConnPrepareContext) error {
		c := conn.(SQLConn)
		err := ExecScript(c, `
			ATTACH DATABASE "file:`+tmpdir+`/test2.db" AS attached;
			PRAGMA busy_timeout=10000;
			PRAGMA main.journal_mode=WAL;
			PRAGMA attached.journal_mode=WAL;
			CREATE TABLE IF NOT EXISTS main.m1 (c);
			CREATE TABLE IF NOT EXISTS main.m2 (c);
			CREATE TABLE IF NOT EXISTS attached.a1 (c);
			CREATE TABLE IF NOT EXISTS attached.a2 (c);

		`)
		if err != nil {
			return err
		}
		return nil
	}, nil))
	defer db.Close()

	// Prime the connections.
	const numConcurrent = 10
	db.SetMaxOpenConns(numConcurrent)
	db.SetMaxIdleConns(numConcurrent)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	var conns []*sql.Conn
	for i := 0; i < numConcurrent; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		c.Close()
	}

	lockTables := func(name string, tables ...string) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer tx.Rollback()

		for _, table := range tables {
			// Read from and write to the same table to lock it.
			_, err := tx.ExecContext(WithPersist(ctx), `INSERT INTO `+table+` SELECT * FROM `+table+` LIMIT 0`)
			if err != nil {
				t.Error(err)
				return
			}
		}
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	// The following goroutines write to the main and noise databases in
	// a different order. This should not result in a deadlock.
	for i := 0; i < numConcurrent; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			lockTables("main-then-attached", "main.m1", "attached.a1")
		}()
		go func() {
			defer wg.Done()
			lockTables("attached-then-main", "attached.a2", "main.m2")
		}()
	}
}

func TestSetWALHook(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	var conns []*sql.Conn
	for i := 1; i <= 2; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conns = append(conns, conn)
	}

	got := []string{}
	for i := 1; i <= 2; i++ {
		hookGen := i
		for connNum, conn := range conns {
			connNum := connNum
			err := SetWALHook(conn, func(dbName string, pages int) {
				s := fmt.Sprintf("conn=%d, db=%s, pages=%v", connNum, dbName, pages)
				if hookGen == 2 { // verify our hook replacement worked
					got = append(got, s)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	if _, err := conns[0].ExecContext(ctx, "CREATE TABLE foo (k INT, v INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conns[1].ExecContext(ctx, "INSERT INTO foo VALUES (1, 2)"); err != nil {
		t.Fatal(err)
	}

	// Disable the hook.
	for _, conn := range conns {
		if err := SetWALHook(conn, nil); err != nil {
			t.Fatal(err)
		}
	}
	// And do another write that we shouldn't get a callback for.
	if _, err := conns[1].ExecContext(ctx, "INSERT INTO foo VALUES (2, 3)"); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"conn=0, db=main, pages=2",
		"conn=1, db=main, pages=3",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("wrong\n got: %q\nwant: %q", got, want)
	}

	// Check allocs
	if err := SetWALHook(conns[0], func(dbName string, pages int) {}); err != nil {
		t.Fatal(err)
	}

	stmt, err := conns[0].PrepareContext(WithPersist(ctx), "UPDATE foo SET v = v + 1 WHERE k in (SELECT k FROM foo LIMIT 1)")
	if err != nil {
		t.Fatal(err)
	}
	n := testing.AllocsPerRun(10000, func() {
		if _, err := stmt.Exec(); err != nil {
			t.Fatal(err)
		}
	})
	const maxAllocs = 3 // as of Go 1.20
	if n > maxAllocs {
		t.Errorf("allocs = %v; want no more than %v", n, maxAllocs)
	}
}

// Tests that we don't remember the SQLite column types of the first row of the
// result set (notably the "NULL" type) and re-use it for all subsequent rows
// like we used to.
func TestNoStickyColumnTypes(t *testing.T) {
	db := openTestDB(t)
	exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v1 ANY, v2 ANY)")

	type row []any
	r := func(v ...any) row { return v }
	rs := func(v ...row) []row { return v }
	tests := []struct {
		name string
		rows []row
	}{
		{"no-null", rs(
			r("a", "b"),
			r("foo", "bar"))},
		{"only-null", rs(
			r(nil, nil))},
		{"null-after-string", rs(
			r("a", "b"),
			r(nil, "bar"))},
		{"string-after-null", rs(
			r(nil, "b"),
			r("foo", "bar"))},
		{"null-after-int", rs(
			r(101, 102),
			r(nil, 202))},
		{"int-after-null", rs(
			r(nil, 102),
			r(201, 202))},
		{"changing-types-within-a-column-between-rows", rs(
			r("foo", nil),
			r(nil, 2),
			r(3, "bar"))},
	}

	// canonical maps from types we get back out from sqlite
	// to the types we provided in the test cases above.
	canonical := func(v any) any {
		switch v := v.(type) {
		default:
			return v
		case []byte:
			return string(v)
		case int64:
			return int(v)
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec(t, db, "DELETE FROM t")
			for primaryKey, r := range tt.rows {
				exec(t, db, "INSERT INTO t VALUES (?, ?, ?)", append([]any{primaryKey}, r...)...)
			}
			rows, err := db.Query("SELECT id, v1, v2 FROM t ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var id int
				var v1, v2 any
				err := rows.Scan(&id, &v1, &v2)
				if err != nil {
					t.Fatal(err)
				}
				v1, v2 = canonical(v1), canonical(v2)
				want := tt.rows[id]
				got := row{v1, v2}
				t.Logf("[%v]: %T, %T", id, v1, v2)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("row %d got %v; want %v", id, got, want)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUsesAfterClose(t *testing.T) {
	ctx := context.Background()

	// Clean up metric after we're done testing it.
	t.Cleanup(func() {
		UsesAfterClose.Init()
	})

	connector := Connector("file:"+t.TempDir()+"/test.db", nil, nil)
	sqlConn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn := sqlConn.(*conn)

	initial := getUsesAfterClose()

	// Close the conn, then use something from the conn which triggers our
	// "used after close" logic.
	conn.Close()
	if _, err = conn.PrepareContext(ctx, "SELECT 1;"); err == nil {
		t.Error("expected error, got nil")
	}

	final := getUsesAfterClose()
	if final != initial+1 {
		t.Errorf("got UsesAfterClose=%d, want %d", final, initial+1)
	}
}

func BenchmarkWALHookAndExec(b *testing.B) {
	ctx := context.Background()
	db := openTestDB(b)
	conn, err := db.Conn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	if err := SetWALHook(conn, func(dbName string, pages int) {}); err != nil {
		b.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TABLE foo (k INT, v INT)"); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	stmt, err := conn.PrepareContext(WithPersist(ctx), "UPDATE foo SET v=123") // will match no rows
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		if _, err := stmt.Exec(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPersist(b *testing.B) {
	b.ReportAllocs()
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

func BenchmarkQueryRows100MixedTypes(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()
	db := openTestDB(b)
	conn, err := db.Conn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	err = ExecScript(conn, `BEGIN;
		CREATE TABLE t (id INTEGER);
		COMMIT;`)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := db.Exec("INSERT INTO t (id) VALUES (?)", i); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()

	ctx = WithPersist(ctx)

	var id int
	var raw sql.RawBytes
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(ctx, "SELECT id, 'hello world some string' FROM t")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			if err := rows.Scan(&id, &raw); err != nil {
				b.Fatal(err)
			}
		}
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEmptyExec(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()
	db := openTestDB(b)
	ctx = WithPersist(ctx)
	for i := 0; i < b.N; i++ {
		if _, err := db.ExecContext(ctx, "SELECT null LIMIT 0;"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBeginTxNoop(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()
	db := openTestDB(b)
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			b.Fatal(err)
		}

		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// TODO(crawshaw): test TextMarshaler
// TODO(crawshaw): test named types
// TODO(crawshaw): check coverage

// This tests that we don't give the same *stmt to two different callers that
// prepare the same persistent query. See:
//
//	https://github.com/tailscale/sqlite-exp/issues/73
func TestPrepareReuse(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	sqlConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()

	// Insert a bunch of values into a table that we'll query to get
	// multiple rows back.
	err = ExecScript(sqlConn,
		`BEGIN;
		CREATE TABLE t (c);
		INSERT INTO t VALUES (1), (2), (3), (4);
		COMMIT;`)
	if err != nil {
		t.Fatal(err)
	}

	ctx = WithPersist(ctx)

	// Calling PrepareContext twice in a row used to return the same
	// statement to both callers.
	const query = "SELECT c FROM t ORDER BY c;"
	stmt1, err := sqlConn.PrepareContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt1.Close()
	stmt2, err := sqlConn.PrepareContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt2.Close()

	rows1, err := stmt1.QueryContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rows1.Close()
	rows2, err := stmt2.QueryContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rows2.Close()

	assertResult := func(rows *sql.Rows, want int) {
		t.Helper()
		var num int
		if err := rows.Scan(&num); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if num != want {
			t.Fatalf("num=%d, want %d", num, want)
		}
	}

	// Each set of rows should get a full copy of the query results; if
	// these are incorrectly shared, then advancing one Rows will change
	// the results from the other.
	for i := 0; i < 4; i++ {
		if !rows1.Next() {
			t.Fatalf("[1] pass %d: Next=false", i)
		}
		if !rows2.Next() {
			t.Fatalf("[2] pass %d: Next=false", i)
		}

		// rows2 should be different from row1 and should return a
		// different set of values.
		assertResult(rows1, i+1)
		assertResult(rows2, i+1)
	}
}

func TestRegression(t *testing.T) {
	// A regression test for a query comprising only comments, which caused the
	// driver to panic in statement cleanup.
	t.Run("CommentOnlyQuery", func(t *testing.T) {
		db := openTestDB(t)

		rows, err := db.Query("-- comments only\n-- nothing else")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		rows.Next()
		t.Log("OK") // Reaching here at all means we didn't panic.
	})
}

func TestDisableFunction(t *testing.T) {
	db := openTestDB(t)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx := context.Background()

	if _, err := conn.ExecContext(ctx, "SELECT LOWER('Hi')"); err != nil {
		t.Fatal("Attempting to use the LOWER function before disabling should have been allowed")
	}

	if err := DisableFunction(conn, "lower", 1); err != nil {
		t.Fatal(err)
	}

	if _, err := conn.ExecContext(ctx, "SELECT LOWER('Hi')"); err == nil {
		t.Fatal("Attempting to use the LOWER function after disabling should have failed")
	}
}

type connLogger struct {
	ch         chan []string
	statements []string
	panicOnUse bool
}

func (cl *connLogger) Begin() {
	if cl.panicOnUse {
		panic("unexpected connLogger.Begin()")
	}
	cl.statements = nil
}

func (cl *connLogger) Statement(s string) {
	if cl.panicOnUse {
		panic("unexpected connLogger.Statement: " + s)
	}
	cl.statements = append(cl.statements, s)
}

func (cl *connLogger) Commit(err error) {
	if cl.panicOnUse {
		panic("unexpected connLogger.Commit()")
	}
	if err != nil {
		return
	}
	cl.ch <- cl.statements
}

func (cl *connLogger) Rollback() {
	if cl.panicOnUse {
		panic("unexpected connLogger.Rollback()")
	}
	cl.statements = nil
}

func TestConnLogger_writable(t *testing.T) {
	for _, commit := range []bool{true, false} {
		doneStatement := "ROLLBACK"
		if commit {
			doneStatement = "COMMIT"
		}
		t.Run(doneStatement, func(t *testing.T) {
			ctx := context.Background()
			ch := make(chan []string, 1)
			txl := connLogger{ch: ch}
			makeLogger := func() ConnLogger { return &txl }
			db := sql.OpenDB(ConnectorWithLogger("file:"+t.TempDir()+"/test.db", nil, nil, makeLogger))
			configDB(t, db)

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec("CREATE TABLE T (x INTEGER)"); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec("INSERT INTO T VALUES (?)", 1); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Query("SELECT x FROM T"); err != nil {
				t.Fatal(err)
			}
			done := tx.Rollback
			if commit {
				done = tx.Commit
			}
			if err := done(); err != nil {
				t.Fatal(err)
			}
			if !commit {
				select {
				case got := <-ch:
					t.Errorf("unexpectedly logged statements for rollback:\n%s", strings.Join(got, "\n"))
				default:
					return
				}
			}

			want := []string{
				"BEGIN IMMEDIATE",
				"CREATE TABLE T (x INTEGER)",
				"INSERT INTO T VALUES (1)",
				doneStatement,
			}
			select {
			case got := <-ch:
				if !slices.Equal(got, want) {
					t.Errorf("unexpected log statements. got:\n%s\n\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
				}
			default:
				t.Fatal("no logged statements after commit")
			}
		})
	}
}

func TestConnLogger_commit_error(t *testing.T) {
	ctx := context.Background()
	ch := make(chan []string, 1)
	txl := connLogger{ch: ch}
	makeLogger := func() ConnLogger { return &txl }
	db := sql.OpenDB(ConnectorWithLogger("file:"+t.TempDir()+"/test.db", nil, nil, makeLogger))
	configDB(t, db)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE A (x INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE B (x INTEGER REFERENCES A(X) DEFERRABLE INITIALLY DEFERRED)"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO B VALUES (?)", 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("expected Commit to error, but didn't")
	}
	select {
	case got := <-ch:
		t.Errorf("unexpectedly logged statements for errored commit:\n%s", strings.Join(got, "\n"))
	default:
		return
	}
}

func TestConnLogger_read_tx(t *testing.T) {
	ctx := context.Background()
	ch := make(chan []string, 1)
	txl := connLogger{ch: ch}
	makeLogger := func() ConnLogger { return &txl }
	db := sql.OpenDB(ConnectorWithLogger("file:"+t.TempDir()+"/test.db", nil, nil, makeLogger))
	configDB(t, db)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("CREATE TABLE T (x INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO T VALUES (?)", 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if len(got) == 0 {
			t.Errorf("expected logged statements for write tx")
		}
	default:
		t.Errorf("expected logged statements for write tx")
	}

	txl.panicOnUse = true
	for _, commit := range []bool{true, false} {
		rx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rx.Query("SELECT x FROM T"); err != nil {
			t.Fatal(err)
		}
		done := rx.Rollback
		if commit {
			done = rx.Commit
		}
		if err := done(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestExpandedSQL(t *testing.T) {
	ctx := context.Background()
	connector := Connector("file:"+t.TempDir()+"/test.db", nil, nil)
	sqlConn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	conn := sqlConn.(*conn)

	sqlStmt, err := conn.PrepareContext(ctx, "SELECT ? + ?")
	if err != nil {
		t.Fatalf("PrepareContext: %v", err)
	}
	stmt, ok := sqlStmt.(*stmt)
	if !ok {
		t.Fatalf("not a *stmt: %#v", stmt)
	}
	if err := stmt.bindAll([]driver.NamedValue{
		{Ordinal: 1, Value: 6},
		{Ordinal: 2, Value: 7},
	}); err != nil {
		t.Errorf("bindAll: %v", err)
	}

	if got, want := stmt.stmt.ExpandedSQL(), "SELECT 6 + 7"; got != want {
		t.Errorf("wrong sql: got %q, want %q", got, want)
	}
}
