//go:build cgo

package sqlitepool

import (
	"context"
	"errors"
	"testing"

	"github.com/tailscale/sqlite-exp/sqliteh"
	"github.com/tailscale/sqlite-exp/sqlstats"
)

func TestPool(t *testing.T) {
	ctx := context.Background()
	initFn := func(db sqliteh.DB) error {
		err := ExecScript(db, `
			PRAGMA synchronous=OFF;
			PRAGMA journal_mode=WAL;
			`)
		return err
	}
	tracer := &sqlstats.Tracer{}
	tempDir := t.TempDir()
	p, err := NewPool("file:"+tempDir+"/sqlitepool_test", 3, initFn, tracer)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := p.BeginTx(ctx, "insert-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec("CREATE TABLE t (c);"); err != nil {
		t.Fatal(err)
	}
	stmt := tx.Prepare("INSERT INTO t (c) VALUES (?);")
	stmt.BindInt64(1, 1)
	if _, _, _, _, err := stmt.StepResult(); err != nil {
		t.Fatal(err)
	}
	var onCommitCalled, onRollbackCalled bool
	tx.OnCommit = func() { onCommitCalled = true }
	tx.OnRollback = func() { onRollbackCalled = true }
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx.Rollback() // no-op, does not call OnRollback
	if !onCommitCalled {
		t.Fatal("onCommit not called")
	}
	if onRollbackCalled {
		t.Fatal("onRollback called")
	}
	if err := tx.Commit(); err == nil {
		t.Fatalf("want error on second commit, got: %v", err)
	}

	tx, err = p.BeginTx(ctx, "insert-2")
	if err != nil {
		t.Fatal(err)
	}
	stmt2 := tx.Prepare("INSERT INTO t (c) VALUES (?);")
	if stmt != stmt2 {
		t.Fatalf("second call to prepare returned a different stmt: %p vs. %p", stmt, stmt2)
	}
	stmt = stmt2
	stmt.BindInt64(1, 2)
	if _, _, _, _, err := stmt.StepResult(); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			const want = `SQLITE_ERROR: near "INVALID": syntax error`
			if r := recover(); r == nil {
				t.Fatal("no panic from invalid prepare")
			} else if r != want {
				t.Fatalf("invalid sql recover: %q, want %q", r, want)
			}
		}()
		tx.Prepare("INVALID SQL")
	}()
	onCommitCalled = false
	onRollbackCalled = false
	tx.OnCommit = func() { onCommitCalled = true }
	tx.OnRollback = func() { onRollbackCalled = true }
	tx.Rollback()
	if onCommitCalled {
		t.Fatal("onCommit called")
	}
	if !onRollbackCalled {
		t.Fatal("onRollback not called")
	}
	if err := tx.Commit(); err == nil {
		t.Fatalf("want error on commit after rollback, got: %v", err)
	}
	tx.Rollback() // no-op

	rx1, err := p.BeginRx(ctx, "read-1")
	if err != nil {
		t.Fatal(err)
	}
	defer rx1.Rollback()
	rx2, err := p.BeginRx(ctx, "read-2")
	if err != nil {
		t.Fatal(err)
	}
	defer rx2.Rollback()

	ctxCancel, cancel := context.WithCancel(ctx)
	rx3Err := make(chan error, 1)
	go func() {
		rx3, err := p.BeginRx(ctxCancel, "read-3")
		if err != nil {
			rx3Err <- err
			return
		}
		rx3.Rollback()
		rx3Err <- errors.New("BeginRx(read-3) did not fail")
	}()
	cancel()
	if err := <-rx3Err; err != context.Canceled {
		t.Fatalf("read-3, not context canceled: %v", err)
	}

	stmt = rx1.Prepare("SELECT count(*) FROM t")
	if row, err := stmt.Step(nil); err != nil {
		t.Fatal(err)
	} else if !row {
		t.Fatal("no row from select count")
	}
	if got, want := int(stmt.ColumnInt64(0)), 1; got != want {
		t.Fatalf("got=%d, want %d", got, want)
	}
	rx1.Rollback()
	rx1.Rollback() // no-op

	rx1, err = p.BeginRx(ctx, "read-1") // now another rx is available
	if err != nil {
		t.Fatal(err)
	}
	rx1.Rollback()
	rx2.Rollback()

	tx, err = p.BeginTx(ctx, "insert-3")
	if err != nil {
		t.Fatal(err)
	}
	if err := ExecScript(tx.DB(), "PRAGMA user_version=5"); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if r := recover(); r != "Tx.Rx.Rollback called, only call Rollback on the Tx object" {
				t.Fatalf("expected panic from Tx.Rx.Rollback, got: %q", r)
			}
		}()
		tx.Rx.Rollback()
	}()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatalf("second commit did not fail, want 'already done'")
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p.Close() // no-op

	if _, err := p.BeginTx(ctx, "after-close"); err == nil {
		t.Fatal("tx-after-close did not fail")
	}
	if _, err := p.BeginRx(ctx, "after-close"); err == nil {
		t.Fatal("rx-after-close did not fail")
	}
}
