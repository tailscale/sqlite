//go:build cgo

// Package sqlitepool implements a pool of SQLite database connections.
package sqlitepool

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tailscale/sqlite-exp/cgosqlite"
	"github.com/tailscale/sqlite-exp/sqliteh"
)

// A Pool is a fixed-size pool of SQLite database connections.
// One is reserved for writable transactions, the others are
// used for read-only transactions.
type Pool struct {
	poolSize    int
	rwConnFree  chan *conn // cap == 1
	roConnsFree chan *conn // cap == poolSize-1
	tracer      sqliteh.Tracer
	closed      chan struct{}
}

type conn struct {
	pool  *Pool
	db    sqliteh.DB
	stmts map[string]sqliteh.Stmt // persistent statements on db
	id    sqliteh.TraceConnID
}

// NewPool creates a Pool of poolSize database connections.
//
// For each connection, initFn is called to initialize the connection.
// Tracer is used to report statistics about the use of the Pool.
func NewPool(filename string, poolSize int, initFn func(sqliteh.DB) error, tracer sqliteh.Tracer) (_ *Pool, err error) {
	p := &Pool{
		poolSize:    poolSize,
		rwConnFree:  make(chan *conn, 1),
		roConnsFree: make(chan *conn, poolSize-1),
		tracer:      tracer,
		closed:      make(chan struct{}),
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("sqlitepool.NewPool: %w", err)
			select {
			case conn := <-p.rwConnFree:
				conn.db.Close()
			default:
			}
			close(p.roConnsFree)
			for conn := range p.roConnsFree {
				conn.db.Close()
			}
		}
	}()
	if poolSize < 2 {
		return nil, fmt.Errorf("poolSize=%d is too small", poolSize)
	}
	for i := 0; i < poolSize; i++ {
		db, err := cgosqlite.Open(filename, sqliteh.OpenFlagsDefault, "")
		if err != nil {
			return nil, err
		}
		if err := initFn(db); err != nil {
			return nil, err
		}
		c := &conn{
			pool:  p,
			db:    db,
			stmts: make(map[string]sqliteh.Stmt),
			id:    sqliteh.TraceConnID(i),
		}
		if i == 0 {
			p.rwConnFree <- c
		} else {
			if err := ExecScript(c.db, "PRAGMA query_only=true"); err != nil {
				return nil, err
			}
			p.roConnsFree <- c
		}
	}

	return p, nil
}

func (c *conn) close() error {
	if c.db == nil {
		return errors.New("sqlitepool conn already closed")
	}
	for _, stmt := range c.stmts {
		stmt.Finalize()
	}
	c.stmts = nil
	err := c.db.Close()
	c.db = nil
	return err
}

func (p *Pool) Close() error {
	select {
	case <-p.closed:
		return errors.New("pool already closed")
	default:
	}
	close(p.closed)

	c := <-p.rwConnFree
	err := c.close()

	for i := 0; i < p.poolSize-1; i++ {
		c := <-p.roConnsFree
		err2 := c.close()
		if err == nil {
			err = err2
		}
	}
	return err
}

var errPoolClosed = fmt.Errorf("%w: sqlitepool closed", context.Canceled)

// BeginTx creates a writable transaction using BEGIN IMMEDIATE.
// The parameter why is passed to the Tracer for debugging.
func (p *Pool) BeginTx(ctx context.Context, why string) (*Tx, error) {
	select {
	case <-p.closed:
		return nil, errPoolClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn := <-p.rwConnFree:
		tx := &Tx{Rx: &Rx{conn: conn, inTx: true}}
		err := tx.Exec("BEGIN IMMEDIATE;")
		if p.tracer != nil {
			p.tracer.BeginTx(ctx, conn.id, why, false, err)
		}
		if err != nil {
			p.rwConnFree <- conn // can't block, buffer is big enough
			return nil, err
		}
		return tx, nil
	}
}

// BeginRx creates a read-only transaction.
// The parameter why is passed to the Tracer for debugging.
func (p *Pool) BeginRx(ctx context.Context, why string) (*Rx, error) {
	select {
	case <-p.closed:
		return nil, errPoolClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn := <-p.roConnsFree:
		rx := &Rx{conn: conn}
		err := rx.Exec("BEGIN;")
		if p.tracer != nil {
			p.tracer.BeginTx(ctx, conn.id, why, true, err)
		}
		if err != nil {
			p.roConnsFree <- conn // can't block, buffer is big enough
			return nil, err
		}
		return &Rx{conn: conn}, nil
	}
}

// Rx is a read-only transaction.
//
// It is *not* safe for concurrent use.
type Rx struct {
	conn *conn
	inTx bool // true if this Rx is embedded in a writable Tx

	// OnRollback is an optional function called after rollback.
	// If Rx is part of a Tx and it is committed, then OnRollback
	// is not called.
	OnRollback func()
}

// Exec executes an SQL statement with no result.
func (rx *Rx) Exec(sql string) error {
	_, _, _, _, err := rx.Prepare(sql).StepResult()
	if err != nil {
		return fmt.Errorf("%w: %v", err, rx.conn.db.ErrMsg())
	}
	return nil
}

// Prepare prepares an SQL statement.
// The Stmt is cached on the connection, so subsequent calls are fast.
func (rx *Rx) Prepare(sql string) sqliteh.Stmt {
	stmt := rx.conn.stmts[sql]
	if stmt != nil {
		return stmt
	}
	stmt, _, err := rx.conn.db.Prepare(sql, sqliteh.SQLITE_PREPARE_PERSISTENT)
	if err != nil {
		// Persistent statements are constant strings hardcoded into
		// programs. Failing to prepare one means the string is bad.
		// Ideally we would detect this at compile time, but barring
		// that, there is no point returning the error because this
		// is not something the program can recover from or handle.
		panic(fmt.Sprintf("%v: %v", err, rx.conn.db.ErrMsg()))
	}
	rx.conn.stmts[sql] = stmt
	return stmt
}

// DB returns the underlying database connection.
//
// Be careful: a transaction is in progress. Any use of BEGIN/COMMIT/ROLLBACK
// should be modelled as a nested transaction, and when done the original
// outer transaction should be left in-progress.
func (rx *Rx) DB() sqliteh.DB {
	return rx.conn.db
}

// ExecScript executes a series of SQL statements against a database connection.
// It is intended for one-off scripts, so the prepared Stmt objects are not
// cached for future calls.
func ExecScript(db sqliteh.DB, queries string) error {
	for {
		queries = strings.TrimSpace(queries)
		if queries == "" {
			return nil
		}
		stmt, rem, err := db.Prepare(queries, 0)
		if err != nil {
			return fmt.Errorf("ExecScript: %w: %v, in remaining script: %s", err, db.ErrMsg(), queries)
		}
		queries = rem
		_, err = stmt.Step(nil)
		if err != nil {
			err = fmt.Errorf("ExecScript: %w: %s: %v", err, stmt.SQL(), db.ErrMsg())
		}
		stmt.Finalize()
		if err != nil {
			return err
		}
	}
}

// Rollback executes ROLLBACK and cleans up the Rx.
// It is a no-op if Rx is already rolled back.
func (rx *Rx) Rollback() {
	if rx.conn == nil {
		return
	}
	if rx.inTx {
		panic("Tx.Rx.Rollback called, only call Rollback on the Tx object")
	}
	err := rx.Exec("ROLLBACK;")
	if rx.conn.pool.tracer != nil {
		rx.conn.pool.tracer.Rollback(rx.conn.id, err)
	}
	rx.conn.pool.roConnsFree <- rx.conn
	rx.conn = nil
	if rx.OnRollback != nil {
		rx.OnRollback()
		rx.OnRollback = nil
	}
	if err != nil {
		panic(err)
	}
}

// Tx is a writable SQLite database transaction.
//
// It is *not* safe for concurrent use.
//
// A Tx contains an embedded Rx, which can be used to pass to functions
// that want to perform read-only queries on the writable Tx.
type Tx struct {
	*Rx

	// OnCommit is an optional function called after successful commit.
	OnCommit func()
}

// Rollback executes ROLLBACK and cleans up the Tx.
// It is a no-op if the Tx is already rolled back or committed.
func (tx *Tx) Rollback() {
	if tx.conn == nil {
		return
	}
	err := tx.Exec("ROLLBACK;")
	if tx.conn.pool.tracer != nil {
		tx.conn.pool.tracer.Rollback(tx.conn.id, err)
	}
	tx.conn.pool.rwConnFree <- tx.conn
	tx.conn = nil
	if tx.OnRollback != nil {
		tx.OnRollback()
		tx.OnRollback = nil
		tx.OnCommit = nil
	}
	if err != nil {
		panic(err)
	}
}

// Commit executes COMMIT and cleans up the Tx.
// It is an error to call if the Tx is already rolled back or committed.
func (tx *Tx) Commit() error {
	if tx.conn == nil {
		return errors.New("tx already done")
	}
	err := tx.Exec("COMMIT;")
	if tx.conn.pool.tracer != nil {
		tx.conn.pool.tracer.Commit(tx.conn.id, err)
	}
	tx.conn.pool.rwConnFree <- tx.conn
	tx.conn = nil
	if tx.OnCommit != nil {
		tx.OnCommit()
		tx.OnCommit = nil
		tx.OnRollback = nil
	}
	return err
}
