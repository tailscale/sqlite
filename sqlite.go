// Copyright (c) 2021 Tailscale Inc & AUTHORS All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite implements a database/sql driver for SQLite3.
//
// This driver requires a file: URI always be used to open a database.
// For details see https://sqlite.org/c3ref/open.html#urifilenames.
//
// # Initializing connections or tracing
//
// If you want to do initial configuration of a connection, or enable
// tracing, use the Connector function:
//
//	connInitFunc := func(ctx context.Context, conn driver.ConnPrepareContext) error {
//		return sqlite.ExecScript(conn.(sqlite.SQLConn), "PRAGMA journal_mode=WAL;")
//	}
//	db, err = sql.OpenDB(sqlite.Connector(sqliteURI, connInitFunc, nil))
//
// # Memory Mode
//
// In-memory databases are popular for tests.
// Use the "memdb" VFS (*not* the legacy in-memory modes) to be compatible
// with the database/sql connection pool:
//
//	file:/dbname?vfs=memdb
//
// Use a different dbname for each memory database opened.
//
// # Binding Types
//
// SQLite is flexible about type conversions, and so is this driver.
// Almost all "basic" Go types (int, float64, string) are accepted and
// directly mapped into SQLite, even if they are named Go types.
// The time.Time type is also accepted (described below).
// Values that implement encoding.TextMarshaler or json.Marshaler are
// stored in SQLite in their marshaled form.
//
// # Binding Time
//
// While SQLite3 has no strict time datatype, it does have a series of built-in
// functions that operate on timestamps that expect columns to be in one of many
// formats: https://sqlite.org/lang_datefunc.html
//
// When encoding a time.Time into one of SQLite's preferred formats, we use the
// shortest timestamp format that can accurately represent the time.Time.
// The supported formats are:
//
//  2. YYYY-MM-DD HH:MM
//  3. YYYY-MM-DD HH:MM:SS
//  4. YYYY-MM-DD HH:MM:SS.SSS
//
// If the time.Time is not UTC (strongly consider storing times in UTC!),
// we follow SQLite's norm of appending "[+-]HH:MM" to the above formats.
//
// It is common in SQLite to store "Unix time", seconds-since-epoch in an
// INTEGER column. This is understood by the date and time functions documented
// in the link above. If you want to do that, pass the result of time.Time.Unix
// to the driver.
//
// # Reading Time
//
// In general, time is hard to extract from SQLite as a time.Time.
// If a column is defined as DATE or DATETIME, then text data is parsed
// as TimeFormat and returned as a time.Time. Integer data is parsed as
// seconds since epoch and returned as a time.Time.
package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding"
	"errors"
	"expvar"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tailscale/sqlite/sqliteh"
)

var Open sqliteh.OpenFunc = func(string, sqliteh.OpenFlags, string) (sqliteh.DB, error) {
	return nil, fmt.Errorf("cgosqlite.Open is missing")
}

// ConnInitFunc is a function called by the driver on new connections.
//
// The conn can be used to execute queries, and implements SQLConn.
// Any error return closes the conn and passes the error to database/sql.
type ConnInitFunc func(ctx context.Context, conn driver.ConnPrepareContext) error

// TimeFormat is the string format this driver uses to store
// microsecond-precision time in SQLite in text format.
const TimeFormat = "2006-01-02 15:04:05.000-0700"

func init() {
	sql.Register("sqlite3", drv{})
}

var maxConnID atomic.Int32

// UsesAfterClose is a metric that is incremented every time an operation is
// attempted on a connection after Close has already been called. The keys are
// internal identifiers for the code path that incremented a counter.
var UsesAfterClose expvar.Map

// ErrClosed is returned when an operation is attempted on a connection after
// Close has already been called.
var ErrClosed = errors.New("sqlite3: already closed")

type drv struct{}

func (drv) Open(name string) (driver.Conn, error) { panic("deprecated, unused") }
func (drv) OpenConnector(name string) (driver.Connector, error) {
	return &connector{name: name, openFlags: sqliteh.OpenFlagsDefault}, nil
}

// Connector returns a [driver.Connector] for the given connection
// parameters.
//
// The tracer may be nil.
func Connector(sqliteURI string, connInitFunc ConnInitFunc, tracer sqliteh.Tracer) driver.Connector {
	return ConnectorWithOpts(sqliteURI, connInitFunc, WithTracer(tracer))
}

// ConnectorWithLogger returns a [driver.Connector] for the given connection
// parameters. makeLogger, if non-nil, is used to create a [ConnLogger] when [Connect] is
// called.
//
// The tracer may also be nil.
func ConnectorWithLogger(sqliteURI string, connInitFunc ConnInitFunc, tracer sqliteh.Tracer, makeLogger func() ConnLogger) driver.Connector {
	return ConnectorWithOpts(sqliteURI, connInitFunc, WithTracer(tracer), WithConnLogger(makeLogger))
}

// ConnectorWithOpts returns a [driver.Connector] for the given connection parameters, optionally
// configured with one or more [ConnectorOpt]s.
func ConnectorWithOpts(sqliteURI string, connInitFunc ConnInitFunc, opts ...ConnectorOpt) driver.Connector {
	p := &connector{
		name:         sqliteURI,
		connInitFunc: connInitFunc,
		openFlags:    sqliteh.OpenFlagsDefault, // default flags unless [WithOpenFlags] option is used
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// ConnectorOpt is an option to [ConnectorWithOpts].
type ConnectorOpt func(p *connector)

// WithTracer returns a [ConnectorOpt] that configures the [driver.Connector]
// to enable tracing on new connections using the given [sqliteh.Tracer].
func WithTracer(tracer sqliteh.Tracer) ConnectorOpt {
	return func(p *connector) {
		p.tracer = tracer
	}
}

// WithConnLogger returns a [ConnectorOpt] that configures the [driver.Connector]
// to use a [ConnLogger] returned by the provided makeLogger when opening new
// connections.
func WithConnLogger(makeLogger func() ConnLogger) ConnectorOpt {
	return func(p *connector) {
		p.makeLogger = makeLogger
	}
}

// WithOpenFlags returns a [ConnectorOpt] that configures the [driver.Connector]
// to use the given [sqliteh.OpenFlags] when opening new connections.
func WithOpenFlags(openFlags sqliteh.OpenFlags) ConnectorOpt {
	return func(p *connector) {
		p.openFlags = openFlags
	}
}

type connector struct {
	name         string
	tracer       sqliteh.Tracer    // or nil
	makeLogger   func() ConnLogger // or nil
	connInitFunc ConnInitFunc
	openFlags    sqliteh.OpenFlags
}

// Driver implements [driver.Connector.Driver].
func (p *connector) Driver() driver.Driver { return drv{} }

// Connect implements [driver.Connector.Connect]
func (p *connector) Connect(ctx context.Context) (driver.Conn, error) {
	db, err := Open(p.name, p.openFlags, "")
	if err != nil {
		if ec, ok := err.(sqliteh.ErrCode); ok {
			e := &Error{
				Code: sqliteh.Code(ec),
				Loc:  "Open",
			}
			if db != nil {
				e.Msg = db.ErrMsg()
			}
			err = e
		}
		if db != nil {
			db.Close()
		}
		return nil, err
	}
	c := &conn{
		db:     db,
		tracer: p.tracer,
		id:     sqliteh.TraceConnID(maxConnID.Add(1)),
	}
	if p.makeLogger != nil {
		c.logger = p.makeLogger()
	}
	if p.connInitFunc != nil {
		if err := p.connInitFunc(ctx, c); err != nil {
			db.Close()
			return nil, fmt.Errorf("sqlite.ConnInitFunc: %w", err)
		}
	}
	return c, nil
}

type txState int

const (
	txStateNone  = txState(0) // connection is not connected to a Tx
	txStateInit  = txState(1) // BeginTx called, but "BEGIN;" not yet executed
	txStateBegun = txState(2) // "BEGIN;" has been executed
)

type conn struct {
	db       sqliteh.DB
	id       sqliteh.TraceConnID
	tracer   sqliteh.Tracer // or nil if unused
	logger   ConnLogger
	stmts    map[string]*stmt // persisted statements
	txState  txState
	readOnly bool
	closed   atomic.Bool
}

func (c *conn) Prepare(query string) (driver.Stmt, error) { panic("deprecated, unused") }
func (c *conn) Begin() (driver.Tx, error)                 { panic("deprecated, unused") }
func (c *conn) Close() error {
	// Don't double-close
	if !c.closed.CompareAndSwap(false, true) {
		UsesAfterClose.Add("Close", 1)
		return nil
	}

	for q, s := range c.stmts {
		s.stmt.Finalize()
		s.closed.Store(true)
		delete(c.stmts, q)
	}
	err := reserr(c.db, "Conn.Close", "", c.db.Close())
	if c.tracer != nil {
		c.tracer.Close(c.id, err)
	}
	return err
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	persist := ctx.Value(persistQuery{}) != nil
	return c.prepare(ctx, query, persist)
}

func (c *conn) prepare(ctx context.Context, query string, persist bool) (s *stmt, err error) {
	if c.closed.Load() {
		UsesAfterClose.Add("prepare", 1)
		return nil, ErrClosed
	}

	query = strings.TrimSpace(query)
	if s := c.stmts[query]; s != nil {
		// don't hand the same statement out twice; this is re-added on s.Close
		delete(c.stmts, query)

		s.prepCtx = ctx
		if !s.closed.CompareAndSwap(true, false) {
			// We'd previously set this to 'false', indicating that
			// this stmt is in-use. Return an error instead of
			// reusing the stmt.
			return nil, ErrClosed
		}

		return s, nil
	}
	if c.tracer != nil {
		// Not a hot path. Any high-load environment should use
		// WithPersist so this is rare.
		start := time.Now()
		defer func() {
			if err != nil {
				c.tracer.Query(ctx, c.id, query, time.Since(start), err)
			}
		}()
	}
	var flags sqliteh.PrepareFlags
	if persist {
		flags = sqliteh.SQLITE_PREPARE_PERSISTENT
	}
	cstmt, rem, err := c.db.Prepare(query, flags)
	if err != nil {
		return nil, reserr(c.db, "Prepare", query, err)
	}
	if rem != "" {
		cstmt.Finalize()
		return nil, &Error{
			Code:  sqliteh.SQLITE_MISUSE,
			Loc:   "Prepare",
			Query: query,
			Msg:   fmt.Sprintf("query has trailing text: %q", rem),
		}
	}
	s = &stmt{
		conn:     c,
		stmt:     cstmt,
		query:    query,
		persist:  persist,
		numInput: -1,
		prepCtx:  ctx,
	}

	if !persist {
		return s, nil
	}

	// NOTE: don't add the statement to c.stmts here, since we could return
	// it to another caller before Close is called; it's added to the
	// c.stmts map on Close.
	if c.stmts == nil {
		c.stmts = make(map[string]*stmt)
	}
	return s, nil
}

func (c *conn) execInternal(ctx context.Context, query string) error {
	s, err := c.prepare(ctx, query, true)
	if err != nil {
		if e, _ := err.(*Error); e != nil {
			e.Loc = "internal:" + e.Loc
		}
		return err
	}
	if _, err := s.ExecContext(ctx, nil); err != nil {
		return err
	}
	s.Close()
	return nil
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.closed.Load() {
		UsesAfterClose.Add("BeginTx", 1)
		return nil, ErrClosed
	}

	const LevelSerializable = 6 // matches the sql package constant
	if opts.Isolation != 0 && opts.Isolation != LevelSerializable {
		return nil, errors.New("github.com/tailscale/sqlite driver only supports serializable isolation level")
	}
	c.readOnly = opts.ReadOnly
	c.txState = txStateInit
	if c.tracer != nil {
		c.tracer.BeginTx(ctx, c.id, "", c.readOnly, nil)
	}
	if err := c.txInit(ctx); err != nil {
		return nil, err
	}
	return &connTx{conn: c}, nil
}

// Raw is so ConnInitFunc can cast to SQLConn.
func (c *conn) Raw(fn func(any) error) error { return fn(c) }

type readOnlyKey struct{}

// ReadOnly applies the query_only pragma to the connection.
func ReadOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, readOnlyKey{}, true)
}

// IsReadOnly reports whether the context has the ReadOnly key.
func IsReadOnly(ctx context.Context) bool {
	return ctx.Value(readOnlyKey{}) != nil
}

func (c *conn) txInit(ctx context.Context) error {
	if c.txState != txStateInit {
		return nil
	}
	c.txState = txStateBegun
	if c.readOnly || IsReadOnly(ctx) {
		if err := c.execInternal(ctx, "BEGIN"); err != nil {
			return err
		}
		if err := c.execInternal(ctx, "PRAGMA query_only=true"); err != nil {
			return err
		}
	} else {
		if c.logger != nil {
			c.logger.Begin()
		}
		// TODO(crawshaw): offer BEGIN DEFERRED (and BEGIN CONCURRENT?)
		// semantics via a context annotation function.
		if err := c.execInternal(ctx, "BEGIN IMMEDIATE"); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) txEnd(ctx context.Context, endStmt string) error {
	defer func() {
		c.txState = txStateNone
		c.readOnly = false
	}()
	if c.txState != txStateBegun {
		return nil
	}

	err := c.execInternal(context.Background(), endStmt)
	if c.readOnly {
		if err2 := c.execInternal(ctx, "PRAGMA query_only=false"); err == nil {
			err = err2
		}
	}
	return err
}

type connTx struct {
	conn *conn
}

func (tx *connTx) Commit() error {
	if tx.conn.closed.Load() {
		UsesAfterClose.Add("tx.Commit", 1)
		return ErrClosed
	}

	readonly := tx.conn.readOnly
	err := tx.conn.txEnd(context.Background(), "COMMIT")
	if tx.conn.tracer != nil {
		tx.conn.tracer.Commit(tx.conn.id, err)
	}
	if tx.conn.logger != nil && !readonly {
		tx.conn.logger.Commit(err)
	}
	return err
}

func (tx *connTx) Rollback() error {
	if tx.conn.closed.Load() {
		UsesAfterClose.Add("tx.Rollback", 1)
		return ErrClosed
	}

	readonly := tx.conn.readOnly
	err := tx.conn.txEnd(context.Background(), "ROLLBACK")
	if tx.conn.tracer != nil {
		tx.conn.tracer.Rollback(tx.conn.id, err)
	}
	if tx.conn.logger != nil && !readonly {
		tx.conn.logger.Rollback()
	}
	return err
}

func reserr(db sqliteh.DB, loc, query string, err error) error {
	if err == nil {
		return nil
	}
	e := &Error{
		Code:  sqliteh.Code(err.(sqliteh.ErrCode)),
		Loc:   loc,
		Query: query,
	}
	// TODO(crawshaw): consider an API to expose this. sqlite.DebugErrMsg(db)?
	if true {
		e.Msg = db.ErrMsg()
	}
	return e
}

type stmt struct {
	conn    *conn
	stmt    sqliteh.Stmt
	query   string
	persist bool        // true if stmt is cached and lives beyond Close
	bound   bool        // true if stmt has parameters bound
	closed  atomic.Bool // true after Close if persist==false

	numInput int // filled on first NumInput only if persist==true

	prepCtx context.Context // the context provided to prepare, for tracing

	// filled on first step only if persist==true
	colDeclTypes []colDeclType
	colNames     []string
}

func (s *stmt) reserr(loc string, err error) error { return reserr(s.conn.db, loc, s.query, err) }

func (s *stmt) NumInput() int {
	if s.closed.Load() {
		UsesAfterClose.Add("stmt.NumInput", 1)
		return 0
	}
	if s.persist {
		if s.numInput == -1 {
			s.numInput = s.stmt.BindParameterCount()
		}
		return s.numInput
	}
	return s.stmt.BindParameterCount()
}

func (s *stmt) Close() error {
	// Always set the 'closed' boolean, even for a persisted query; this is
	// set from false -> true in prepare(), above.
	if s.conn.closed.Load() {
		UsesAfterClose.Add("Stmt.Close_conn", 1)
		return nil
	}
	if !s.closed.CompareAndSwap(false, true) {
		UsesAfterClose.Add("Stmt.Close", 1)
		return nil
	}

	// We return this statement to the conn only if it's persistent, and
	// only if there's not already a statement with the same query already
	// cached there.
	shouldPersist := s.persist
	if shouldPersist {
		if _, alreadyPersisted := s.conn.stmts[s.query]; alreadyPersisted {
			shouldPersist = false
		}
	}
	if shouldPersist {
		err := s.reserr("Stmt.Close", s.resetAndClear())
		if err == nil {
			s.conn.stmts[s.query] = s
		}
		return err
	}
	return s.reserr("Stmt.Close", s.stmt.Finalize())
}
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) { panic("deprecated, unused") }
func (s *stmt) Query(args []driver.Value) (driver.Rows, error)  { panic("deprecated, unused") }

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed.Load() {
		UsesAfterClose.Add("stmt.ExecContext", 1)
		return nil, ErrClosed
	}
	if err := s.resetAndClear(); err != nil {
		return nil, s.reserr("Stmt.Exec(Reset)", err)
	}
	if err := s.bindAll(args); err != nil {
		return nil, s.reserr("Stmt.Exec(Bind)", err)
	}
	if s.conn.logger != nil && !s.conn.readOnly {
		s.conn.logger.Statement(s.stmt.ExpandedSQL())
	}

	if ctx.Value(queryCancelKey{}) != nil {
		done := make(chan struct{})
		pctx, pcancel := context.WithCancel(ctx)
		db := s.stmt.DBHandle()
		context.AfterFunc(pctx, func() {
			defer close(done)

			// Note: We respond to cancellation on the primary context (ctx) not
			// the cleanup context (pctx).
			if ctx.Err() != nil {
				if s.closed.Load() {
					UsesAfterClose.Add("stmt.ExecContext.Interrupt", 1)
				} else {
					db.Interrupt()
				}
			}
		})

		// We must wait prior to returning to ensure a cancellation context (if
		// present) has completed, so that a cancellation cannot outlast this
		// request and fire during a later execution.
		defer func() { pcancel(); <-done }()
	}

	row, lastInsertRowID, changes, duration, err := s.stmt.StepResult()
	s.bound = false // StepResult resets the query
	err = s.reserr("Stmt.Exec", err)
	if s.conn.tracer != nil {
		s.conn.tracer.Query(s.prepCtx, s.conn.id, s.query, duration, err)
	}
	if err != nil {
		return nil, err
	}
	_ = row // TODO: return error if exec on query which returns rows?
	return getStmtResult(lastInsertRowID, changes), nil
}

var (
	stmtResultZeroRows = &stmtResult{}
	stmtResultOneRow   = &stmtResult{rowsAffected: 1}
)

func getStmtResult(lastInsertID int64, rowsAffected int64) *stmtResult {
	// Some common cases to avoid allocs:
	if lastInsertID == 0 {
		switch rowsAffected {
		case 0:
			return stmtResultZeroRows
		case 1:
			return stmtResultOneRow
		}
	}
	return &stmtResult{lastInsertID: lastInsertID, rowsAffected: rowsAffected}
}

type stmtResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (res *stmtResult) LastInsertId() (int64, error) { return res.lastInsertID, nil }
func (res *stmtResult) RowsAffected() (int64, error) { return res.rowsAffected, nil }

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed.Load() {
		UsesAfterClose.Add("stmt.QueryContext", 1)
		return nil, ErrClosed
	}
	if err := s.resetAndClear(); err != nil {
		return nil, s.reserr("Stmt.Query(Reset)", err)
	}
	if err := s.bindAll(args); err != nil {
		return nil, err
	}
	cancel := func() {}
	var done chan struct{}
	if ctx.Value(queryCancelKey{}) != nil {
		done = make(chan struct{})
		pctx, pcancel := context.WithCancel(ctx)
		cancel = pcancel
		db := s.stmt.DBHandle()
		context.AfterFunc(pctx, func() {
			defer close(done)

			// Note: We respond to cancellation on the primary context (ctx) not
			// the cleanup context (pctx).
			if ctx.Err() != nil {
				if s.closed.Load() {
					UsesAfterClose.Add("stmt.QueryContext.Interrupt", 1)
				} else {
					db.Interrupt()
				}
			}
		})
		// In this case we do not have an early exit, so we don't need to
		// dissociate the cancellation handler: If the caller gets an error, it
		// will explicitly trigger the cancellation and wait in (*rows).Close.
	}
	return &rows{stmt: s, cancel: cancel, done: done}, nil
}

func (s *stmt) resetAndClear() error {
	if !s.bound {
		return nil
	}
	s.bound = false
	duration, err := s.stmt.ResetAndClear()
	if s.conn.tracer != nil {
		s.conn.tracer.Query(s.prepCtx, s.conn.id, s.query, duration, err)
	}
	return err
}

func (s *stmt) bindAll(args []driver.NamedValue) error {
	if s.bound {
		panic("sqlite: impossible state, query already running: " + s.query)
	}
	s.bound = true
	if s.conn.tracer != nil {
		s.stmt.StartTimer()
	}
	for _, arg := range args {
		if err := s.bind(arg); err != nil {
			return err
		}
	}
	return nil
}

func (s *stmt) bind(arg driver.NamedValue) error {
	// TODO(crawshaw): could use a union-ish data type for debugName
	// to avoid the allocation.
	var debugName any
	if arg.Name == "" {
		debugName = arg.Ordinal
	} else {
		debugName = arg.Name
		index := s.stmt.BindParameterIndexSearch(arg.Name)
		if index == 0 {
			return &Error{
				Code:  sqliteh.SQLITE_MISUSE,
				Loc:   "Bind",
				Query: s.query,
				Msg:   fmt.Sprintf("unknown parameter name %q", arg.Name),
			}
		}
		arg.Ordinal = index
	}

	// Start with obvious types, including time.Time before TextMarshaler.
	found, err := s.bindBasic(debugName, arg.Ordinal, arg.Value)
	if err != nil {
		return err
	} else if found {
		return nil
	}

	if m, _ := arg.Value.(encoding.TextMarshaler); m != nil {
		b, err := m.MarshalText()
		if err != nil {
			// TODO: modify Error to carry an error so we can %w?
			return &Error{
				Code:  sqliteh.SQLITE_MISUSE,
				Loc:   "Bind",
				Query: s.query,
				Msg:   fmt.Sprintf("Bind:%v: cannot marshal %T: %v", debugName, arg.Value, err),
			}
		}
		_, err = s.bindBasic(debugName, arg.Ordinal, b)
		return err
	}

	// Look for named basic types or other convertible types.
	val := reflect.ValueOf(arg.Value)
	typ := reflect.TypeOf(arg.Value)
	switch typ.Kind() {
	case reflect.Bool:
		b := int64(0)
		if val.Bool() {
			b = 1
		}
		_, err := s.bindBasic(debugName, arg.Ordinal, b)
		return err
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		_, err := s.bindBasic(debugName, arg.Ordinal, val.Int())
		return err
	case reflect.Uint, reflect.Uint64:
		return &Error{
			Code:  sqliteh.SQLITE_MISUSE,
			Loc:   "Bind",
			Query: s.query,
			Msg:   fmt.Sprintf("Bind:%v: sqlite does not support uint64 (try a string or TextMarshaler)", debugName),
		}
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		_, err := s.bindBasic(debugName, arg.Ordinal, int64(val.Uint()))
		return err
	case reflect.Float32, reflect.Float64:
		_, err := s.bindBasic(debugName, arg.Ordinal, val.Float())
		return err
	case reflect.String:
		// TODO(crawshaw): decompose bindBasic somehow.
		// But first: more tests that the errors make sense for each type.
		_, err := s.bindBasic(debugName, arg.Ordinal, val.String())
		return err
	}

	return &Error{
		Code:  sqliteh.SQLITE_MISUSE,
		Loc:   "Bind",
		Query: s.query,
		Msg:   fmt.Sprintf("Bind:%v: unknown value type %T (try a string or TextMarshaler)", debugName, arg.Value),
	}
}

func (s *stmt) bindBasic(debugName any, ordinal int, v any) (found bool, err error) {
	defer func() {
		if err != nil {
			err = s.reserr(fmt.Sprintf("Bind:%v:%T", debugName, v), err)
		}
	}()
	switch v := v.(type) {
	case nil:
		return true, s.stmt.BindNull(ordinal)
	case string:
		return true, s.stmt.BindText64(ordinal, v)
	case int:
		return true, s.stmt.BindInt64(ordinal, int64(v))
	case int64:
		return true, s.stmt.BindInt64(ordinal, v)
	case float64:
		return true, s.stmt.BindDouble(ordinal, v)
	case []byte:
		if len(v) == 0 {
			return true, s.stmt.BindZeroBlob64(ordinal, 0)
		} else {
			return true, s.stmt.BindBlob64(ordinal, v)
		}
	case time.Time:
		// Shortest of:
		//	YYYY-MM-DD HH:MM
		// 	YYYY-MM-DD HH:MM:SS
		//	YYYY-MM-DD HH:MM:SS.SSS
		str := v.Format(TimeFormat)
		str = strings.TrimSuffix(str, "-0000")
		str = strings.TrimSuffix(str, ".000")
		str = strings.TrimSuffix(str, ":00")
		return true, s.stmt.BindText64(ordinal, str)
	default:
		return false, nil
	}
}

// colDeclType is whether and how the declared SQLite column type should
// map to any special handling (as a date, or as a boolean, etc).
type colDeclType byte

const (
	declTypeUnknown colDeclType = iota
	declTypeDateOrTime
	declTypeBoolean
)

func colDeclTypeFromString(s string) colDeclType {
	if strings.EqualFold(s, "DATETIME") || strings.EqualFold(s, "DATE") {
		return declTypeDateOrTime
	}
	if strings.EqualFold(s, "BOOLEAN") {
		return declTypeBoolean
	}
	return declTypeUnknown
}

type rows struct {
	stmt   *stmt
	closed bool
	cancel context.CancelFunc // call when query ends
	done   chan struct{}      // either nil, or closed when cancellation is done

	// colType is the column types for Step to fill on each row. We only use 23
	// as it packs well with the closed bool byte above (24 bytes total, same as
	// a slice) and it's uncommon for queries to select so many columns. But if
	// they do, we still work: we just query the column type via cgo on each
	// row. So a bit slower, but fine.
	colType [23]sqliteh.ColumnType

	colNames []string // filled on call to Columns

	// Filled on first call to Next.
	colDeclTypes []colDeclType
}

func (r *rows) Columns() []string {
	if r.closed {
		panic("Columns called after Rows was closed")
	}
	if r.stmt.closed.Load() {
		UsesAfterClose.Add("rows.Columns", 1)
		return nil
	}
	if r.colNames == nil {
		if r.stmt.colNames != nil {
			r.colNames = r.stmt.colNames
		} else {
			r.colNames = make([]string, r.stmt.stmt.ColumnCount())
			for i := range r.colNames {
				r.colNames[i] = r.stmt.stmt.ColumnName(i)
			}
			if r.stmt.persist {
				r.stmt.colNames = r.colNames
			}
		}
	}
	return append([]string{}, r.colNames...)
}

func (r *rows) Close() error {
	if r.closed {
		return errors.New("sqlite rows result already closed")
	}
	if r.stmt.closed.Load() {
		UsesAfterClose.Add("rows.Close", 1)
		return ErrClosed
	}
	r.closed = true
	r.cancel()
	if r.done != nil {
		<-r.done
	}
	if err := r.stmt.resetAndClear(); err != nil {
		return r.stmt.reserr("Rows.Close(Reset)", err)
	}
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.closed {
		return errors.New("sqlite rows result already closed")
	}
	if r.stmt.closed.Load() {
		UsesAfterClose.Add("rows.Next", 1)
		return ErrClosed
	}
	hasRow, err := r.stmt.stmt.Step(r.colType[:])
	if err != nil {
		return r.stmt.reserr("Rows.Next", err)
	}
	if !hasRow {
		return io.EOF
	}

	if r.colDeclTypes == nil {
		r.colDeclTypes = r.stmt.colDeclTypes
	}
	if r.colDeclTypes == nil {
		colCount := r.stmt.stmt.ColumnCount()
		r.colDeclTypes = make([]colDeclType, colCount)
		for i := range r.colDeclTypes {
			r.colDeclTypes[i] = colDeclTypeFromString(r.stmt.stmt.ColumnDeclType(i))
		}
		if r.stmt.persist {
			r.stmt.colDeclTypes = r.colDeclTypes
		}
	}

	for i := range dest {
		var colType sqliteh.ColumnType
		if i < len(r.colType) {
			// Common case, for the first couple dozen columns.
			colType = r.colType[i]
		} else {
			// If it's a really wide query, then call into
			// cgo for columns past the length of
			// r.colType.
			colType = r.stmt.stmt.ColumnType(i)
		}

		if r.colDeclTypes[i] == declTypeDateOrTime {
			switch colType {
			case sqliteh.SQLITE_INTEGER:
				v := r.stmt.stmt.ColumnInt64(i)
				dest[i] = time.Unix(v, 0)
			case sqliteh.SQLITE_FLOAT:
				dest[i] = r.stmt.stmt.ColumnDouble(i)
				// TODO: treat as time?
			case sqliteh.SQLITE_TEXT:
				v := r.stmt.stmt.ColumnText(i)
				format := TimeFormat
				if len(format) > len(v) {
					format = strings.TrimSuffix(format, "-0700")
				}
				if len(format) > len(v) {
					format = strings.TrimSuffix(format, ".000")
				}
				if len(format) > len(v) {
					format = strings.TrimSuffix(format, ":05")
				}
				t, err := time.Parse(format, v)
				if err != nil {
					return fmt.Errorf("cannot parse time from column %d: %v", i, err)
				}
				dest[i] = t
			}
			continue
		}
		switch colType {
		case sqliteh.SQLITE_INTEGER:
			val := r.stmt.stmt.ColumnInt64(i)
			if r.colDeclTypes[i] == declTypeBoolean {
				dest[i] = val > 0
			} else {
				dest[i] = val
			}
		case sqliteh.SQLITE_FLOAT:
			dest[i] = r.stmt.stmt.ColumnDouble(i)
		case sqliteh.SQLITE_BLOB, sqliteh.SQLITE_TEXT:
			dest[i] = r.stmt.stmt.ColumnBlob(i)
		case sqliteh.SQLITE_NULL:
			dest[i] = nil
		}
	}
	return nil
}

// Error is an error produced by SQLite.
type Error struct {
	Code  sqliteh.Code // SQLite extended error code (SQLITE_OK is an invalid value)
	Loc   string       // method name that generated the error
	Query string       // original SQL query text
	Msg   string       // value of sqlite3_errmsg, set sqlite.ErrMsg = true
}

func (err Error) Error() string {
	b := new(strings.Builder)
	b.WriteString("sqlite")
	if err.Loc != "" {
		b.WriteByte('.')
		b.WriteString(err.Loc)
	}
	b.WriteString(": ")
	b.WriteString(err.Code.String())
	if err.Msg != "" {
		b.WriteString(": ")
		b.WriteString(err.Msg)
	}
	if err.Query != "" {
		b.WriteString(" (")
		b.WriteString(err.Query)
		b.WriteByte(')')
	}
	return b.String()
}

// SQLConn is a database/sql.Conn.
// (We cannot create a circular package dependency here.)
type SQLConn interface {
	Raw(func(driverConn any) error) error
}

// ExecScript executes a set of SQL queries on an sql.Conn.
// It stops on the first error.
// It is recommended you wrap your script in a BEGIN; ... COMMIT; block.
//
// Usage:
//
//	c, err := db.Conn(ctx)
//	if err != nil {
//		// handle err
//	}
//	if err := sqlite.ExecScript(c, queries); err != nil {
//		// handle err
//	}
//	c.Close() // return sql.Conn to pool
func ExecScript(sqlconn SQLConn, queries string) error {
	return sqlconn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("sqlite.ExecScript: sql.Conn is not the sqlite driver: %T", driverConn)
		}

		for {
			queries = strings.TrimSpace(queries)
			if queries == "" {
				return nil
			}
			cstmt, rem, err := c.db.Prepare(queries, 0)
			if err != nil {
				return reserr(c.db, "ExecScript", queries, err)
			}
			queries = rem
			_, err = cstmt.Step(nil)
			cstmt.Finalize()
			if err != nil {
				// TODO(crawshaw): consider checking sqlite3_txn_state
				// here and issuing a rollback, incase this script was:
				//	BEGIN; BAD-SQL; COMMIT;
				// So we don't leave the connection open.
				return reserr(c.db, "ExecScript", queries, err)
			}
		}
	})
}

// BusyTimeout calls sqlite3_busy_timeout on the underlying connection.
func BusyTimeout(sqlconn SQLConn, d time.Duration) error {
	return sqlconn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("sqlite.BusyTimeout: sql.Conn is not the sqlite driver: %T", driverConn)
		}
		c.db.BusyTimeout(d)
		return nil
	})
}

// SetWALHook calls sqlite3_wal_hook.
//
// If hook is nil, the hook is removed.
func SetWALHook(sqlconn SQLConn, hook func(dbName string, pages int)) error {
	return sqlconn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("sqlite.TxnState: sql.Conn is not the sqlite driver: %T", driverConn)
		}
		c.db.SetWALHook(hook)
		return nil
	})
}

// TxnState calls sqlite3_txn_state on the underlying connection.
func TxnState(sqlconn SQLConn, schema string) (state sqliteh.TxnState, err error) {
	return state, sqlconn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("sqlite.TxnState: sql.Conn is not the sqlite driver: %T", driverConn)
		}
		state = c.db.TxnState(schema)
		return nil
	})
}

// Checkpoint calls sqlite3_wal_checkpoint_v2 on the underlying connection.
func Checkpoint(sqlconn SQLConn, dbName string, mode sqliteh.Checkpoint) (numFrames, numFramesCheckpointed int, err error) {
	err = sqlconn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("sqlite.Checkpoint: sql.Conn is not the sqlite driver: %T", driverConn)
		}
		numFrames, numFramesCheckpointed, err = c.db.Checkpoint(dbName, mode)
		return reserr(c.db, "Checkpoint", dbName, err)
	})
	return numFrames, numFramesCheckpointed, err
}

// DisableFunction disables the named function on the given connection.
// numArgs must match the number of args of the function to be disabled.
func DisableFunction(sqlconn SQLConn, name string, numArgs int) error {
	return sqlconn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("sqlite.DisableFunction: sql.Conn is not the sqlite driver: %T", driverConn)
		}
		return c.db.DisableFunction(name, numArgs)
	})
}

// WithPersist makes a ctx instruct the sqlite driver to persist a prepared query.
//
// This should be used with recurring queries to avoid constant parsing and
// planning of the query by SQLite.
func WithPersist(ctx context.Context) context.Context {
	return context.WithValue(ctx, persistQuery{}, persistQuery{})
}

// persistQuery is used as a context value.
type persistQuery struct{}

// WithQueryCancel makes a ctx that instructs the sqlite driver to explicitly
// interrupt a running query if its argument context ends.  By default, without
// this option, queries will only check the context between steps.
func WithQueryCancel(ctx context.Context) context.Context {
	return context.WithValue(ctx, queryCancelKey{}, queryCancelKey{})
}

// queryCancelKey is a context key for query context enforcement.
type queryCancelKey struct{}

// ConnLogger is implemented by the caller to support statement-level logging
// for write transactions. Only Exec calls are logged, not Query calls, as this
// is intended as a mechanism to replay failed transactions.
//
// Aside from logging only executed statements, ConnLogger also differs from
// [sqliteh.Tracer] by logging the expanded SQL, instead of the query with
// placeholders.
type ConnLogger interface {
	// Begin is called when a writable transaction is opened.
	Begin()

	// Statement is called with evaluated SQL when a statement is executed.
	Statement(sql string)

	// Commit is called after a commit statement, with the error resulting
	// from the attempted commit.
	Commit(error)

	// Rollback is called after a rollback statement.
	Rollback()
}

// LogCallback receives SQLite log messages.
type LogCallback func(code sqliteh.Code, msg string)
