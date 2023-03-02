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

var maxConnID int32 // accessed only via sync/atomic

type drv struct{}

func (d drv) Open(name string) (driver.Conn, error) { panic("deprecated, unused") }
func (d drv) OpenConnector(name string) (driver.Connector, error) {
	return &connector{name: name}, nil
}

func Connector(sqliteURI string, connInitFunc ConnInitFunc, tracer sqliteh.Tracer) driver.Connector {
	return &connector{
		name:         sqliteURI,
		tracer:       tracer,
		connInitFunc: connInitFunc,
	}
}

type connector struct {
	name         string
	tracer       sqliteh.Tracer
	connInitFunc ConnInitFunc
}

func (p *connector) Driver() driver.Driver { return drv{} }
func (p *connector) Connect(ctx context.Context) (driver.Conn, error) {
	db, err := Open(p.name, sqliteh.OpenFlagsDefault, "")
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
		id:     sqliteh.TraceConnID(atomic.AddInt32(&maxConnID, 1)),
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
	tracer   sqliteh.Tracer
	stmts    map[string]*stmt // persisted statements
	txState  txState
	readOnly bool
}

func (c *conn) Prepare(query string) (driver.Stmt, error) { panic("deprecated, unused") }
func (c *conn) Begin() (driver.Tx, error)                 { panic("deprecated, unused") }
func (c *conn) Close() error {
	for q, s := range c.stmts {
		s.stmt.Finalize()
		delete(c.stmts, q)
	}
	return reserr(c.db, "Conn.Close", "", c.db.Close())
}
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	persist := ctx.Value(persistQuery{}) != nil
	return c.prepare(ctx, query, persist)
}

func (c *conn) prepare(ctx context.Context, query string, persist bool) (s *stmt, err error) {
	query = strings.TrimSpace(query)
	if s := c.stmts[query]; s != nil {
		s.prepCtx = ctx
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

	if c.stmts == nil {
		c.stmts = make(map[string]*stmt)
	}
	c.stmts[query] = s
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
func (c *conn) Raw(fn func(interface{}) error) error { return fn(c) }

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
		// TODO(crawshaw): offer BEGIN DEFERRED (and BEGIN CONCURRENT?)
		// semantics via a context annotation function.
		if err := c.execInternal(ctx, "BEGIN IMMEDIATE"); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) txEnd(ctx context.Context, endStmt string) error {
	state, readOnly := c.txState, c.readOnly
	c.txState = txStateNone
	c.readOnly = false
	if state != txStateBegun {
		return nil
	}

	err := c.execInternal(context.Background(), endStmt)
	if readOnly {
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
	err := tx.conn.txEnd(context.Background(), "COMMIT")
	if tx.conn.tracer != nil {
		tx.conn.tracer.Commit(tx.conn.id, err)
	}
	return err
}

func (tx *connTx) Rollback() error {
	err := tx.conn.txEnd(context.Background(), "ROLLBACK")
	if tx.conn.tracer != nil {
		tx.conn.tracer.Rollback(tx.conn.id, err)
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
	persist bool // true if stmt is cached and lives beyond Close
	bound   bool // true if stmt has parameters bound

	numInput int // filled on first NumInput only if persist==true

	prepCtx context.Context // the context provided to prepare, for tracing

	// filled on first step only if persist==true
	colTypes     []sqliteh.ColumnType
	colDeclTypes []string
	colNames     []string
}

func (s *stmt) reserr(loc string, err error) error { return reserr(s.conn.db, loc, s.query, err) }

func (s *stmt) NumInput() int {
	if s.persist {
		if s.numInput == -1 {
			s.numInput = s.stmt.BindParameterCount()
		}
		return s.numInput
	}
	return s.stmt.BindParameterCount()
}

func (s *stmt) Close() error {
	if s.persist {
		return s.reserr("Stmt.Close", s.resetAndClear())
	}
	return s.reserr("Stmt.Close", s.stmt.Finalize())
}
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) { panic("deprecated, unused") }
func (s *stmt) Query(args []driver.Value) (driver.Rows, error)  { panic("deprecated, unused") }

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.resetAndClear(); err != nil {
		return nil, s.reserr("Stmt.Exec(Reset)", err)
	}
	if err := s.bindAll(args); err != nil {
		return nil, s.reserr("Stmt.Exec(Bind)", err)
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
	return stmtResult{lastInsertID: lastInsertRowID, rowsAffected: changes}, nil
}

type stmtResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (res stmtResult) LastInsertId() (int64, error) { return res.lastInsertID, nil }
func (res stmtResult) RowsAffected() (int64, error) { return res.rowsAffected, nil }

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.resetAndClear(); err != nil {
		return nil, s.reserr("Stmt.Query(Reset)", err)
	}
	if err := s.bindAll(args); err != nil {
		return nil, err
	}
	return &rows{stmt: s}, nil
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
	var debugName interface{}
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

func (s *stmt) bindBasic(debugName interface{}, ordinal int, v interface{}) (found bool, err error) {
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

type rows struct {
	stmt   *stmt
	closed bool

	colNames []string // filled on call to Columns

	// Filled on first call to Next.
	colTypes     []sqliteh.ColumnType
	colDeclTypes []string
}

func (r *rows) Columns() []string {
	if r.closed {
		panic("Columns called after Rows was closed")
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
	r.closed = true
	if err := r.stmt.resetAndClear(); err != nil {
		return r.stmt.reserr("Rows.Close(Reset)", err)
	}
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.closed {
		return errors.New("sqlite rows result already closed")
	}
	hasRow, err := r.stmt.stmt.Step()
	if err != nil {
		return r.stmt.reserr("Rows.Next", err)
	}
	if !hasRow {
		return io.EOF
	}

	if r.colTypes == nil {
		if r.stmt.colTypes != nil {
			r.colTypes = r.stmt.colTypes
			r.colDeclTypes = r.stmt.colDeclTypes
		} else {
			colCount := r.stmt.stmt.ColumnCount()
			r.colTypes = make([]sqliteh.ColumnType, colCount)
			r.colDeclTypes = make([]string, colCount)
			for i := range r.colTypes {
				r.colTypes[i] = r.stmt.stmt.ColumnType(i)
				r.colDeclTypes[i] = r.stmt.stmt.ColumnDeclType(i)
			}
			if r.stmt.persist {
				r.stmt.colTypes = r.colTypes
				r.stmt.colDeclTypes = r.colDeclTypes
			}
		}
	}

	for i := range dest {
		if strings.EqualFold(r.colDeclTypes[i], "DATETIME") || strings.EqualFold(r.colDeclTypes[i], "DATE") {
			switch r.colTypes[i] {
			case sqliteh.SQLITE_INTEGER:
				v := r.stmt.stmt.ColumnInt64(i)
				dest[i] = time.Unix(v, 0)
			case sqliteh.SQLITE_FLOAT:
				dest[i] = r.stmt.stmt.ColumnDouble(i)
				// TODO: treat as time?
			case sqliteh.SQLITE_TEXT:
				v := r.stmt.stmt.ColumnText(i)
				format := TimeFormat
				if len(format) < len(v) {
					format = strings.TrimSuffix(format, "-0700")
				}
				if len(format) < len(v) {
					format = strings.TrimSuffix(format, ".000")
				}
				if len(format) < len(v) {
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
		switch r.colTypes[i] {
		case sqliteh.SQLITE_INTEGER:
			val := r.stmt.stmt.ColumnInt64(i)
			if strings.EqualFold(r.colDeclTypes[i], "BOOLEAN") {
				dest[i] = val > 0
			} else {
				dest[i] = val
			}
		case sqliteh.SQLITE_FLOAT:
			dest[i] = r.stmt.stmt.ColumnDouble(i)
		case sqliteh.SQLITE_TEXT:
			dest[i] = r.stmt.stmt.ColumnText(i)
		case sqliteh.SQLITE_BLOB:
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
	Raw(func(driverConn interface{}) error) error
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
	return sqlconn.Raw(func(driverConn interface{}) error {
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
			_, err = cstmt.Step()
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
	return sqlconn.Raw(func(driverConn interface{}) error {
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
	return sqlconn.Raw(func(driverConn interface{}) error {
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
	return state, sqlconn.Raw(func(driverConn interface{}) error {
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
	err = sqlconn.Raw(func(driverConn interface{}) error {
		c, ok := driverConn.(*conn)
		if !ok {
			return fmt.Errorf("sqlite.Checkpoint: sql.Conn is not the sqlite driver: %T", driverConn)
		}
		numFrames, numFramesCheckpointed, err = c.db.Checkpoint(dbName, mode)
		return reserr(c.db, "Checkpoint", dbName, err)
	})
	return numFrames, numFramesCheckpointed, err
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
