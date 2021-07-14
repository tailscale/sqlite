// Copyright (c) 2021 Tailscale Inc & AUTHORS All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite implements a database/sql driver for SQLite3.
//
// This driver requires a file: URI always be used to open a database.
// For details see https://sqlite.org/c3ref/open.html#urifilenames.
//
//
// Memory Mode
//
// In-memory databases are popular for tests.
// Use standard URIs to open in-memory databases, as documented
// in the link above. E.g.
//
//	file:/whatever?mode=memory
//
// If opening a database in memory mode with the "?mode=memory" query
// parameter, make sure immediately after opening to set
//
//	db.SetMaxOpenConns(1)
//
// This driver does not support shared cache mode, as the SQLite
// authors recommend against it:
//
//	"Any use of shared cache is discouraged."
//	https://www.sqlite.org/c3ref/enable_shared_cache.html
//
// For far more realistic in-memory database testing, use
//
//	PRAGMA journal_mode=WAL;
//	PRAGMA synchronous=OFF;
//
// On any modern OS, this will generate syscalls as part of writing
// data, but will never block on I/O.
//
//
// Binding Types
//
// SQLite is flexible about type conversions, and so is this driver.
// Almost all "basic" Go types (int, float64, string) are accepted and
// directly mapped into SQLite, even if they are named Go types.
// The time.Time type is also accepted (described below).
// Values that implement encoding.TextMarshaler or json.Marshaler are
// stored in SQLite in their marshaled form.
//
//
// Binding Time
//
// While SQLite3 has no strict time datatype, it does have a series of built-in
// functions that operate on timestamps that expect columns to be in one of many
// formats: https://sqlite.org/lang_datefunc.html
//
// When encoding a time.Time into one of SQLite's preferred formats, we use the
// shortest timestamp format that can accurately represent the time.Time.
// The supported formats are:
//
//	2. YYYY-MM-DD HH:MM
//	3. YYYY-MM-DD HH:MM:SS
//	4. YYYY-MM-DD HH:MM:SS.SSS
//
// If the time.Time is not UTC (strongly consider storing times in UTC!),
// we follow SQLite's norm of appending "[+-]HH:MM" to the above formats.
//
// It is common in SQLite to store "Unix time", seconds-since-epoch in an
// INTEGER column. This is understood by the date and time functions documented
// in the link above. If you want to do that, pass the result of time.Time.Unix
// to the driver.
//
//
// Reading Time
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
	"time"

	"github.com/tailscale/sqlite/sqliteh"
)

var Open sqliteh.OpenFunc = func(string, sqliteh.OpenFlags, string) (sqliteh.DB, error) {
	return nil, fmt.Errorf("cgosqlite.Open is missing")
}

// TimeFormat is the string format this driver uses to store
// microsecond-precision time in SQLite in text format.
const TimeFormat = "2006-01-02 15:04:05.000-0700"

func init() {
	sql.Register("sqlite3", drv{})
}

type drv struct{}

func (d drv) Open(name string) (driver.Conn, error) { panic("deprecated, unused") }
func (d drv) OpenConnector(name string) (driver.Connector, error) {
	return &connector{name: name}, nil
}

type connector struct {
	name string
}

func (c *connector) Driver() driver.Driver { return drv{} }
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	flags := sqliteh.SQLITE_OPEN_URI |
		sqliteh.SQLITE_OPEN_READWRITE |
		sqliteh.SQLITE_OPEN_CREATE
	db, err := Open(c.name, flags, "")
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
	return &conn{db: db}, nil
}

type conn struct {
	db    sqliteh.DB
	stmts map[string]*stmt // persisted statements
}

func (c *conn) Prepare(query string) (driver.Stmt, error) { panic("deprecated, unused") }
func (c *conn) Begin() (driver.Tx, error)                 { panic("deprecated, unused") }
func (c *conn) Close() error {
	return reserr(c.db, "Conn.Close", "", c.db.Close())
}
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	persist := ctx.Value(persistQuery{}) != nil
	return c.prepare(query, persist)
}

func (c *conn) prepare(query string, persist bool) (*stmt, error) {
	query = strings.TrimSpace(query)
	if s := c.stmts[query]; s != nil {
		return s, nil
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
	s := &stmt{db: c.db, stmt: cstmt, query: query, persist: persist, numInput: -1}

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
	s, err := c.prepare(query, true)
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
	if opts.ReadOnly {
		// TODO: match the state of the connection.
		//return nil, errors.New("github.com/tailscale/sqlite driver does not support read-only Tx yet")
	}
	if err := c.execInternal(ctx, "BEGIN"); err != nil {
		return nil, err
	}
	return &connTx{c: c}, nil
}

type connTx struct {
	c *conn
}

func (tx *connTx) Commit() error {
	return tx.c.execInternal(context.Background(), "COMMIT")
}

func (tx *connTx) Rollback() error {
	return tx.c.execInternal(context.Background(), "ROLLBACK")
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
	db      sqliteh.DB
	stmt    sqliteh.Stmt
	query   string
	persist bool // true if stmt is cached and lives beyond Close
	bound   bool // true if stmt has parameters bound

	numInput int // filled on first NumInput only if persist==true

	// filled on first step only if persist==true
	colTypes     []sqliteh.ColumnType
	colDeclTypes []string
	colNames     []string
}

func (s *stmt) reserr(loc string, err error) error { return reserr(s.db, loc, s.query, err) }

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
		return nil, s.reserr("Stmt.Query(Reset)", err)
	}
	if err := s.bindAll(args); err != nil {
		return nil, err
	}
	row, lastInsertRowID, changes, err := s.stmt.StepResult()
	s.bound = false // StepResult resets the query
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
	return s.stmt.ResetAndClear()
}

func (s *stmt) bindAll(args []driver.NamedValue) error {
	if s.bound {
		panic("sqlite: impossible state, query already running: " + s.query)
	}
	s.bound = true
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
		return err
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
				return err
			}
		}
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
