//go:build cgo

package sqlitepool

// This file contains bridging functions designed to let users of
// database/sql move to sqlitepool without changing the semantics
// of their code.
//
// Eventually users should piece-wise migrate to another interface.
// (Or we should invest in this interface? Seems suboptimal.)

import (
	sqlpkg "database/sql"
	"database/sql/driver"
	"encoding"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/tailscale/sqlite-exp/sqliteh"
)

// Exec is like database/sql.Tx.Exec.
// Only use this for one-off/rare queries.
// For normal queries, see the Exec method on Tx.
func Exec(db sqliteh.DB, sql string, args ...any) error {
	stmt, _, err := db.Prepare(sql, 0)
	if err != nil {
		return err
	}
	if err := bindAll(db, stmt, args...); err != nil {
		return fmt.Errorf("Exec: %w", err)
	}
	_, _, _, _, err = stmt.StepResult()
	if err != nil {
		err = fmt.Errorf("%w: %v", err, db.ErrMsg())
	}
	stmt.Finalize()
	return err
}

// QueryRow is like database/sql.Tx.QueryRow.
// Only use this for one-off/rare queries.
// For normal queries, see the methods on Rx.
func QueryRow(db sqliteh.DB, sql string, args ...any) *Row {
	stmt, _, err := db.Prepare(sql, 0)
	if err != nil {
		return &Row{err: fmt.Errorf("QueryRow: %w: %v", err, db.ErrMsg())}
	}
	if err := bindAll(db, stmt, args...); err != nil {
		return &Row{err: fmt.Errorf("QueryRow: %w", err)}
	}
	row, err := stmt.Step(nil)
	if err != nil {
		msg := db.ErrMsg()
		stmt.Finalize()
		return &Row{err: fmt.Errorf("QueryRow: %w: %v", err, msg)}
	}
	if !row {
		stmt.Finalize()
		return &Row{err: sqlpkg.ErrNoRows}
	}
	return &Row{stmt: stmt, oneOff: true}
}

// Query is like database/sql.Tx.Query.
// Only use this for one-off/rare queries.
// For normal queries, see the methods on Rx.
func Query(db sqliteh.DB, sql string, args ...any) (*Rows, error) {
	stmt, _, err := db.Prepare(sql, 0)
	if err != nil {
		return nil, fmt.Errorf("Query: %w: %v", err, db.ErrMsg())
	}
	if err := bindAll(db, stmt, args...); err != nil {
		return nil, err
	}
	return &Rows{stmt: stmt, oneOff: true}, nil
}

// Exec is like database/sql.Tx.Exec.
func (tx *Tx) Exec(sql string, args ...any) error {
	stmt := tx.Prepare(sql)
	if err := bindAll(tx.conn.db, stmt, args...); err != nil {
		return err
	}
	_, _, _, _, err := stmt.StepResult()
	if err != nil {
		return fmt.Errorf("%w: %v", err, tx.conn.db.ErrMsg())
	}
	return nil
}

func (tx *Tx) ExecRes(sql string, args ...any) (rowsAffected int64, err error) {
	stmt := tx.Prepare(sql)
	if err := bindAll(tx.conn.db, stmt, args...); err != nil {
		return 0, err
	}
	_, _, rowsAffected, _, err = stmt.StepResult()
	return rowsAffected, err
}

// QueryRow is like database/sql.Tx.QueryRow.
func (rx *Rx) QueryRow(sql string, args ...any) *Row {
	stmt := rx.Prepare(sql)
	if err := bindAll(rx.conn.db, stmt, args...); err != nil {
		return &Row{err: fmt.Errorf("QueryRow: %w", err)}
	}
	row, err := stmt.Step(nil)
	if err != nil {
		msg := rx.DB().ErrMsg()
		stmt.ResetAndClear()
		return &Row{err: fmt.Errorf("QueryRow: %w: %v", err, msg)}
	}
	if !row {
		stmt.ResetAndClear()
		return &Row{err: sqlpkg.ErrNoRows}
	}
	return &Row{stmt: stmt}
}

// Query is like database/sql.Tx.Query.
func (rx *Rx) Query(sql string, args ...any) (*Rows, error) {
	stmt := rx.Prepare(sql)
	if err := bindAll(rx.conn.db, stmt, args...); err != nil {
		return nil, fmt.Errorf("Query: %w", err)
	}
	return &Rows{stmt: stmt}, nil
}

// Rows is like database/sql.Tx.Rows.
type Rows struct {
	stmt   sqliteh.Stmt
	err    error
	oneOff bool
}

func (rs *Rows) Next() bool {
	if rs.err != nil {
		return false
	}
	row, err := rs.stmt.Step(nil)
	if err != nil {
		rs.err = fmt.Errorf("QueryRow.Next: %w: %v", err, rs.stmt.DBHandle().ErrMsg())
		return false
	}
	if !row {
		rs.stmt.ResetAndClear()
	}
	return row
}

func (rs *Rows) Err() error {
	return rs.err
}

func (rs *Rows) Scan(dest ...any) error {
	if rs.err != nil {
		return rs.err
	}
	return scanAll(rs.stmt, dest...)
}

func (rs *Rows) Close() error {
	if rs.stmt == nil {
		return nil
	}
	_, err := rs.stmt.ResetAndClear()
	msg := rs.stmt.DBHandle().ErrMsg()
	var err2 error
	if rs.oneOff {
		err2 = rs.stmt.Finalize()
	}
	rs.stmt = nil
	if err != nil {
		return fmt.Errorf("Rows.ResetAndClear: %w: %v", err, msg)
	}
	if err2 != nil {
		return fmt.Errorf("Rows.ResetAndClear: %w: %v", err2, rs.stmt.DBHandle().ErrMsg())
	}
	return nil
}

// Row is like database/sql.Tx.Row.
type Row struct {
	stmt   sqliteh.Stmt
	err    error
	oneOff bool
}

func (r *Row) Err() error {
	return r.err
}

func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	err := scanAll(r.stmt, dest...)
	r.stmt.ResetAndClear()
	if r.oneOff {
		r.stmt.Finalize()
	}
	return err
}

type scanner interface {
	Scan(value any) error
}

// scanAll mimics (some of) the sqlite driver's scanning logic, which is
// split across the driver and the database/sql package.
func scanAll(stmt sqliteh.Stmt, dest ...any) error {
	for i := 0; i < len(dest); i++ {
		if s, ok := dest[i].(scanner); ok {
			// We have a handful of *sql.NullInt64 objects in
			// our tree, so we implement minimal support for
			// them here. TODO: remove some time.
			var v any
			switch stmt.ColumnType(i) {
			case sqliteh.SQLITE_INTEGER:
				v = stmt.ColumnInt64(i)
			case sqliteh.SQLITE_FLOAT:
				v = stmt.ColumnDouble(i)
			case sqliteh.SQLITE_TEXT:
				v = stmt.ColumnText(i)
			case sqliteh.SQLITE_BLOB:
				v = stmt.ColumnText(i)
			case sqliteh.SQLITE_NULL:
				v = nil
			}
			if err := s.Scan(v); err != nil {
				return err
			}
			continue
		}
		v := reflect.ValueOf(dest[i])
		if v.Elem().Kind() == reflect.Slice && v.Elem().Type().Elem().Kind() == reflect.Uint8 {
			b := append([]byte(nil), stmt.ColumnBlob(i)...)
			v.Elem().SetBytes(b)
			continue
		}
		switch v.Elem().Kind() {
		case reflect.Bool:
			v.Elem().SetBool(stmt.ColumnInt64(i) != 0)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v.Elem().SetInt(stmt.ColumnInt64(i))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			v.Elem().SetUint(uint64(stmt.ColumnInt64(i)))
		case reflect.Float32, reflect.Float64:
			v.Elem().SetFloat(stmt.ColumnDouble(i))
		case reflect.String:
			v.Elem().SetString(stmt.ColumnText(i))
		default:
			return fmt.Errorf("sqlitepool.scan:%d: cannot handle destination kind %v (%T)", i, v.Kind(), dest[i])
		}
	}
	return nil
}

func bindAll(db sqliteh.DB, stmt sqliteh.Stmt, args ...any) error {
	for i, arg := range args {
		if err := bind(db, stmt, i+1, arg); err != nil {
			stmt.ResetAndClear()
			return fmt.Errorf("bind: %d, %q: %w", i, arg, err)
		}
	}
	return nil
}

type driverValue interface {
	Value() (driver.Value, error)
}

// bind, from the driver in sqlite.go.
func bind(db sqliteh.DB, s sqliteh.Stmt, ordinal int, v any) error {
	// Start with obvious types, including time.Time before TextMarshaler.
	found, err := bindBasic(db, s, ordinal, v)
	if err != nil {
		return err
	} else if found {
		return nil
	}

	if m, _ := v.(driverValue); m != nil {
		// We have a few NullInt64s we need to handle.
		// TODO: remove or rethink in the future.
		var err error
		v, err = m.Value()
		if err != nil {
			return fmt.Errorf("sqlitepool.bind:%d: bad driver.Value: %w", ordinal, err)
		}
		if v == nil {
			_, err := bindBasic(db, s, ordinal, nil)
			return err
		}
	}

	if m, _ := v.(encoding.TextMarshaler); m != nil {
		b, err := m.MarshalText()
		if err != nil {
			return fmt.Errorf("sqlitepool.bind:%d: cannot marshal %T: %w", ordinal, v, err)
		}
		_, err = bindBasic(db, s, ordinal, b)
		return err
	}

	// Look for named basic types or other convertible types.
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Pointer {
		if val.IsNil() {
			_, err := bindBasic(db, s, ordinal, nil)
			return err
		}
		val = val.Elem()
	}
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Bool:
		b := int64(0)
		if val.Bool() {
			b = 1
		}
		_, err := bindBasic(db, s, ordinal, b)
		return err
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var i int64
		if !val.IsZero() {
			i = val.Int()
		}
		_, err := bindBasic(db, s, ordinal, i)
		return err
	case reflect.Uint, reflect.Uint64:
		return fmt.Errorf("sqlitepool.bind:%d: sqlite does not support uint64 (try a string or TextMarshaler)", ordinal)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		_, err := bindBasic(db, s, ordinal, int64(val.Uint()))
		return err
	case reflect.Float32, reflect.Float64:
		_, err := bindBasic(db, s, ordinal, val.Float())
		return err
	case reflect.String:
		_, err := bindBasic(db, s, ordinal, val.String())
		return err
	}

	return fmt.Errorf("sqlitepool.bind:%d: unknown value type %T (try a string or TextMarshaler)", ordinal, v)
}

// bindBasic, from the driver in sqlite.go.
func bindBasic(db sqliteh.DB, s sqliteh.Stmt, ordinal int, v any) (found bool, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("sqlitepool.bind:%d:%T: %w: %v", ordinal, v, err, db.ErrMsg())
		}
	}()
	switch v := v.(type) {
	case nil:
		return true, s.BindNull(ordinal)
	case string:
		return true, s.BindText64(ordinal, v)
	case int:
		return true, s.BindInt64(ordinal, int64(v))
	case int64:
		return true, s.BindInt64(ordinal, v)
	case float64:
		return true, s.BindDouble(ordinal, v)
	case []byte:
		if len(v) == 0 {
			return true, s.BindZeroBlob64(ordinal, 0)
		} else {
			return true, s.BindBlob64(ordinal, v)
		}
	case time.Time:
		// Shortest of:
		//	YYYY-MM-DD HH:MM
		// 	YYYY-MM-DD HH:MM:SS
		//	YYYY-MM-DD HH:MM:SS.SSS
		str := v.Format(timeFormat)
		str = strings.TrimSuffix(str, "-0000")
		str = strings.TrimSuffix(str, ".000")
		str = strings.TrimSuffix(str, ":00")
		return true, s.BindText64(ordinal, str)
	default:
		return false, nil
	}
}

// timeFormat from the driver in sqlite.go.
const timeFormat = "2006-01-02 15:04:05.000-0700"
