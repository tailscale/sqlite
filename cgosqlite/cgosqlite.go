package cgosqlite

// This list of compiler options is heavily influenced by:
//
// https://www.sqlite.org/compile.html#recommended_compile_time_options
//
// One exception is we do not use SQLITE_OMIT_DECLTYPE, as the design
// of the database/sql driver seems to require it.

// #cgo CFLAGS: -DSQLITE_THREADSAFE=2
// #cgo CFLAGS: -DSSQLITE_DQS=0
// #cgo CFLAGS: -DSSQLITE_DEFAULT_MEMSTATUS=0
// #cgo CFLAGS: -DSSQLITE_DEFAULT_WAL_SYNCHRONOUS=1
// #cgo CFLAGS: -DSSQLITE_LIKE_DOESNT_MATCH_BLOBS
// #cgo CFLAGS: -DSSQLITE_MAX_EXPR_DEPTH=0
// #cgo CFLAGS: -DSSQLITE_OMIT_DEPRECATED
// #cgo CFLAGS: -DSSQLITE_OMIT_PROGRESS_CALLBACK
// #cgo CFLAGS: -DSSQLITE_OMIT_SHARED_CACHE
// #cgo CFLAGS: -DSSQLITE_USE_ALLOCA
// #cgo CFLAGS: -DSSQLITE_OMIT_AUTOINIT
// #cgo CFLAGS: -DSQLITE_ENABLE_FTS5
// #cgo CFLAGS: -DSQLITE_ENABLE_RTREE
// #cgo CFLAGS: -DSQLITE_ENABLE_JSON1
// #cgo CFLAGS: -DSQLITE_ENABLE_SESSION
// #cgo CFLAGS: -DSQLITE_ENABLE_SNAPSHOT
// #cgo CFLAGS: -DSQLITE_ENABLE_PREUPDATE_HOOK
// #cgo CFLAGS: -DSQLITE_ENABLE_COLUMN_METADATA
// #cgo CFLAGS: -DSQLITE_ENABLE_STAT4
// #cgo CFLAGS: -DHAVE_USLEEP=1
// #cgo linux LDFLAGS: -ldl -lm
// #cgo linux CFLAGS: -std=c99
//
// #include <stdint.h>
// #include <stdlib.h>
// #include <string.h>
// #include <pthread.h>
// #include <sqlite3.h>
// #include "cgosqlite.h"
import "C"
import (
	"unsafe"

	"github.com/tailscale/sqlite/sqliteh"
)

func init() {
	C.sqlite3_initialize()
}

// DB is an sqlite3* database connection object.
// https://sqlite.org/c3ref/sqlite3.html
type DB struct {
	db *C.sqlite3

	declTypes map[string]string
}

// Stmt is an sqlite3_stmt* database connection object.
// https://sqlite.org/c3ref/stmt.html
type Stmt struct {
	db   *DB
	stmt *C.sqlite3_stmt
}

// Open is sqlite3_open_v2.
//
// Surprisingly: an error opening the DB can return a non-nil handle.
// Call Close on it.
//
// https://sqlite.org/c3ref/open.html
func Open(filename string, flags sqliteh.OpenFlags, vfs string) (*DB, error) {
	cfilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cfilename))

	cvfs := (*C.char)(nil)
	if vfs != "" {
		cvfs = C.CString(vfs)
		defer C.free(unsafe.Pointer(cvfs))
	}

	var cdb *C.sqlite3
	res := C.sqlite3_open_v2(cfilename, &cdb, C.int(flags), cvfs)
	var db *DB
	if cdb != nil {
		db = &DB{db: cdb}
	}
	return db, errCode(res)
}

// Close is sqlite3_close.
// https://sqlite.org/c3ref/close.html
func (db *DB) Close() error {
	// TODO(crawshaw): consider using sqlite3_close_v2, if we are going to use finalizers for cleanup.
	res := C.sqlite3_close(db.db)
	return errCode(res)
}

// ErrMsg is sqlite3_errmsg.
// https://sqlite.org/c3ref/errcode.html
func (db *DB) ErrMsg() string {
	return C.GoString(C.sqlite3_errmsg(db.db))
}

// Changes is sqlite3_changes.
// https://sqlite.org/c3ref/changes.html
func (db *DB) Changes() int {
	return int(C.sqlite3_changes(db.db))
}

// TotalChanges is sqlite3_total_changes.
// https://sqlite.org/c3ref/total_changes.html
func (db *DB) TotalChanges() int {
	return int(C.sqlite3_total_changes(db.db))
}

// ExtendedErrCode is sqlite3_extended_errcode.
// https://sqlite.org/c3ref/errcode.html
func (db *DB) ExtendedErrCode() sqliteh.Code {
	return sqliteh.Code(C.sqlite3_extended_errcode(db.db))
}

// LastInsertRowid is sqlite3_last_insert_rowid.
// https://sqlite.org/c3ref/last_insert_rowid.html
func (db *DB) LastInsertRowid() int64 {
	return int64(C.sqlite3_last_insert_rowid(db.db))
}

// Prepare is sqlite3_prepare_v3.
// https://www.sqlite.org/c3ref/prepare.html
func (db *DB) Prepare(query string, prepFlags sqliteh.PrepareFlags) (stmt *Stmt, remainingQuery string, err error) {
	csql := C.CString(query)
	defer C.free(unsafe.Pointer(csql))

	var cstmt *C.sqlite3_stmt
	var csqlTail *C.char
	res := C.sqlite3_prepare_v3(db.db, csql, C.int(len(query))+1, C.uint(prepFlags), &cstmt, &csqlTail)
	if err := errCode(res); err != nil {
		return nil, "", err
	}
	remainingQuery = query[len(query)-int(C.strlen(csqlTail)):]
	return &Stmt{db: db, stmt: cstmt}, remainingQuery, nil
}

// DBHandle is sqlite3_db_handle.
// https://www.sqlite.org/c3ref/db_handle.html.
func (stmt *Stmt) DBHandle() *DB {
	cdb := C.sqlite3_db_handle(stmt.stmt)
	if cdb != nil {
		return &DB{db: cdb}
	}
	return nil
}

// SQL is sqlite3_sql.
// https://www.sqlite.org/c3ref/expanded_sql.html
func (stmt *Stmt) SQL() string {
	return C.GoString(C.sqlite3_sql(stmt.stmt))
}

// ExpandedSQL is sqlite3_expanded_sql.
// https://www.sqlite.org/c3ref/expanded_sql.html
func (stmt *Stmt) ExpandedSQL() string {
	return C.GoString(C.sqlite3_expanded_sql(stmt.stmt))
}

// Reset is sqlite3_reset.
// https://www.sqlite.org/c3ref/reset.html
func (stmt *Stmt) Reset() error {
	return errCode(C.sqlite3_reset(stmt.stmt))
}

// Finalize is sqlite3_finalize.
// https://sqlite.org/c3ref/finalize.html
func (stmt *Stmt) Finalize() error {
	return errCode(C.sqlite3_finalize(stmt.stmt))
}

// ClearBindings sqlite3_clear_bindings.
//
// https://www.sqlite.org/c3ref/clear_bindings.html
func (stmt *Stmt) ClearBindings() error {
	return errCode(C.sqlite3_clear_bindings(stmt.stmt))
}

func (stmt *Stmt) ColumnDatabaseName(col int) string {
	return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_database_name(stmt.stmt, C.int(col)))))
}

func (stmt *Stmt) ColumnTableName(col int) string {
	return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_table_name(stmt.stmt, C.int(col)))))
}

// Step is sqlite3_step.
// 	For SQLITE_ROW, Step returns (true, nil).
// 	For SQLITE_DONE, Step returns (false, nil).
// 	For any error, Step retursn (false, err).
// https://www.sqlite.org/c3ref/step.html
func (stmt *Stmt) Step() (row bool, err error) {
	res := C.sqlite3_step(stmt.stmt)
	switch res {
	case C.SQLITE_ROW:
		return true, nil
	case C.SQLITE_DONE:
		return false, nil
	default:
		return false, errCode(res)
	}
}

// StepResult is sqlite3_step + sqlite3_last_insert_rowid + sqlite3_changes.
// 	For SQLITE_ROW, Step returns (true, nil).
// 	For SQLITE_DONE, Step returns (false, nil).
// 	For any error, Step retursn (false, err).
// https://www.sqlite.org/c3ref/step.html
func (stmt *Stmt) StepResult() (row bool, lastInsertRowID, changes int64, err error) {
	var rowid, chng C.sqlite3_int64
	res := C.step_result(stmt.stmt, &rowid, &chng)
	lastInsertRowID = int64(rowid)
	changes = int64(chng)

	switch res {
	case C.SQLITE_ROW:
		return true, lastInsertRowID, changes, nil
	case C.SQLITE_DONE:
		return false, lastInsertRowID, changes, nil
	default:
		return false, lastInsertRowID, changes, errCode(res)
	}
}

// BindDouble is sqlite3_bind_double.
// https://sqlite.org/c3ref/bind_blob.html
func (stmt *Stmt) BindDouble(col int, val float64) error {
	return errCode(C.sqlite3_bind_double(stmt.stmt, C.int(col), C.double(val)))
}

// BindInt64 is sqlite3_bind_int64.
// https://sqlite.org/c3ref/bind_blob.html
func (stmt *Stmt) BindInt64(col int, val int64) error {
	return errCode(C.sqlite3_bind_int64(stmt.stmt, C.int(col), C.sqlite3_int64(val)))
}

// BindNull is sqlite3_bind_null.
// https://sqlite.org/c3ref/bind_blob.html
func (stmt *Stmt) BindNull(col int) error {
	return errCode(C.sqlite3_bind_null(stmt.stmt, C.int(col)))
}

// BindText64 is sqlite3_bind_text64.
// https://sqlite.org/c3ref/bind_blob.html
func (stmt *Stmt) BindText64(col int, val string) error {
	if len(val) == 0 {
		return errCode(C.bind_text64_empty(stmt.stmt, C.int(col)))
	}
	v := C.CString(val) // freed by sqlite
	return errCode(C.bind_text64(stmt.stmt, C.int(col), v, C.sqlite3_uint64(len(val))))
}

// BindZeroBlob64 is sqlite3_bind_zeroblob64.
// https://sqlite.org/c3ref/bind_blob.html
func (stmt *Stmt) BindZeroBlob64(col int, n uint64) error {
	return errCode(C.sqlite3_bind_zeroblob64(stmt.stmt, C.int(col), C.sqlite3_uint64(n)))
}

// BindBlob64 is sqlite3_bind_blob64.
// https://sqlite.org/c3ref/bind_blob.html
func (stmt *Stmt) BindBlob64(col int, val []byte) error {
	var str *C.char
	if len(val) > 0 {
		str = (*C.char)(unsafe.Pointer(&val[0]))
	}
	return errCode(C.bind_blob64(stmt.stmt, C.int(col), str, C.sqlite3_uint64(len(val))))
}

// BindParameterCount is sqlite3_bind_parameter_count.
// https://sqlite.org/c3ref/bind_parameter_count.html
func (stmt *Stmt) BindParameterCount() int {
	return int(C.sqlite3_bind_parameter_count(stmt.stmt))
}

// BindParameterName is sqlite3_bind_parameter_name.
// https://sqlite.org/c3ref/bind_parameter_count.html
func (stmt *Stmt) BindParameterName(col int) string {
	cstr := C.sqlite3_bind_parameter_name(stmt.stmt, C.int(col))
	if cstr == nil {
		return ""
	}
	return C.GoString(cstr)
}

// BindParameterIndex is sqlite3_bind_parameter_index.
// Returns zero if no matching parameter is found.
// https://sqlite.org/c3ref/bind_parameter_index.html
func (stmt *Stmt) BindParameterIndex(name string) int {
	return int(C.bind_parameter_index(stmt.stmt, name))
}

// BindParameterIndexSearch calls sqlite3_bind_parameter_index,
// prepending ':', '@', and '?' until it finds a matching paramter.
func (stmt *Stmt) BindParameterIndexSearch(name string) int {
	// TODO: do prepend in C to save allocation
	if i := stmt.BindParameterIndex(":" + name); i > 0 {
		return i
	}
	if i := stmt.BindParameterIndex("@" + name); i > 0 {
		return i
	}
	return stmt.BindParameterIndex("?" + name)
}

// Column is sqlite3_column_count.
// https://sqlite.org/c3ref/column_count.html
func (stmt *Stmt) ColumnCount() int {
	return int(C.sqlite3_column_count(stmt.stmt))
}

// ColumnName is sqlite3_column_name.
// https://sqlite.org/c3ref/column_name.html
func (stmt *Stmt) ColumnName(col int) string {
	return C.GoString(C.sqlite3_column_name(stmt.stmt, C.int(col)))
}

// ColumnText is sqlite3_column_text.
// https://sqlite.org/c3ref/column_blob.html
func (stmt *Stmt) ColumnText(col int) string {
	str := (*C.char)(unsafe.Pointer(C.sqlite3_column_text(stmt.stmt, C.int(col))))
	n := C.sqlite3_column_bytes(stmt.stmt, C.int(col))
	if str == nil || n == 0 {
		return ""
	}
	return C.GoStringN(str, n)
}

// ColumnBlob is sqlite3_column_blob.
//
// WARNING: The returned memory is managed by C and is only valid until
//          another call is made on this Stmt.
//
// https://sqlite.org/c3ref/column_blob.html
func (stmt *Stmt) ColumnBlob(col int) []byte {
	res := C.sqlite3_column_blob(stmt.stmt, C.int(col))
	if res == nil {
		return nil
	}
	n := int(C.sqlite3_column_bytes(stmt.stmt, C.int(col)))

	slice := struct {
		data unsafe.Pointer
		len  int
		cap  int
	}{data: unsafe.Pointer(res), len: n, cap: n}
	return *(*[]byte)(unsafe.Pointer(&slice))
}

// ColumnDouble is sqlite3_column_double.
// https://sqlite.org/c3ref/column_blob.html
func (stmt *Stmt) ColumnDouble(col int) float64 {
	return float64(C.sqlite3_column_double(stmt.stmt, C.int(col)))
}

// ColumnInt64 is sqlite3_column_int64.
// https://sqlite.org/c3ref/column_blob.html
func (stmt *Stmt) ColumnInt64(col int) int64 {
	return int64(C.sqlite3_column_int64(stmt.stmt, C.int(col)))
}

// ColumnType is sqlite3_column_type.
// https://www.sqlite.org/c3ref/column_blob.html
func (stmt *Stmt) ColumnType(col int) sqliteh.ColumnType {
	return sqliteh.ColumnType(C.sqlite3_column_type(stmt.stmt, C.int(col)))
}

// ColumnDeclType is sqlite3_column_decltype.
// https://sqlite.org/c3ref/column_decltype.html
func (stmt *Stmt) ColumnDeclType(col int) string {
	cstr := C.sqlite3_column_decltype(stmt.stmt, C.int(col))
	if cstr == nil {
		return ""
	}
	clen := C.strlen(cstr)
	b := (*[1 << 30]byte)(unsafe.Pointer(cstr))[0:clen]
	if stmt.db.declTypes == nil {
		stmt.db.declTypes = make(map[string]string)
	}
	if res, found := stmt.db.declTypes[string(b)]; found {
		return res
	}
	res := string(b)
	stmt.db.declTypes[res] = res
	return res
}

var emptyCStr = C.CString("")

func errCode(code C.int) error { return sqliteh.CodeAsError(sqliteh.Code(code)) }
