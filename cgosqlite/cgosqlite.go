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
	"time"
	"unsafe"

	"github.com/tailscale/sqlite/sqliteh"
)

func init() {
	C.sqlite3_initialize()
}

// DB implements sqliteh.DB.
type DB struct {
	db *C.sqlite3

	declTypes map[string]string
}

// Stmt implements sqliteh.Stmt.
type Stmt struct {
	db   *DB
	stmt *C.sqlite3_stmt
}

// Open implements sqliteh.OpenFunc.
func Open(filename string, flags sqliteh.OpenFlags, vfs string) (sqliteh.DB, error) {
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

func (db *DB) Close() error {
	// TODO(crawshaw): consider using sqlite3_close_v2, if we are going to use finalizers for cleanup.
	res := C.sqlite3_close(db.db)
	return errCode(res)
}

func (db *DB) ErrMsg() string {
	return C.GoString(C.sqlite3_errmsg(db.db))
}

func (db *DB) Changes() int {
	return int(C.sqlite3_changes(db.db))
}

func (db *DB) TotalChanges() int {
	return int(C.sqlite3_total_changes(db.db))
}

func (db *DB) ExtendedErrCode() sqliteh.Code {
	return sqliteh.Code(C.sqlite3_extended_errcode(db.db))
}

func (db *DB) LastInsertRowid() int64 {
	return int64(C.sqlite3_last_insert_rowid(db.db))
}

func (db *DB) BusyTimeout(d time.Duration) {
	C.sqlite3_busy_timeout(db.db, C.int(d/1e6))
}

func (db *DB) Prepare(query string, prepFlags sqliteh.PrepareFlags) (stmt sqliteh.Stmt, remainingQuery string, err error) {
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

func (stmt *Stmt) DBHandle() sqliteh.DB {
	cdb := C.sqlite3_db_handle(stmt.stmt)
	if cdb != nil {
		return &DB{db: cdb}
	}
	return nil
}

func (stmt *Stmt) SQL() string {
	return C.GoString(C.sqlite3_sql(stmt.stmt))
}

func (stmt *Stmt) ExpandedSQL() string {
	return C.GoString(C.sqlite3_expanded_sql(stmt.stmt))
}

func (stmt *Stmt) Reset() error {
	return errCode(C.sqlite3_reset(stmt.stmt))
}

func (stmt *Stmt) Finalize() error {
	return errCode(C.sqlite3_finalize(stmt.stmt))
}

func (stmt *Stmt) ClearBindings() error {
	return errCode(C.sqlite3_clear_bindings(stmt.stmt))
}

func (stmt *Stmt) ResetAndClear() error {
	return errCode(C.reset_and_clear(stmt.stmt))
}

func (stmt *Stmt) ColumnDatabaseName(col int) string {
	return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_database_name(stmt.stmt, C.int(col)))))
}

func (stmt *Stmt) ColumnTableName(col int) string {
	return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_table_name(stmt.stmt, C.int(col)))))
}

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

func (stmt *Stmt) BindDouble(col int, val float64) error {
	return errCode(C.sqlite3_bind_double(stmt.stmt, C.int(col), C.double(val)))
}

func (stmt *Stmt) BindInt64(col int, val int64) error {
	return errCode(C.sqlite3_bind_int64(stmt.stmt, C.int(col), C.sqlite3_int64(val)))
}

func (stmt *Stmt) BindNull(col int) error {
	return errCode(C.sqlite3_bind_null(stmt.stmt, C.int(col)))
}

func (stmt *Stmt) BindText64(col int, val string) error {
	if len(val) == 0 {
		return errCode(C.bind_text64_empty(stmt.stmt, C.int(col)))
	}
	v := C.CString(val) // freed by sqlite
	return errCode(C.bind_text64(stmt.stmt, C.int(col), v, C.sqlite3_uint64(len(val))))
}

func (stmt *Stmt) BindZeroBlob64(col int, n uint64) error {
	return errCode(C.sqlite3_bind_zeroblob64(stmt.stmt, C.int(col), C.sqlite3_uint64(n)))
}

func (stmt *Stmt) BindBlob64(col int, val []byte) error {
	var str *C.char
	if len(val) > 0 {
		str = (*C.char)(unsafe.Pointer(&val[0]))
	}
	return errCode(C.bind_blob64(stmt.stmt, C.int(col), str, C.sqlite3_uint64(len(val))))
}

func (stmt *Stmt) BindParameterCount() int {
	return int(C.sqlite3_bind_parameter_count(stmt.stmt))
}

func (stmt *Stmt) BindParameterName(col int) string {
	cstr := C.sqlite3_bind_parameter_name(stmt.stmt, C.int(col))
	if cstr == nil {
		return ""
	}
	return C.GoString(cstr)
}

func (stmt *Stmt) BindParameterIndex(name string) int {
	return int(C.bind_parameter_index(stmt.stmt, name))
}

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

func (stmt *Stmt) ColumnCount() int {
	return int(C.sqlite3_column_count(stmt.stmt))
}

func (stmt *Stmt) ColumnName(col int) string {
	return C.GoString(C.sqlite3_column_name(stmt.stmt, C.int(col)))
}

func (stmt *Stmt) ColumnText(col int) string {
	str := (*C.char)(unsafe.Pointer(C.sqlite3_column_text(stmt.stmt, C.int(col))))
	n := C.sqlite3_column_bytes(stmt.stmt, C.int(col))
	if str == nil || n == 0 {
		return ""
	}
	return C.GoStringN(str, n)
}

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

func (stmt *Stmt) ColumnDouble(col int) float64 {
	return float64(C.sqlite3_column_double(stmt.stmt, C.int(col)))
}

func (stmt *Stmt) ColumnInt64(col int) int64 {
	return int64(C.sqlite3_column_int64(stmt.stmt, C.int(col)))
}

func (stmt *Stmt) ColumnType(col int) sqliteh.ColumnType {
	return sqliteh.ColumnType(C.sqlite3_column_type(stmt.stmt, C.int(col)))
}

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
