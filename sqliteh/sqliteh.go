// Package sqliteh contains SQLite constants for Gophers.
package sqliteh

// Given everything in here has an sqliteh. prefix,
// why not strip the SQLITE_ prefix from constants?
// Because this way standard names show up in search.

import (
	"context"
	"sync"
	"time"
)

// OpenFunc is sqlite3_open_v2.
//
// Surprisingly: an error opening the DB can return a non-nil handle.
// Call Close on it.
//
// https://sqlite.org/c3ref/open.html
type OpenFunc func(filename string, flags OpenFlags, vfs string) (DB, error)

// DB is an sqlite3* database connection object.
// https://sqlite.org/c3ref/sqlite3.html
type DB interface {
	// Close is sqlite3_close.
	// https://sqlite.org/c3ref/close.html
	Close() error
	// Interrupt is sqlite3_interrupt.
	// https://www.sqlite.org/c3ref/interrupt.html
	Interrupt()
	// ErrMsg is sqlite3_errmsg.
	// https://sqlite.org/c3ref/errcode.html
	ErrMsg() string
	// Changes is sqlite3_changes.
	// https://sqlite.org/c3ref/changes.html
	Changes() int
	// TotalChanges is sqlite3_total_changes.
	// https://sqlite.org/c3ref/total_changes.html
	TotalChanges() int
	// ExtendedErrCode is sqlite3_extended_errcode.
	// https://sqlite.org/c3ref/errcode.html
	ExtendedErrCode() Code
	// LastInsertRowid is sqlite3_last_insert_rowid.
	// https://sqlite.org/c3ref/last_insert_rowid.html
	LastInsertRowid() int64
	// Prepare is sqlite3_prepare_v3.
	// https://www.sqlite.org/c3ref/prepare.html
	Prepare(query string, prepFlags PrepareFlags) (stmt Stmt, remainingQuery string, err error)
	// BusyTimeout is sqlite3_busy_timeout.
	// https://www.sqlite.org/c3ref/busy_timeout.html
	BusyTimeout(time.Duration)
	// Checkpoint is sqlite3_wal_checkpoint_v2.
	Checkpoint(db string, mode Checkpoint) (numFrames, numFramesCheckpointed int, err error)
	// AutoCheckpoint is sqlite3_wal_autocheckpoint.
	// https://sqlite.org/c3ref/wal_autocheckpoint.html
	AutoCheckpoint(n int) error
	// TxnState is sqlite3_txn_state.
	TxnState(schema string) TxnState
	// SetWALHook is sqlite3_wal_hook.
	//
	// If hook is nil, the hook is removed.
	SetWALHook(hook func(dbName string, pages int))
	// DisableFunction disables an existing function (including built-ins) using
	// sqlite3_create_function. The name and numArgs must match the existing
	// function's signature.
	//
	DisableFunction(name string, numArgs int) error
}

// Stmt is an sqlite3_stmt* database connection object.
// https://sqlite.org/c3ref/stmt.html
type Stmt interface {
	// DBHandle is sqlite3_db_handle.
	// https://www.sqlite.org/c3ref/db_handle.html.
	DBHandle() DB
	// SQL is sqlite3_sql.
	// https://www.sqlite.org/c3ref/expanded_sql.html
	SQL() string
	// ExpandedSQL is sqlite3_expanded_sql.
	// https://www.sqlite.org/c3ref/expanded_sql.html
	ExpandedSQL() string
	// StartTimer starts recording elapsed duration.
	StartTimer()
	// Reset is sqlite3_reset.
	// https://www.sqlite.org/c3ref/reset.html
	Reset() error
	// ResetAndClear is sqlite3_reset + sqlite3_clear_bindings.
	// It reports the duration elapsed since the call to StartTimer.
	ResetAndClear() (time.Duration, error)
	// Finalize is sqlite3_finalize.
	// https://sqlite.org/c3ref/finalize.html
	Finalize() error
	// ClearBindings sqlite3_clear_bindings.
	//
	// https://www.sqlite.org/c3ref/clear_bindings.html
	ClearBindings() error
	// Step is sqlite3_step.
	//
	// For SQLITE_ROW, Step returns (true, nil).
	// For SQLITE_DONE, Step returns (false, nil).
	// For any error, Step returns (false, err).
	//
	// As an optional optimization, if colType is provided, up to len(colType)
	// bytes are populated with the corresponding ColumnType values. If nil or
	// zero length, or the result is other than (true, nil), it's not used.
	// If the slice is too short (len is less than the number of columns), then
	// the results are truncated.
	//
	// https://www.sqlite.org/c3ref/step.html
	Step(colType []ColumnType) (row bool, err error)
	// StepResult executes a one-row query and resets the statment:
	//	sqlite3_step
	//	sqlite3_last_insert_rowid + sqlite3_changes
	//	sqlite3_reset + sqlite3_clear_bindings
	// Results:
	// 	For SQLITE_ROW, Step returns row=true err=nil
	// 	For SQLITE_DONE, Step returns row=false err=nil
	// 	For any error, Step returns row=false err.
	// https://www.sqlite.org/c3ref/step.html
	StepResult() (row bool, lastInsertRowID, changes int64, d time.Duration, err error)
	// BindDouble is sqlite3_bind_double.
	// https://sqlite.org/c3ref/bind_blob.html
	BindDouble(col int, val float64) error
	// BindInt64 is sqlite3_bind_int64.
	// https://sqlite.org/c3ref/bind_blob.html
	BindInt64(col int, val int64) error
	// BindNull is sqlite3_bind_null.
	// https://sqlite.org/c3ref/bind_blob.html
	BindNull(col int) error
	// BindText64 is sqlite3_bind_text64.
	// https://sqlite.org/c3ref/bind_blob.html
	BindText64(col int, val string) error
	// BindZeroBlob64 is sqlite3_bind_zeroblob64.
	// https://sqlite.org/c3ref/bind_blob.html
	BindZeroBlob64(col int, n uint64) error
	// BindBlob64 is sqlite3_bind_blob64.
	// https://sqlite.org/c3ref/bind_blob.html
	BindBlob64(col int, val []byte) error
	// BindParameterCount is sqlite3_bind_parameter_count.
	// https://sqlite.org/c3ref/bind_parameter_count.html
	BindParameterCount() int
	// BindParameterName is sqlite3_bind_parameter_name.
	// https://sqlite.org/c3ref/bind_parameter_count.html
	BindParameterName(col int) string
	// BindParameterIndex is sqlite3_bind_parameter_index.
	// Returns zero if no matching parameter is found.
	// https://sqlite.org/c3ref/bind_parameter_index.html
	BindParameterIndex(name string) int
	// BindParameterIndexSearch calls sqlite3_bind_parameter_index,
	// prepending ':', '@', and '?' until it finds a matching paramter.
	BindParameterIndexSearch(name string) int
	// ColumnCount is sqlite3_column_count.
	// https://sqlite.org/c3ref/column_count.html
	ColumnCount() int
	// ColumnName is sqlite3_column_name.
	// https://sqlite.org/c3ref/column_name.html
	ColumnName(col int) string
	// ColumnText is sqlite3_column_text.
	// https://sqlite.org/c3ref/column_blob.html
	ColumnText(col int) string
	// ColumnBlob is sqlite3_column_blob.
	//
	// WARNING: The returned memory is managed by C and is only valid until
	//          another call is made on this Stmt.
	//
	// https://sqlite.org/c3ref/column_blob.html
	ColumnBlob(col int) []byte
	// ColumnDouble is sqlite3_column_double.
	// https://sqlite.org/c3ref/column_blob.html
	ColumnDouble(col int) float64
	// ColumnInt64 is sqlite3_column_int64.
	// https://sqlite.org/c3ref/column_blob.html
	ColumnInt64(col int) int64
	// ColumnType is sqlite3_column_type.
	// https://www.sqlite.org/c3ref/column_blob.html
	ColumnType(col int) ColumnType
	// ColumnDeclType is sqlite3_column_decltype.
	// https://sqlite.org/c3ref/column_decltype.html
	ColumnDeclType(col int) string
	// ColumnDatabaseName is sqlite3_column_database_name.
	// https://sqlite.org/c3ref/column_database_name.html
	ColumnDatabaseName(col int) string
	// ColumnTableName is sqlite3_column_table_name.
	// https://sqlite.org/c3ref/column_database_name.html
	ColumnTableName(col int) string
}

// ColumnType are constants for each of the SQLite datatypes.
// https://www.sqlite.org/c3ref/c_blob.html
type ColumnType byte

const (
	SQLITE_INTEGER ColumnType = 1
	SQLITE_FLOAT   ColumnType = 2
	SQLITE_TEXT    ColumnType = 3
	SQLITE_BLOB    ColumnType = 4
	SQLITE_NULL    ColumnType = 5
)

func (t ColumnType) String() string {
	switch t {
	case SQLITE_INTEGER:
		return "SQLITE_INTEGER"
	case SQLITE_FLOAT:
		return "SQLITE_FLOAT"
	case SQLITE_TEXT:
		return "SQLITE_TEXT"
	case SQLITE_BLOB:
		return "SQLITE_BLOB"
	case SQLITE_NULL:
		return "SQLITE_NULL"
	default:
		return "UNKNOWN_SQLITE_DATATYPE"
	}
}

// https://www.sqlite.org/c3ref/c_prepare_normalize.html
type PrepareFlags int

const (
	SQLITE_PREPARE_PERSISTENT PrepareFlags = 0x01
	SQLITE_PREPARE_NORMALIZE  PrepareFlags = 0x02
	SQLITE_PREPARE_NO_VTAB    PrepareFlags = 0x04
)

// https://sqlite.org/c3ref/c_dbconfig_defensive.html
type DBConfig int

const (
	SQLITE_DBCONFIG_MAINDBNAME            DBConfig = 1000
	SQLITE_DBCONFIG_LOOKASIDE             DBConfig = 1001
	SQLITE_DBCONFIG_ENABLE_FKEY           DBConfig = 1002
	SQLITE_DBCONFIG_ENABLE_TRIGGER        DBConfig = 1003
	SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER DBConfig = 1004
	SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION DBConfig = 1005
	SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE      DBConfig = 1006
	SQLITE_DBCONFIG_ENABLE_QPSG           DBConfig = 1007
	SQLITE_DBCONFIG_TRIGGER_EQP           DBConfig = 1008
	SQLITE_DBCONFIG_RESET_DATABASE        DBConfig = 1009
	SQLITE_DBCONFIG_DEFENSIVE             DBConfig = 1010
	SQLITE_DBCONFIG_WRITABLE_SCHEMA       DBConfig = 1011
	SQLITE_DBCONFIG_LEGACY_ALTER_TABLE    DBConfig = 1012
	SQLITE_DBCONFIG_DQS_DML               DBConfig = 1013
	SQLITE_DBCONFIG_DQS_DDL               DBConfig = 1014
	SQLITE_DBCONFIG_ENABLE_VIEW           DBConfig = 1015
	SQLITE_DBCONFIG_LEGACY_FILE_FORMAT    DBConfig = 1016
	SQLITE_DBCONFIG_TRUSTED_SCHEMA        DBConfig = 1017
)

// OpenFlags are flags used when opening a DB.
//
// https://www.sqlite.org/c3ref/c_open_autoproxy.html
type OpenFlags int

const (
	SQLITE_OPEN_READONLY       OpenFlags = 0x00000001
	SQLITE_OPEN_READWRITE      OpenFlags = 0x00000002
	SQLITE_OPEN_CREATE         OpenFlags = 0x00000004
	SQLITE_OPEN_DELETEONCLOSE  OpenFlags = 0x00000008
	SQLITE_OPEN_EXCLUSIVE      OpenFlags = 0x00000010
	SQLITE_OPEN_AUTOPROXY      OpenFlags = 0x00000020
	SQLITE_OPEN_URI            OpenFlags = 0x00000040
	SQLITE_OPEN_MEMORY         OpenFlags = 0x00000080
	SQLITE_OPEN_MAIN_DB        OpenFlags = 0x00000100
	SQLITE_OPEN_TEMP_DB        OpenFlags = 0x00000200
	SQLITE_OPEN_TRANSIENT_DB   OpenFlags = 0x00000400
	SQLITE_OPEN_MAIN_JOURNAL   OpenFlags = 0x00000800
	SQLITE_OPEN_TEMP_JOURNAL   OpenFlags = 0x00001000
	SQLITE_OPEN_SUBJOURNAL     OpenFlags = 0x00002000
	SQLITE_OPEN_MASTER_JOURNAL OpenFlags = 0x00004000
	SQLITE_OPEN_NOMUTEX        OpenFlags = 0x00008000
	SQLITE_OPEN_FULLMUTEX      OpenFlags = 0x00010000
	SQLITE_OPEN_SHAREDCACHE    OpenFlags = 0x00020000
	SQLITE_OPEN_PRIVATECACHE   OpenFlags = 0x00040000
	SQLITE_OPEN_WAL            OpenFlags = 0x00080000
	SQLITE_OPEN_NOFOLLOW       OpenFlags = 0x00100000

	OpenFlagsDefault = SQLITE_OPEN_READWRITE |
		SQLITE_OPEN_CREATE |
		SQLITE_OPEN_WAL |
		SQLITE_OPEN_URI |
		SQLITE_OPEN_NOMUTEX
)

var allOpenFlags = []OpenFlags{
	SQLITE_OPEN_READONLY,
	SQLITE_OPEN_READWRITE,
	SQLITE_OPEN_CREATE,
	SQLITE_OPEN_DELETEONCLOSE,
	SQLITE_OPEN_EXCLUSIVE,
	SQLITE_OPEN_AUTOPROXY,
	SQLITE_OPEN_URI,
	SQLITE_OPEN_MEMORY,
	SQLITE_OPEN_MAIN_DB,
	SQLITE_OPEN_TEMP_DB,
	SQLITE_OPEN_TRANSIENT_DB,
	SQLITE_OPEN_MAIN_JOURNAL,
	SQLITE_OPEN_TEMP_JOURNAL,
	SQLITE_OPEN_SUBJOURNAL,
	SQLITE_OPEN_MASTER_JOURNAL,
	SQLITE_OPEN_NOMUTEX,
	SQLITE_OPEN_FULLMUTEX,
	SQLITE_OPEN_SHAREDCACHE,
	SQLITE_OPEN_PRIVATECACHE,
	SQLITE_OPEN_WAL,
	SQLITE_OPEN_NOFOLLOW,
}

var openFlagsStrings = map[OpenFlags]string{
	SQLITE_OPEN_READONLY:       "SQLITE_OPEN_READONLY",
	SQLITE_OPEN_READWRITE:      "SQLITE_OPEN_READWRITE",
	SQLITE_OPEN_CREATE:         "SQLITE_OPEN_CREATE",
	SQLITE_OPEN_DELETEONCLOSE:  "SQLITE_OPEN_DELETEONCLOSE",
	SQLITE_OPEN_EXCLUSIVE:      "SQLITE_OPEN_EXCLUSIVE",
	SQLITE_OPEN_AUTOPROXY:      "SQLITE_OPEN_AUTOPROXY",
	SQLITE_OPEN_URI:            "SQLITE_OPEN_URI",
	SQLITE_OPEN_MEMORY:         "SQLITE_OPEN_MEMORY",
	SQLITE_OPEN_MAIN_DB:        "SQLITE_OPEN_MAIN_DB",
	SQLITE_OPEN_TEMP_DB:        "SQLITE_OPEN_TEMP_DB",
	SQLITE_OPEN_TRANSIENT_DB:   "SQLITE_OPEN_TRANSIENT_DB",
	SQLITE_OPEN_MAIN_JOURNAL:   "SQLITE_OPEN_MAIN_JOURNAL",
	SQLITE_OPEN_TEMP_JOURNAL:   "SQLITE_OPEN_TEMP_JOURNAL",
	SQLITE_OPEN_SUBJOURNAL:     "SQLITE_OPEN_SUBJOURNAL",
	SQLITE_OPEN_MASTER_JOURNAL: "SQLITE_OPEN_MASTER_JOURNAL",
	SQLITE_OPEN_NOMUTEX:        "SQLITE_OPEN_NOMUTEX",
	SQLITE_OPEN_FULLMUTEX:      "SQLITE_OPEN_FULLMUTEX",
	SQLITE_OPEN_SHAREDCACHE:    "SQLITE_OPEN_SHAREDCACHE",
	SQLITE_OPEN_PRIVATECACHE:   "SQLITE_OPEN_PRIVATECACHE",
	SQLITE_OPEN_WAL:            "SQLITE_OPEN_WAL",
	SQLITE_OPEN_NOFOLLOW:       "SQLITE_OPEN_NOFOLLOW",
}

func (o OpenFlags) String() string {
	var flags []byte
	for i, flag := range allOpenFlags {
		if o&flag == 0 {
			continue
		}
		if i > 0 {
			flags = append(flags, '|')
		}
		flagStr, ok := openFlagsStrings[flag]
		if ok {
			flags = append(flags, flagStr...)
		} else {
			flags = append(flags, "UNKNOWN_FLAG:"...)
			var buf [20]byte
			flags = append(flags, itoa(buf[:], int64(flag))...)
		}
	}
	return string(flags)
}

// Checkpoint is a WAL checkpoint mode.
// It is used by sqlite3_wal_checkpoint_v2.
//
// https://sqlite.org/c3ref/wal_checkpoint_v2.html
type Checkpoint int

const (
	SQLITE_CHECKPOINT_PASSIVE  Checkpoint = 0
	SQLITE_CHECKPOINT_FULL     Checkpoint = 1
	SQLITE_CHECKPOINT_RESTART  Checkpoint = 2
	SQLITE_CHECKPOINT_TRUNCATE Checkpoint = 3
)

func (mode Checkpoint) String() string {
	switch mode {
	default:
		var buf [20]byte
		return "SQLITE_CHECKPOINT_UNKNOWN(" + string(itoa(buf[:], int64(mode))) + ")"
	case SQLITE_CHECKPOINT_PASSIVE:
		return "SQLITE_CHECKPOINT_PASSIVE"
	case SQLITE_CHECKPOINT_FULL:
		return "SQLITE_CHECKPOINT_FULL"
	case SQLITE_CHECKPOINT_RESTART:
		return "SQLITE_CHECKPOINT_RESTART"
	case SQLITE_CHECKPOINT_TRUNCATE:
		return "SQLITE_CHECKPOINT_TRUNCATE"
	}
}

// TxnState is a transaction state.
// It is used by sqlite3_txn_state.
//
// https://sqlite.org/c3ref/txn_state.html
type TxnState int

const (
	SQLITE_TXN_NONE  TxnState = 0
	SQLITE_TXN_READ  TxnState = 1
	SQLITE_TXN_WRITE TxnState = 2
)

func (state TxnState) String() string {
	switch state {
	default:
		var buf [20]byte
		return "SQLITE_TXN_UNKNOWN(" + string(itoa(buf[:], int64(state))) + ")"
	case SQLITE_TXN_NONE:
		return "SQLITE_TXN_NONE"
	case SQLITE_TXN_READ:
		return "SQLITE_TXN_READ"
	case SQLITE_TXN_WRITE:
		return "SQLITE_TXN_WRITE"
	}
}

// ErrCode is an SQLite error code as a Go error.
// It must not be one of the status codes SQLITE_OK, SQLITE_ROW, or SQLITE_DONE.
type ErrCode Code

func (e ErrCode) Error() string {
	return Code(e).String()
}

// Code is an SQLite extended error code.
//
// The three SQLite result codes (SQLITE_OK, SQLITE_ROW, and SQLITE_DONE),
// are not errors so they should not be used in an Error.
type Code int

func (code Code) String() string {
	switch code {
	default:
		var buf [20]byte
		return "SQLITE_UNKNOWN_ERR(" + string(itoa(buf[:], int64(code))) + ")"
	case SQLITE_OK:
		return "SQLITE_OK(not an error)"
	case SQLITE_ROW:
		return "SQLITE_ROW(not an error)"
	case SQLITE_DONE:
		return "SQLITE_DONE(not an error)"
	case SQLITE_ERROR:
		return "SQLITE_ERROR"
	case SQLITE_INTERNAL:
		return "SQLITE_INTERNAL"
	case SQLITE_PERM:
		return "SQLITE_PERM"
	case SQLITE_ABORT:
		return "SQLITE_ABORT"
	case SQLITE_BUSY:
		return "SQLITE_BUSY"
	case SQLITE_LOCKED:
		return "SQLITE_LOCKED"
	case SQLITE_NOMEM:
		return "SQLITE_NOMEM"
	case SQLITE_READONLY:
		return "SQLITE_READONLY"
	case SQLITE_INTERRUPT:
		return "SQLITE_INTERRUPT"
	case SQLITE_IOERR:
		return "SQLITE_IOERR"
	case SQLITE_CORRUPT:
		return "SQLITE_CORRUPT"
	case SQLITE_NOTFOUND:
		return "SQLITE_NOTFOUND"
	case SQLITE_FULL:
		return "SQLITE_FULL"
	case SQLITE_CANTOPEN:
		return "SQLITE_CANTOPEN"
	case SQLITE_PROTOCOL:
		return "SQLITE_PROTOCOL"
	case SQLITE_EMPTY:
		return "SQLITE_EMPTY"
	case SQLITE_SCHEMA:
		return "SQLITE_SCHEMA"
	case SQLITE_TOOBIG:
		return "SQLITE_TOOBIG"
	case SQLITE_CONSTRAINT:
		return "SQLITE_CONSTRAINT"
	case SQLITE_MISMATCH:
		return "SQLITE_MISMATCH"
	case SQLITE_MISUSE:
		return "SQLITE_MISUSE"
	case SQLITE_NOLFS:
		return "SQLITE_NOLFS"
	case SQLITE_AUTH:
		return "SQLITE_AUTH"
	case SQLITE_FORMAT:
		return "SQLITE_FORMAT"
	case SQLITE_RANGE:
		return "SQLITE_RANGE"
	case SQLITE_NOTADB:
		return "SQLITE_NOTADB"
	case SQLITE_NOTICE:
		return "SQLITE_NOTICE"
	case SQLITE_WARNING:
		return "SQLITE_WARNING"

	case SQLITE_ERROR_MISSING_COLLSEQ:
		return "SQLITE_ERROR_MISSING_COLLSEQ"
	case SQLITE_ERROR_RETRY:
		return "SQLITE_ERROR_RETRY"
	case SQLITE_ERROR_SNAPSHOT:
		return "SQLITE_ERROR_SNAPSHOT"
	case SQLITE_IOERR_READ:
		return "SQLITE_IOERR_READ"
	case SQLITE_IOERR_SHORT_READ:
		return "SQLITE_IOERR_SHORT_READ"
	case SQLITE_IOERR_WRITE:
		return "SQLITE_IOERR_WRITE"
	case SQLITE_IOERR_FSYNC:
		return "SQLITE_IOERR_FSYNC"
	case SQLITE_IOERR_DIR_FSYNC:
		return "SQLITE_IOERR_DIR_FSYNC"
	case SQLITE_IOERR_TRUNCATE:
		return "SQLITE_IOERR_TRUNCATE"
	case SQLITE_IOERR_FSTAT:
		return "SQLITE_IOERR_FSTAT"
	case SQLITE_IOERR_UNLOCK:
		return "SQLITE_IOERR_UNLOCK"
	case SQLITE_IOERR_RDLOCK:
		return "SQLITE_IOERR_RDLOCK"
	case SQLITE_IOERR_DELETE:
		return "SQLITE_IOERR_DELETE"
	case SQLITE_IOERR_BLOCKED:
		return "SQLITE_IOERR_BLOCKED"
	case SQLITE_IOERR_NOMEM:
		return "SQLITE_IOERR_NOMEM"
	case SQLITE_IOERR_ACCESS:
		return "SQLITE_IOERR_ACCESS"
	case SQLITE_IOERR_CHECKRESERVEDLOCK:
		return "SQLITE_IOERR_CHECKRESERVEDLOCK"
	case SQLITE_IOERR_LOCK:
		return "SQLITE_IOERR_LOCK"
	case SQLITE_IOERR_CLOSE:
		return "SQLITE_IOERR_CLOSE"
	case SQLITE_IOERR_DIR_CLOSE:
		return "SQLITE_IOERR_DIR_CLOSE"
	case SQLITE_IOERR_SHMOPEN:
		return "SQLITE_IOERR_SHMOPEN"
	case SQLITE_IOERR_SHMSIZE:
		return "SQLITE_IOERR_SHMSIZE"
	case SQLITE_IOERR_SHMLOCK:
		return "SQLITE_IOERR_SHMLOCK"
	case SQLITE_IOERR_SHMMAP:
		return "SQLITE_IOERR_SHMMAP"
	case SQLITE_IOERR_SEEK:
		return "SQLITE_IOERR_SEEK"
	case SQLITE_IOERR_DELETE_NOENT:
		return "SQLITE_IOERR_DELETE_NOENT"
	case SQLITE_IOERR_MMAP:
		return "SQLITE_IOERR_MMAP"
	case SQLITE_IOERR_GETTEMPPATH:
		return "SQLITE_IOERR_GETTEMPPATH"
	case SQLITE_IOERR_CONVPATH:
		return "SQLITE_IOERR_CONVPATH"
	case SQLITE_IOERR_VNODE:
		return "SQLITE_IOERR_VNODE"
	case SQLITE_IOERR_AUTH:
		return "SQLITE_IOERR_AUTH"
	case SQLITE_IOERR_BEGIN_ATOMIC:
		return "SQLITE_IOERR_BEGIN_ATOMIC"
	case SQLITE_IOERR_COMMIT_ATOMIC:
		return "SQLITE_IOERR_COMMIT_ATOMIC"
	case SQLITE_IOERR_ROLLBACK_ATOMIC:
		return "SQLITE_IOERR_ROLLBACK_ATOMIC"
	case SQLITE_LOCKED_SHAREDCACHE:
		return "SQLITE_LOCKED_SHAREDCACHE"
	case SQLITE_BUSY_RECOVERY:
		return "SQLITE_BUSY_RECOVERY"
	case SQLITE_BUSY_SNAPSHOT:
		return "SQLITE_BUSY_SNAPSHOT"
	case SQLITE_CANTOPEN_NOTEMPDIR:
		return "SQLITE_CANTOPEN_NOTEMPDIR"
	case SQLITE_CANTOPEN_ISDIR:
		return "SQLITE_CANTOPEN_ISDIR"
	case SQLITE_CANTOPEN_FULLPATH:
		return "SQLITE_CANTOPEN_FULLPATH"
	case SQLITE_CANTOPEN_CONVPATH:
		return "SQLITE_CANTOPEN_CONVPATH"
	case SQLITE_CORRUPT_VTAB:
		return "SQLITE_CORRUPT_VTAB"
	case SQLITE_READONLY_RECOVERY:
		return "SQLITE_READONLY_RECOVERY"
	case SQLITE_READONLY_CANTLOCK:
		return "SQLITE_READONLY_CANTLOCK"
	case SQLITE_READONLY_ROLLBACK:
		return "SQLITE_READONLY_ROLLBACK"
	case SQLITE_READONLY_DBMOVED:
		return "SQLITE_READONLY_DBMOVED"
	case SQLITE_READONLY_CANTINIT:
		return "SQLITE_READONLY_CANTINIT"
	case SQLITE_READONLY_DIRECTORY:
		return "SQLITE_READONLY_DIRECTORY"
	case SQLITE_ABORT_ROLLBACK:
		return "SQLITE_ABORT_ROLLBACK"
	case SQLITE_CONSTRAINT_CHECK:
		return "SQLITE_CONSTRAINT_CHECK"
	case SQLITE_CONSTRAINT_COMMITHOOK:
		return "SQLITE_CONSTRAINT_COMMITHOOK"
	case SQLITE_CONSTRAINT_FOREIGNKEY:
		return "SQLITE_CONSTRAINT_FOREIGNKEY"
	case SQLITE_CONSTRAINT_FUNCTION:
		return "SQLITE_CONSTRAINT_FUNCTION"
	case SQLITE_CONSTRAINT_NOTNULL:
		return "SQLITE_CONSTRAINT_NOTNULL"
	case SQLITE_CONSTRAINT_PRIMARYKEY:
		return "SQLITE_CONSTRAINT_PRIMARYKEY"
	case SQLITE_CONSTRAINT_TRIGGER:
		return "SQLITE_CONSTRAINT_TRIGGER"
	case SQLITE_CONSTRAINT_UNIQUE:
		return "SQLITE_CONSTRAINT_UNIQUE"
	case SQLITE_CONSTRAINT_VTAB:
		return "SQLITE_CONSTRAINT_VTAB"
	case SQLITE_CONSTRAINT_ROWID:
		return "SQLITE_CONSTRAINT_ROWID"
	case SQLITE_NOTICE_RECOVER_WAL:
		return "SQLITE_NOTICE_RECOVER_WAL"
	case SQLITE_NOTICE_RECOVER_ROLLBACK:
		return "SQLITE_NOTICE_RECOVER_ROLLBACK"
	case SQLITE_WARNING_AUTOINDEX:
		return "SQLITE_WARNING_AUTOINDEX"
	case SQLITE_AUTH_USER:
		return "SQLITE_AUTH_USER"
	}
}

const (
	SQLITE_OK         = Code(0) // do not use in Error
	SQLITE_ERROR      = Code(1)
	SQLITE_INTERNAL   = Code(2)
	SQLITE_PERM       = Code(3)
	SQLITE_ABORT      = Code(4)
	SQLITE_BUSY       = Code(5)
	SQLITE_LOCKED     = Code(6)
	SQLITE_NOMEM      = Code(7)
	SQLITE_READONLY   = Code(8)
	SQLITE_INTERRUPT  = Code(9)
	SQLITE_IOERR      = Code(10)
	SQLITE_CORRUPT    = Code(11)
	SQLITE_NOTFOUND   = Code(12)
	SQLITE_FULL       = Code(13)
	SQLITE_CANTOPEN   = Code(14)
	SQLITE_PROTOCOL   = Code(15)
	SQLITE_EMPTY      = Code(16)
	SQLITE_SCHEMA     = Code(17)
	SQLITE_TOOBIG     = Code(18)
	SQLITE_CONSTRAINT = Code(19)
	SQLITE_MISMATCH   = Code(20)
	SQLITE_MISUSE     = Code(21)
	SQLITE_NOLFS      = Code(22)
	SQLITE_AUTH       = Code(23)
	SQLITE_FORMAT     = Code(24)
	SQLITE_RANGE      = Code(25)
	SQLITE_NOTADB     = Code(26)
	SQLITE_NOTICE     = Code(27)
	SQLITE_WARNING    = Code(28)
	SQLITE_ROW        = Code(100) // do not use in Error
	SQLITE_DONE       = Code(101) // do not use in Error

	// Extended error codes

	SQLITE_ERROR_MISSING_COLLSEQ   = Code(SQLITE_ERROR | (1 << 8))
	SQLITE_ERROR_RETRY             = Code(SQLITE_ERROR | (2 << 8))
	SQLITE_ERROR_SNAPSHOT          = Code(SQLITE_ERROR | (3 << 8))
	SQLITE_IOERR_READ              = Code(SQLITE_IOERR | (1 << 8))
	SQLITE_IOERR_SHORT_READ        = Code(SQLITE_IOERR | (2 << 8))
	SQLITE_IOERR_WRITE             = Code(SQLITE_IOERR | (3 << 8))
	SQLITE_IOERR_FSYNC             = Code(SQLITE_IOERR | (4 << 8))
	SQLITE_IOERR_DIR_FSYNC         = Code(SQLITE_IOERR | (5 << 8))
	SQLITE_IOERR_TRUNCATE          = Code(SQLITE_IOERR | (6 << 8))
	SQLITE_IOERR_FSTAT             = Code(SQLITE_IOERR | (7 << 8))
	SQLITE_IOERR_UNLOCK            = Code(SQLITE_IOERR | (8 << 8))
	SQLITE_IOERR_RDLOCK            = Code(SQLITE_IOERR | (9 << 8))
	SQLITE_IOERR_DELETE            = Code(SQLITE_IOERR | (10 << 8))
	SQLITE_IOERR_BLOCKED           = Code(SQLITE_IOERR | (11 << 8))
	SQLITE_IOERR_NOMEM             = Code(SQLITE_IOERR | (12 << 8))
	SQLITE_IOERR_ACCESS            = Code(SQLITE_IOERR | (13 << 8))
	SQLITE_IOERR_CHECKRESERVEDLOCK = Code(SQLITE_IOERR | (14 << 8))
	SQLITE_IOERR_LOCK              = Code(SQLITE_IOERR | (15 << 8))
	SQLITE_IOERR_CLOSE             = Code(SQLITE_IOERR | (16 << 8))
	SQLITE_IOERR_DIR_CLOSE         = Code(SQLITE_IOERR | (17 << 8))
	SQLITE_IOERR_SHMOPEN           = Code(SQLITE_IOERR | (18 << 8))
	SQLITE_IOERR_SHMSIZE           = Code(SQLITE_IOERR | (19 << 8))
	SQLITE_IOERR_SHMLOCK           = Code(SQLITE_IOERR | (20 << 8))
	SQLITE_IOERR_SHMMAP            = Code(SQLITE_IOERR | (21 << 8))
	SQLITE_IOERR_SEEK              = Code(SQLITE_IOERR | (22 << 8))
	SQLITE_IOERR_DELETE_NOENT      = Code(SQLITE_IOERR | (23 << 8))
	SQLITE_IOERR_MMAP              = Code(SQLITE_IOERR | (24 << 8))
	SQLITE_IOERR_GETTEMPPATH       = Code(SQLITE_IOERR | (25 << 8))
	SQLITE_IOERR_CONVPATH          = Code(SQLITE_IOERR | (26 << 8))
	SQLITE_IOERR_VNODE             = Code(SQLITE_IOERR | (27 << 8))
	SQLITE_IOERR_AUTH              = Code(SQLITE_IOERR | (28 << 8))
	SQLITE_IOERR_BEGIN_ATOMIC      = Code(SQLITE_IOERR | (29 << 8))
	SQLITE_IOERR_COMMIT_ATOMIC     = Code(SQLITE_IOERR | (30 << 8))
	SQLITE_IOERR_ROLLBACK_ATOMIC   = Code(SQLITE_IOERR | (31 << 8))
	SQLITE_IOERR_DATA              = Code(SQLITE_IOERR | (32 << 8))
	SQLITE_IOERR_CORRUPTFS         = Code(SQLITE_IOERR | (33 << 8))
	SQLITE_LOCKED_SHAREDCACHE      = Code(SQLITE_LOCKED | (1 << 8))
	SQLITE_LOCKED_VTAB             = Code(SQLITE_LOCKED | (2 << 8))
	SQLITE_BUSY_RECOVERY           = Code(SQLITE_BUSY | (1 << 8))
	SQLITE_BUSY_SNAPSHOT           = Code(SQLITE_BUSY | (2 << 8))
	SQLITE_BUSY_TIMEOUT            = Code(SQLITE_BUSY | (3 << 8))
	SQLITE_CANTOPEN_NOTEMPDIR      = Code(SQLITE_CANTOPEN | (1 << 8))
	SQLITE_CANTOPEN_ISDIR          = Code(SQLITE_CANTOPEN | (2 << 8))
	SQLITE_CANTOPEN_FULLPATH       = Code(SQLITE_CANTOPEN | (3 << 8))
	SQLITE_CANTOPEN_CONVPATH       = Code(SQLITE_CANTOPEN | (4 << 8))
	SQLITE_CANTOPEN_DIRTYWAL       = Code(SQLITE_CANTOPEN | (5 << 8)) /* Not Used */
	SQLITE_CANTOPEN_SYMLINK        = Code(SQLITE_CANTOPEN | (6 << 8))
	SQLITE_CORRUPT_VTAB            = Code(SQLITE_CORRUPT | (1 << 8))
	SQLITE_CORRUPT_SEQUENCE        = Code(SQLITE_CORRUPT | (2 << 8))
	SQLITE_CORRUPT_INDEX           = Code(SQLITE_CORRUPT | (3 << 8))
	SQLITE_READONLY_RECOVERY       = Code(SQLITE_READONLY | (1 << 8))
	SQLITE_READONLY_CANTLOCK       = Code(SQLITE_READONLY | (2 << 8))
	SQLITE_READONLY_ROLLBACK       = Code(SQLITE_READONLY | (3 << 8))
	SQLITE_READONLY_DBMOVED        = Code(SQLITE_READONLY | (4 << 8))
	SQLITE_READONLY_CANTINIT       = Code(SQLITE_READONLY | (5 << 8))
	SQLITE_READONLY_DIRECTORY      = Code(SQLITE_READONLY | (6 << 8))
	SQLITE_ABORT_ROLLBACK          = Code(SQLITE_ABORT | (2 << 8))
	SQLITE_CONSTRAINT_CHECK        = Code(SQLITE_CONSTRAINT | (1 << 8))
	SQLITE_CONSTRAINT_COMMITHOOK   = Code(SQLITE_CONSTRAINT | (2 << 8))
	SQLITE_CONSTRAINT_FOREIGNKEY   = Code(SQLITE_CONSTRAINT | (3 << 8))
	SQLITE_CONSTRAINT_FUNCTION     = Code(SQLITE_CONSTRAINT | (4 << 8))
	SQLITE_CONSTRAINT_NOTNULL      = Code(SQLITE_CONSTRAINT | (5 << 8))
	SQLITE_CONSTRAINT_PRIMARYKEY   = Code(SQLITE_CONSTRAINT | (6 << 8))
	SQLITE_CONSTRAINT_TRIGGER      = Code(SQLITE_CONSTRAINT | (7 << 8))
	SQLITE_CONSTRAINT_UNIQUE       = Code(SQLITE_CONSTRAINT | (8 << 8))
	SQLITE_CONSTRAINT_VTAB         = Code(SQLITE_CONSTRAINT | (9 << 8))
	SQLITE_CONSTRAINT_ROWID        = Code(SQLITE_CONSTRAINT | (10 << 8))
	SQLITE_CONSTRAINT_PINNED       = Code(SQLITE_CONSTRAINT | (11 << 8))
	SQLITE_NOTICE_RECOVER_WAL      = Code(SQLITE_NOTICE | (1 << 8))
	SQLITE_NOTICE_RECOVER_ROLLBACK = Code(SQLITE_NOTICE | (2 << 8))
	SQLITE_WARNING_AUTOINDEX       = Code(SQLITE_WARNING | (1 << 8))
	SQLITE_AUTH_USER               = Code(SQLITE_AUTH | (1 << 8))
	SQLITE_OK_LOAD_PERMANENTLY     = Code(SQLITE_OK | (1 << 8))
	SQLITE_OK_SYMLINK              = Code(SQLITE_OK | (2 << 8))
)

// CodeAsError is used to intern Codes into ErrCodes.
// SQLite non-error status codes return nil.
func CodeAsError(code Code) error {
	if code == SQLITE_OK || code == SQLITE_ROW || code == SQLITE_DONE {
		return nil
	}
	codeAsErrorInitOnce.Do(codeAsErrorInit)
	err := codeAsError[code]
	if err == nil {
		return ErrCode(code)
	}
	return err
}

var codeAsError map[Code]error

var codeAsErrorInitOnce sync.Once

func codeAsErrorInit() {
	codeAsError = map[Code]error{
		SQLITE_ERROR:                   ErrCode(SQLITE_ERROR),
		SQLITE_INTERNAL:                ErrCode(SQLITE_INTERNAL),
		SQLITE_PERM:                    ErrCode(SQLITE_PERM),
		SQLITE_ABORT:                   ErrCode(SQLITE_ABORT),
		SQLITE_BUSY:                    ErrCode(SQLITE_BUSY),
		SQLITE_LOCKED:                  ErrCode(SQLITE_LOCKED),
		SQLITE_NOMEM:                   ErrCode(SQLITE_NOMEM),
		SQLITE_READONLY:                ErrCode(SQLITE_READONLY),
		SQLITE_INTERRUPT:               ErrCode(SQLITE_INTERRUPT),
		SQLITE_IOERR:                   ErrCode(SQLITE_IOERR),
		SQLITE_CORRUPT:                 ErrCode(SQLITE_CORRUPT),
		SQLITE_NOTFOUND:                ErrCode(SQLITE_NOTFOUND),
		SQLITE_FULL:                    ErrCode(SQLITE_FULL),
		SQLITE_CANTOPEN:                ErrCode(SQLITE_CANTOPEN),
		SQLITE_PROTOCOL:                ErrCode(SQLITE_PROTOCOL),
		SQLITE_EMPTY:                   ErrCode(SQLITE_EMPTY),
		SQLITE_SCHEMA:                  ErrCode(SQLITE_SCHEMA),
		SQLITE_TOOBIG:                  ErrCode(SQLITE_TOOBIG),
		SQLITE_CONSTRAINT:              ErrCode(SQLITE_CONSTRAINT),
		SQLITE_MISMATCH:                ErrCode(SQLITE_MISMATCH),
		SQLITE_MISUSE:                  ErrCode(SQLITE_MISUSE),
		SQLITE_NOLFS:                   ErrCode(SQLITE_NOLFS),
		SQLITE_AUTH:                    ErrCode(SQLITE_AUTH),
		SQLITE_FORMAT:                  ErrCode(SQLITE_FORMAT),
		SQLITE_RANGE:                   ErrCode(SQLITE_RANGE),
		SQLITE_NOTADB:                  ErrCode(SQLITE_NOTADB),
		SQLITE_NOTICE:                  ErrCode(SQLITE_NOTICE),
		SQLITE_WARNING:                 ErrCode(SQLITE_WARNING),
		SQLITE_ERROR_MISSING_COLLSEQ:   ErrCode(SQLITE_ERROR_MISSING_COLLSEQ),
		SQLITE_ERROR_RETRY:             ErrCode(SQLITE_ERROR_RETRY),
		SQLITE_ERROR_SNAPSHOT:          ErrCode(SQLITE_ERROR_SNAPSHOT),
		SQLITE_IOERR_READ:              ErrCode(SQLITE_IOERR_READ),
		SQLITE_IOERR_SHORT_READ:        ErrCode(SQLITE_IOERR_SHORT_READ),
		SQLITE_IOERR_WRITE:             ErrCode(SQLITE_IOERR_WRITE),
		SQLITE_IOERR_FSYNC:             ErrCode(SQLITE_IOERR_FSYNC),
		SQLITE_IOERR_DIR_FSYNC:         ErrCode(SQLITE_IOERR_DIR_FSYNC),
		SQLITE_IOERR_TRUNCATE:          ErrCode(SQLITE_IOERR_TRUNCATE),
		SQLITE_IOERR_FSTAT:             ErrCode(SQLITE_IOERR_FSTAT),
		SQLITE_IOERR_UNLOCK:            ErrCode(SQLITE_IOERR_UNLOCK),
		SQLITE_IOERR_RDLOCK:            ErrCode(SQLITE_IOERR_RDLOCK),
		SQLITE_IOERR_DELETE:            ErrCode(SQLITE_IOERR_DELETE),
		SQLITE_IOERR_BLOCKED:           ErrCode(SQLITE_IOERR_BLOCKED),
		SQLITE_IOERR_NOMEM:             ErrCode(SQLITE_IOERR_NOMEM),
		SQLITE_IOERR_ACCESS:            ErrCode(SQLITE_IOERR_ACCESS),
		SQLITE_IOERR_CHECKRESERVEDLOCK: ErrCode(SQLITE_IOERR_CHECKRESERVEDLOCK),
		SQLITE_IOERR_LOCK:              ErrCode(SQLITE_IOERR_LOCK),
		SQLITE_IOERR_CLOSE:             ErrCode(SQLITE_IOERR_CLOSE),
		SQLITE_IOERR_DIR_CLOSE:         ErrCode(SQLITE_IOERR_DIR_CLOSE),
		SQLITE_IOERR_SHMOPEN:           ErrCode(SQLITE_IOERR_SHMOPEN),
		SQLITE_IOERR_SHMSIZE:           ErrCode(SQLITE_IOERR_SHMSIZE),
		SQLITE_IOERR_SHMLOCK:           ErrCode(SQLITE_IOERR_SHMLOCK),
		SQLITE_IOERR_SHMMAP:            ErrCode(SQLITE_IOERR_SHMMAP),
		SQLITE_IOERR_SEEK:              ErrCode(SQLITE_IOERR_SEEK),
		SQLITE_IOERR_DELETE_NOENT:      ErrCode(SQLITE_IOERR_DELETE_NOENT),
		SQLITE_IOERR_MMAP:              ErrCode(SQLITE_IOERR_MMAP),
		SQLITE_IOERR_GETTEMPPATH:       ErrCode(SQLITE_IOERR_GETTEMPPATH),
		SQLITE_IOERR_CONVPATH:          ErrCode(SQLITE_IOERR_CONVPATH),
		SQLITE_IOERR_VNODE:             ErrCode(SQLITE_IOERR_VNODE),
		SQLITE_IOERR_AUTH:              ErrCode(SQLITE_IOERR_AUTH),
		SQLITE_IOERR_BEGIN_ATOMIC:      ErrCode(SQLITE_IOERR_BEGIN_ATOMIC),
		SQLITE_IOERR_COMMIT_ATOMIC:     ErrCode(SQLITE_IOERR_COMMIT_ATOMIC),
		SQLITE_IOERR_ROLLBACK_ATOMIC:   ErrCode(SQLITE_IOERR_ROLLBACK_ATOMIC),
		SQLITE_IOERR_DATA:              ErrCode(SQLITE_IOERR_DATA),
		SQLITE_IOERR_CORRUPTFS:         ErrCode(SQLITE_IOERR_CORRUPTFS),
		SQLITE_LOCKED_SHAREDCACHE:      ErrCode(SQLITE_LOCKED_SHAREDCACHE),
		SQLITE_LOCKED_VTAB:             ErrCode(SQLITE_LOCKED_VTAB),
		SQLITE_BUSY_RECOVERY:           ErrCode(SQLITE_BUSY_RECOVERY),
		SQLITE_BUSY_SNAPSHOT:           ErrCode(SQLITE_BUSY_SNAPSHOT),
		SQLITE_BUSY_TIMEOUT:            ErrCode(SQLITE_BUSY_TIMEOUT),
		SQLITE_CANTOPEN_NOTEMPDIR:      ErrCode(SQLITE_CANTOPEN_NOTEMPDIR),
		SQLITE_CANTOPEN_ISDIR:          ErrCode(SQLITE_CANTOPEN_ISDIR),
		SQLITE_CANTOPEN_FULLPATH:       ErrCode(SQLITE_CANTOPEN_FULLPATH),
		SQLITE_CANTOPEN_CONVPATH:       ErrCode(SQLITE_CANTOPEN_CONVPATH),
		SQLITE_CANTOPEN_DIRTYWAL:       ErrCode(SQLITE_CANTOPEN_DIRTYWAL),
		SQLITE_CANTOPEN_SYMLINK:        ErrCode(SQLITE_CANTOPEN_SYMLINK),
		SQLITE_CORRUPT_VTAB:            ErrCode(SQLITE_CORRUPT_VTAB),
		SQLITE_CORRUPT_SEQUENCE:        ErrCode(SQLITE_CORRUPT_SEQUENCE),
		SQLITE_CORRUPT_INDEX:           ErrCode(SQLITE_CORRUPT_INDEX),
		SQLITE_READONLY_RECOVERY:       ErrCode(SQLITE_READONLY_RECOVERY),
		SQLITE_READONLY_CANTLOCK:       ErrCode(SQLITE_READONLY_CANTLOCK),
		SQLITE_READONLY_ROLLBACK:       ErrCode(SQLITE_READONLY_ROLLBACK),
		SQLITE_READONLY_DBMOVED:        ErrCode(SQLITE_READONLY_DBMOVED),
		SQLITE_READONLY_CANTINIT:       ErrCode(SQLITE_READONLY_CANTINIT),
		SQLITE_READONLY_DIRECTORY:      ErrCode(SQLITE_READONLY_DIRECTORY),
		SQLITE_ABORT_ROLLBACK:          ErrCode(SQLITE_ABORT_ROLLBACK),
		SQLITE_CONSTRAINT_CHECK:        ErrCode(SQLITE_CONSTRAINT_CHECK),
		SQLITE_CONSTRAINT_COMMITHOOK:   ErrCode(SQLITE_CONSTRAINT_COMMITHOOK),
		SQLITE_CONSTRAINT_FOREIGNKEY:   ErrCode(SQLITE_CONSTRAINT_FOREIGNKEY),
		SQLITE_CONSTRAINT_FUNCTION:     ErrCode(SQLITE_CONSTRAINT_FUNCTION),
		SQLITE_CONSTRAINT_NOTNULL:      ErrCode(SQLITE_CONSTRAINT_NOTNULL),
		SQLITE_CONSTRAINT_PRIMARYKEY:   ErrCode(SQLITE_CONSTRAINT_PRIMARYKEY),
		SQLITE_CONSTRAINT_TRIGGER:      ErrCode(SQLITE_CONSTRAINT_TRIGGER),
		SQLITE_CONSTRAINT_UNIQUE:       ErrCode(SQLITE_CONSTRAINT_UNIQUE),
		SQLITE_CONSTRAINT_VTAB:         ErrCode(SQLITE_CONSTRAINT_VTAB),
		SQLITE_CONSTRAINT_ROWID:        ErrCode(SQLITE_CONSTRAINT_ROWID),
		SQLITE_CONSTRAINT_PINNED:       ErrCode(SQLITE_CONSTRAINT_PINNED),
		SQLITE_NOTICE_RECOVER_WAL:      ErrCode(SQLITE_NOTICE_RECOVER_WAL),
		SQLITE_NOTICE_RECOVER_ROLLBACK: ErrCode(SQLITE_NOTICE_RECOVER_ROLLBACK),
		SQLITE_WARNING_AUTOINDEX:       ErrCode(SQLITE_WARNING_AUTOINDEX),
		SQLITE_AUTH_USER:               ErrCode(SQLITE_AUTH_USER),
		SQLITE_OK_LOAD_PERMANENTLY:     ErrCode(SQLITE_OK_LOAD_PERMANENTLY),
		SQLITE_OK_SYMLINK:              ErrCode(SQLITE_OK_SYMLINK),
	}
}

func itoa(buf []byte, val int64) []byte {
	i := len(buf) - 1
	neg := false
	if val < 0 {
		neg = true
		val = 0 - val
	}
	for val >= 10 {
		buf[i] = byte(val%10 + '0')
		i--
		val /= 10
	}
	buf[i] = byte(val + '0')
	if neg {
		i--
		buf[i] = '-'
	}
	return buf[i:]
}

// TraceConnID uniquely identifies an SQLite connection in this process.
//
// It is provided to the Tracer to let it associate transaction events.
type TraceConnID int

// A Tracer traces use of an SQLite database connection.
//
// Each tracer method is provided with a TraceConnID, which is a stable
// identifier of the underlying sql connection that the event happened
// to, which can be used to collate events.
//
// Some tracer methods take a ctx which is the context object
// provided by the user to that method. This can be used by the tracer
// to plumb through context values.
//
// Any error that occurred executing the event is reported to the
// tracer in the err parameter. If err is not nil, the event failed.
type Tracer interface {
	// Query is called by the driver to report a completed query.
	//
	// The query string is the string the user provided to Prepare.
	// No parameters are filled in.
	//
	// The duration covers the complete execution time of the query,
	// including both time spent inside SQLite, and time spent in user
	// code between calls to rows.Next.
	Query(prepCtx context.Context, id TraceConnID, query string, duration time.Duration, err error)

	// BeginTx is called by the driver to report the beginning of Tx.
	BeginTx(beginCtx context.Context, id TraceConnID, why string, readOnly bool, err error)

	// Commit is called by the driver to report the end of a Tx.
	Commit(id TraceConnID, err error)

	// Rollback is called by the driver to report the end of a Tx.
	Rollback(id TraceConnID, err error)

	// Close is called when the SQLite connection is closed.
	Close(id TraceConnID, err error)
}
