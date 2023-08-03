#include "stdint.h"

// uintptr versions of sqlite3 pointer types, to avoid allocations
// in cgo code. (go/corp/9919)
typedef uintptr_t handle_sqlite3_stmt; // a *sqlite3_stmt
typedef uintptr_t handle_sqlite3; // a *sqlite3 (DB conn)

// Helper methods to deal with int <-> pointer pain.

static int bind_text64(handle_sqlite3_stmt stmt, int col, const char* str, sqlite3_uint64 len) {
	return sqlite3_bind_text64((sqlite3_stmt*)(stmt), col, str, len, free, SQLITE_UTF8);
}

static int bind_text64_empty(handle_sqlite3_stmt stmt, int col) {
	return sqlite3_bind_text64((sqlite3_stmt*)(stmt), col, "", 0, SQLITE_STATIC, SQLITE_UTF8);
}

static int bind_blob64(handle_sqlite3_stmt stmt, int col, char* str, sqlite3_uint64 n) {
	return sqlite3_bind_blob64((sqlite3_stmt*)(stmt), col, str, n, SQLITE_TRANSIENT);
}

static int ts_sqlite3_bind_double(handle_sqlite3_stmt stmt, int col, double v) {
	return sqlite3_bind_double((sqlite3_stmt*)(stmt), col, v);
}

static int ts_sqlite3_bind_int64(handle_sqlite3_stmt stmt, int col, sqlite3_int64 v) {
	return sqlite3_bind_int64((sqlite3_stmt*)(stmt), col, v);
}

static int ts_sqlite3_bind_null(handle_sqlite3_stmt stmt, int col) {
	return sqlite3_bind_null((sqlite3_stmt*)(stmt), col);
}

// We only need the Go string's memory for the duration of the call,
// and the GC pins it for us if we pass the gostring_t to C, so we
// do the conversion here instead of with C.CString.
static int bind_parameter_index(handle_sqlite3_stmt stmt, _GoString_ s) {
	size_t n = _GoStringLen(s);
	const char *p = (const char *)_GoStringPtr(s);

	// Start with zeroed zName to provide NUL-terminated string.
	char zName[256] = {0};
	if (n >= sizeof zName) {
		return 0;
	}
	memmove(zName, p, n);
	return sqlite3_bind_parameter_index((sqlite3_stmt*)(stmt), zName);
}

static void monotonic_clock_gettime(struct timespec* t) {
	clock_gettime(CLOCK_MONOTONIC, t);
}

static int64_t ns_since(const struct timespec t1)
{
	struct timespec t2;
	monotonic_clock_gettime(&t2);
	return ((int64_t)t2.tv_sec - (int64_t)t1.tv_sec) * (int64_t)1000000000 +
	  ((int64_t)t2.tv_nsec - (int64_t)t1.tv_nsec);
}

// step_result combines several cgo calls to save overhead.
static int step_result(handle_sqlite3_stmt stmth, sqlite3_int64* rowid, sqlite3_int64* changes, int64_t* duration_ns) {
	sqlite3_stmt* stmt = (sqlite3_stmt*)(stmth);
	struct timespec t1;
	if (duration_ns) {
		monotonic_clock_gettime(&t1);
	}
	int ret = sqlite3_step(stmt);
	sqlite3* db = sqlite3_db_handle(stmt);
	*rowid = sqlite3_last_insert_rowid(db);
	*changes = sqlite3_changes(db);
	sqlite3_reset(stmt);
	sqlite3_clear_bindings(stmt);
	if (duration_ns) {
		*duration_ns = ns_since(t1);
	}
	return ret;
}

// reset_and_clear combines two cgo calls to save overhead.
static int reset_and_clear(handle_sqlite3_stmt stmth, struct timespec* start, int64_t* duration_ns) {
	sqlite3_stmt* stmt = (sqlite3_stmt*)(stmth);
	int ret = sqlite3_reset(stmt);
	int ret2 = sqlite3_clear_bindings(stmt);
	if (duration_ns) {
		*duration_ns = ns_since(*start);
	}
	if (ret != SQLITE_OK) {
		return ret;
	}
	return ret2;
}

int walCallbackGo(sqlite3 *db, char *dbName, int dbNameLen, int pages);

static int wal_callback_into_go(void *userData, sqlite3 *db, const char *dbName,
                            int pages) {
	return walCallbackGo(db, (char *)dbName, strlen(dbName), pages);
}

// ts_sqlite3_wal_hook_go makes db's WAL hook call into Go.
//
// It must already be registered on Go's side first.
static void ts_sqlite3_wal_hook_go(sqlite3* db) {
	sqlite3_wal_hook(db, wal_callback_into_go, 0);
}

static int ts_sqlite3_step(handle_sqlite3_stmt stmth, char* outType , int outTypeLen) {
	sqlite3_stmt* stmt = (sqlite3_stmt*)(stmth);
	int res = sqlite3_step(stmt);
	if (res == SQLITE_ROW && outTypeLen > 0) {
		int cols = sqlite3_column_count(stmt);
		for (int i = 0; i < cols && i < outTypeLen; i++) {
			outType[i] = (char) sqlite3_column_type(stmt, i);
		}
	}
	return res;
}

static const unsigned char *ts_sqlite3_column_text(handle_sqlite3_stmt stmt, int iCol) {
	return sqlite3_column_text((sqlite3_stmt*)(stmt), iCol);
}

static const unsigned char *ts_sqlite3_column_blob(handle_sqlite3_stmt stmt, int iCol) {
	return sqlite3_column_blob((sqlite3_stmt*)(stmt), iCol);
}

static int ts_sqlite3_column_type(handle_sqlite3_stmt stmt, int iCol) {
	return sqlite3_column_type((sqlite3_stmt*)(stmt), iCol);
}

static int ts_sqlite3_column_bytes(handle_sqlite3_stmt stmt, int iCol) {
	return sqlite3_column_bytes((sqlite3_stmt*)(stmt), iCol);
}

static double ts_sqlite3_column_double(handle_sqlite3_stmt stmt, int iCol) {
	return sqlite3_column_double((sqlite3_stmt*)(stmt), iCol);
}

static sqlite3_int64 ts_sqlite3_column_int64(handle_sqlite3_stmt stmt, int iCol) {
	return sqlite3_column_int64((sqlite3_stmt*)(stmt), iCol);
}
