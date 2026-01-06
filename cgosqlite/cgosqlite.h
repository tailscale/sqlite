#include <math.h> // math.h is required on linux for FTS5.
#include "sqlite3.h"
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

// We only need the Go string's memory for the duration of the call,
// and the GC pins it for us if we pass the gostring_t to C, so we
// do the conversion here instead of with C.CString.
static int bind_parameter_index(sqlite3_stmt* stmt, _GoString_ s) {
	size_t n = _GoStringLen(s);
	const char *p = (const char *)_GoStringPtr(s);

	// Start with zeroed zName to provide NUL-terminated string.
	char zName[256] = {0};
	if (n >= sizeof zName) {
		return 0;
	}
	memmove(zName, p, n);
	return sqlite3_bind_parameter_index(stmt, zName);
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
static int step_result(sqlite3_stmt* stmt, sqlite3_int64* rowid, sqlite3_int64* changes, int64_t* duration_ns) {
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
static int reset_and_clear(sqlite3_stmt* stmt, struct timespec* start, int64_t* duration_ns) {
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

static int ts_sqlite3_step(sqlite3_stmt* stmt, char* outType , int outTypeLen) {
	int res = sqlite3_step(stmt);
	if (res == SQLITE_ROW && outTypeLen > 0) {
		int cols = sqlite3_column_count(stmt);
		for (int i = 0; i < cols && i < outTypeLen; i++) {
			outType[i] = (char) sqlite3_column_type(stmt, i);
		}
	}
	return res;
}

static int ts_sqlite3_disable_function(sqlite3 *db, const char *zFunctionName, int nArg) {
	return sqlite3_create_function(db, zFunctionName, nArg, SQLITE_ANY, NULL, NULL, NULL, NULL);
}
