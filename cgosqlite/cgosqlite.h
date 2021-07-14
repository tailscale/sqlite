// Helper methods to deal with int <-> pointer pain.

static int bind_text64(sqlite3_stmt* stmt, int col, const char* str, sqlite3_uint64 len) {
	return sqlite3_bind_text64(stmt, col, str, len, free, SQLITE_UTF8);
}

static int bind_text64_empty(sqlite3_stmt* stmt, int col) {
	return sqlite3_bind_text64(stmt, col, "", 0, SQLITE_STATIC, SQLITE_UTF8);
}

static int bind_blob64(sqlite3_stmt* stmt, int col, char* str, sqlite3_uint64 n) {
	return sqlite3_bind_blob64(stmt, col, str, n, SQLITE_TRANSIENT);
}

// We only need the Go string's memory for the duration of the call,
// and the GC pins it for us if we pass the gostring_t to C, so we
// do the conversion here instead of with C.CString.
static int bind_parameter_index(sqlite3_stmt* stmt, _GoString_ s) {
	size_t n = _GoStringLen(s);
	const char *p = _GoStringPtr(s);

	// Start with zeroed zName to provide NUL-terminated string.
	char zName[256] = {0};
	if (n >= sizeof zName) {
		return 0;
	}
	memmove(zName, p, n);
	return sqlite3_bind_parameter_index(stmt, zName);
}

static void monotonic_clock_gettime(struct timespec* t) {
	// TODO(crawshaw): for some reason, I cannot get CLOCK_MONOTONIC
	// defined properly here by cgo. My C compiler seems otherwise
	// fine, I can compile a small C program on linux referring to
	// CLOCK_MONOTONIC.
	// 
	// For now, use the value directly.
	// It is defined in POSIX so it won't change any time soon.
	clock_gettime(1, t); // CLOCK_MONOTONIC
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
