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

// step_result combines three cgo calls to save overhead.
static int step_result(sqlite3_stmt* stmt, sqlite3_int64* rowid, sqlite3_int64* changes) {
	int ret = sqlite3_step(stmt);
	sqlite3* db = sqlite3_db_handle(stmt);
	*rowid = sqlite3_last_insert_rowid(db);
	*changes = sqlite3_changes(db);
	return ret;
}
