static void monotonic_clock_gettime(struct timespec* t) {
       clock_gettime(CLOCK_MONOTONIC, t);
}

int64_t ns_since(const struct timespec t1)
{
       struct timespec t2;
       monotonic_clock_gettime(&t2);
       return ((int64_t)t2.tv_sec - (int64_t)t1.tv_sec) * (int64_t)1000000000 +
         ((int64_t)t2.tv_nsec - (int64_t)t1.tv_nsec);
}

#define VALUE_NULL  0
#define VALUE_INT64 1
#define VALUE_TEXT  2

// cValue matches the Go type named value.
struct cValue {
	uint64_t valueType; // one of VALUE_*
	uint64_t value;     // either int64 or off/len uint32
};

static int bulk_bind(sqlite3_stmt* stmt, struct cValue* params, const char* text, size_t textLen) {
	int count = sqlite3_bind_parameter_count(stmt);
	int ret;
	for (int i = 0; i < count; i++) {
		switch (params[i].valueType) {
		case VALUE_INT64:
			ret = sqlite3_bind_int64(stmt, i+1, (sqlite3_int64)params[i].value);
			if (ret) {
				return ret;
			}
			break;
		case VALUE_TEXT: {
			uint32_t off = (uint32_t)(params[i].value>>32);
			uint32_t len = (uint32_t)(params[i].value);
			if (((size_t)off + (size_t)len) > textLen) {
				return SQLITE_MISUSE;
			}
			const char* p = &text[off];
			ret = sqlite3_bind_text64(stmt, i+1, p, len, SQLITE_STATIC, SQLITE_UTF8);
			if (ret) {
				return ret;
			}
			break;
		}
		case VALUE_NULL:
		default:
			 ret = sqlite3_bind_null(stmt, i+1);
			 if (ret) {
				 return ret;
			 }
			 break;
		}
	}
	return 0;
}

static int bulk_exec(
		sqlite3_stmt* stmt,
		struct cValue* params,
		const char* text, size_t textLen,
		sqlite3_int64* rowid,
		sqlite3_int64* changes,
		int64_t* duration_ns) {
	struct timespec t1;
	if (duration_ns) {
	       monotonic_clock_gettime(&t1);
	}
	int ret = bulk_bind(stmt, params, text, textLen);
	if (ret) {
		return ret;
	}
	ret = sqlite3_step(stmt);
	if (ret != SQLITE_DONE) {
		if (duration_ns) {
		       *duration_ns = ns_since(t1);
		}
		return ret;
	}
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

#define DATA_ARR_LEN 128
#define BULK_TEXT_TOO_SMALL -1

static int bulk_query_step(
		sqlite3_stmt* stmt,
		struct cValue* data,
		char* dataText, size_t dataTextLen,
		size_t* rowsRead,
		int64_t* duration_ns) {
	int ret;
	int row;     // data offset
	int off = 0; // dataText offset

	int numCols = sqlite3_column_count(stmt);
	for (row = 0; (row+1)*numCols < DATA_ARR_LEN; row++) {
		for (int col = 0; col < numCols; col++) {
			struct cValue* d = &data[row*numCols + col];
			switch (sqlite3_column_type(stmt, col)) {
			case SQLITE_NULL:
				d->valueType = VALUE_NULL;
				break;
			case SQLITE_INTEGER:
			case SQLITE_FLOAT:
				d->valueType = VALUE_INT64;
				d->value = sqlite3_column_int64(stmt, col);
				break;
			case SQLITE_TEXT:
			case SQLITE_BLOB: {
				int len = sqlite3_column_bytes(stmt, col);
				if (off+len >= dataTextLen) {
					*rowsRead = row; // do not include the current row
					return BULK_TEXT_TOO_SMALL;
				}
				memcpy(dataText+off, sqlite3_column_blob(stmt, col), len);
				d->valueType = VALUE_TEXT;
				d->value = ((uint64_t)off)<<32 | ((uint64_t)len);
				off += len;
				break;
			}
			}
		}

		ret = sqlite3_step(stmt);
		if (ret != SQLITE_ROW) {
			*rowsRead = row + 1;
			if (ret == SQLITE_DONE) {
				sqlite3_reset(stmt);
				sqlite3_clear_bindings(stmt);
			}
			return ret;
		}
	}

	*rowsRead = row;
	return ret;
}

static int bulk_query(
		sqlite3_stmt* stmt,
		struct cValue* params,
		const char* text, size_t textLen,
		int64_t* duration_ns) {
	struct timespec t1;
	if (duration_ns) {
	       monotonic_clock_gettime(&t1);
	}
	sqlite3_reset(stmt);
	int ret = bulk_bind(stmt, params, text, textLen);
	if (ret) {
		return ret;
	}
	ret = sqlite3_step(stmt);
	if (duration_ns) {
	       *duration_ns = ns_since(t1);
	}
	return ret;
}
