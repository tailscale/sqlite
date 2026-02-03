package cgosqlite

/*
#include <sqlite3.h>

void logCallbackGo(void* userData, int errCode, char* msgC);

static void log_callback_into_go(void *userData, int errCode, const char *msg) {
	logCallbackGo(userData, errCode, (char*)msg);
}

static int ts_sqlite3_config_log(void) {
	// TODO(raggi): if the library gains new uses of sqlite3_config they need to
	// share a mutex.
	return sqlite3_config(SQLITE_CONFIG_LOG, log_callback_into_go, NULL);
}
*/
import "C"
import (
	"sync"
	"unsafe"

	"github.com/tailscale/sqlite-exp/sqliteh"
)

// LogCallback receives SQLite log messages.
type LogCallback func(code sqliteh.Code, msg string)

var (
	logCallbackMu sync.Mutex
	logCallback   LogCallback
)

//export logCallbackGo
func logCallbackGo(userData unsafe.Pointer, errCode C.int, msgC *C.char) {
	logCallbackMu.Lock()
	cb := logCallback
	logCallbackMu.Unlock()

	if cb == nil {
		return
	}

	msg := C.GoString(msgC)
	cb(sqliteh.Code(errCode), msg)
}

// SetLogCallback sets the global SQLite log callback.
// If callback is nil, logs are discarded.
func SetLogCallback(callback LogCallback) error {
	logCallbackMu.Lock()
	logCallback = callback
	logCallbackMu.Unlock()

	res := C.ts_sqlite3_config_log()
	return errCode(res)
}
