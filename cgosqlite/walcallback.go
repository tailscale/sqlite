package cgosqlite

/*
#include <sqlite3.h>
*/
import "C"
import (
	"sync"
	"unsafe"
)

type walHookCb func(dbName string, pages int)

var walHookFunc sync.Map // from *C.sqlite3 to walHookCb

//export walCallbackGo
func walCallbackGo(db *C.sqlite3, dbNameC *C.char, dbNameLen C.int, pages C.int) C.int {
	v, _ := walHookFunc.Load(db)
	hook, _ := v.(walHookCb)
	if hook == nil {
		return C.int(0)
	}

	dbNameB := unsafe.Slice((*byte)(unsafe.Pointer(dbNameC)), dbNameLen)
	dbName := stringFromBytes(dbNameB)
	hook(dbName, int(pages))
	return C.int(0) // result's kinda useless
}
