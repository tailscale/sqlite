package cgosqlite

// #include <stdint.h>
// #include <stdlib.h>
// #include <string.h>
// #include <pthread.h>
// #include <sqlite3.h>
// #include <time.h>
// #include "bulk.h"
import "C"
import (
	"fmt"
	"time"
	"unsafe"

	"github.com/tailscale/sqlite/sqliteh"
)

type valueType uint64

const (
	valueNULL  = valueType(0) // matches VALUE_NULL
	valueInt64 = valueType(1) // matches VALUE_INT64
	valueText  = valueType(2) // matches VALUE_TEXT
)

// value has the same memory layout as the type cValue.
type value struct {
	valueType valueType
	value     uint64 // either int64 value or { off, len uint32 } into paramsText
}

// bulkStmt implements sqliteh.BulkStmt.
// It is embedded into BulkQuery/BulkQueryRow/BulkExec to provide common features.
type bulkStmt struct {
	active       bool // true if a query has been started and not yet reset
	stmt         *Stmt
	params       []value
	paramsText   []byte
	paramIndexes map[string]int
}

func newBulkStmt(s *Stmt) *bulkStmt {
	b := &bulkStmt{
		stmt:   s,
		params: make([]value, s.BindParameterCount()),
	}
	return b
}

func (b *bulkStmt) clear() {
	for i := range b.params {
		b.params[i].valueType = valueNULL
	}
	b.paramsText = b.paramsText[:0]
}

func (b *bulkStmt) ResetAndClear() {
	if b.active {
		b.stmt.ResetAndClear()
		b.active = false
	}
	b.clear()
}

func (b *bulkStmt) Finalize() error {
	b.clear()
	return b.stmt.Finalize()
}

func (b *bulkStmt) ParamIndex(name string) int {
	i, ok := b.paramIndexes[name]
	if !ok {
		if b.paramIndexes == nil {
			b.paramIndexes = make(map[string]int)
		}
		i = b.stmt.BindParameterIndex(name)
		b.paramIndexes[name] = i
	}
	return i
}

func (b *bulkStmt) SetInt64(i int, value int64) {
	v := b.params[i]
	v.valueType = valueInt64
	v.value = uint64(value)
}

func (b *bulkStmt) SetNull(i int) {
	v := b.params[i]
	v.valueType = valueNULL
}

func (b *bulkStmt) SetText(i int, value []byte) {
	off := len(b.paramsText)
	b.paramsText = append(b.paramsText, value...)

	v := b.params[i]
	v.valueType = valueText
	v.value = uint64(off)<<32 | uint64(len(value))
}

type BulkExec struct {
	*bulkStmt
}

func NewBulkExec(stmt *Stmt) (*BulkExec, error) {
	b := &BulkExec{bulkStmt: newBulkStmt(stmt)}
	// TODO check for lack of return rows?
	return b, nil
}

func (b *BulkExec) Exec() (lastInsertRowID, changes int64, d time.Duration, err error) {
	b.stmt.rowid, b.stmt.changes, b.stmt.duration = 0, 0, 0
	var params unsafe.Pointer
	if len(b.params) > 0 {
		params = unsafe.Pointer(&b.params[0])
	}
	var paramsText unsafe.Pointer
	if len(b.paramsText) > 0 {
		paramsText = unsafe.Pointer(&b.paramsText[0])
	}
	res := C.bulk_exec(
		b.stmt.stmt,
		(*C.struct_cValue)(params),
		(*C.char)(paramsText), C.size_t(len(b.paramsText)),
		&b.stmt.rowid, &b.stmt.changes, &b.stmt.duration,
	)
	b.clear()
	if sqliteh.Code(res) != sqliteh.SQLITE_DONE {
		return lastInsertRowID, changes, d, errCode(res)
	}
	lastInsertRowID = int64(b.stmt.rowid)
	changes = int64(b.stmt.changes)
	d = time.Duration(b.stmt.duration)
	return lastInsertRowID, changes, d, nil
}

const dataArrLen = 128 // matches DATA_ARR_LEN in C

type BulkQuery struct {
	*bulkStmt
	err      error
	dataArr  [dataArrLen]value // backing array of data
	data     []value
	dataText []byte
	duration int64 // total duration of all time spent in sqlite
	colCount int
}

func NewBulkQuery(stmt *Stmt) (*BulkQuery, error) {
	b := &BulkQuery{
		bulkStmt: newBulkStmt(stmt),
		dataText: make([]byte, 1<<16),
		colCount: stmt.ColumnCount(),
	}
	return b, nil
}

func (b *BulkQuery) Int64(i int) int64 {
	if b.data[i].valueType != valueInt64 {
		panic(fmt.Sprintf("attempting to access column %d as int64, type is %v", i, b.data[i].valueType))
	}
	return int64(b.data[i].value)
}

func (b *BulkQuery) Null(i int) bool { return b.data[i].valueType == valueNULL }

func (b *BulkQuery) Text(i int) []byte {
	if b.data[i].valueType != valueText {
		panic(fmt.Sprintf("attempting to access column %d as text, type is %v", i, b.data[i].valueType))
	}
	v := b.data[i].value
	voff := uint32(v >> 32)
	vlen := uint32(v)
	return b.dataText[voff : voff+vlen]
}

func (b *BulkQuery) Error() error { return b.err }

func (b *BulkQuery) resizeDataText() {
	b.dataText = append(b.dataText, make([]byte, len(b.dataText))...)
}

func (b *BulkQuery) Query() {
	b.err = nil
	b.active = false
	b.stmt.duration = 0
	var params unsafe.Pointer
	if len(b.params) > 0 {
		params = unsafe.Pointer(&b.params[0])
	}
	var paramsText unsafe.Pointer
	if len(b.paramsText) > 0 {
		paramsText = unsafe.Pointer(&b.paramsText[0])
	}
	res := C.bulk_query(
		b.stmt.stmt,
		(*C.struct_cValue)(params),
		(*C.char)(paramsText), C.size_t(len(b.paramsText)),
		&b.stmt.duration,
	)
	b.duration = int64(b.stmt.duration)
	if res == C.SQLITE_ROW {
		b.active = true
	}
	b.err = errCode(res)
}

func (b *BulkQuery) Next() bool {
	if len(b.data) > 0 {
		b.data = b.data[b.colCount:]
		if len(b.data) > 0 {
			return true
		}
	}
	if !b.active {
		return false
	}

	for {
		b.stmt.duration = 0
		data := unsafe.Pointer(&b.dataArr[0])
		dataText := unsafe.Pointer(&b.dataText[0])
		var rowsRead C.size_t
		res := C.bulk_query_step(
			b.stmt.stmt,
			(*C.struct_cValue)(data),
			(*C.char)(dataText), C.size_t(len(b.dataText)),
			&rowsRead,
			&b.stmt.duration,
		)

		if rowsRead == 0 {
			switch res {
			case C.SQLITE_DONE:
				b.active = false
				return false
			case C.BULK_TEXT_TOO_SMALL:
				b.resizeDataText()
				continue
			default:
				b.err = errCode(res)
				return false
			}
		} else {
			b.data = b.dataArr[:int(rowsRead)*b.colCount]
			switch res {
			case C.SQLITE_ROW, C.BULK_TEXT_TOO_SMALL:
				b.active = true
				return true
			case C.SQLITE_DONE:
				b.active = false
				return true
			default:
				b.err = errCode(res)
				return false
			}
		}
	}
}
