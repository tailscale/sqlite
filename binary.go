// Copyright (c) 2023 Tailscale Inc & AUTHORS All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/tailscale/sqlite/sqliteh"
	"golang.org/x/sys/cpu"
)

type driverConnRawCall struct {
	f func(driverConn any) error

	// results
	dc *conn
	ok bool
}

var driverConnRawCallPool = &sync.Pool{
	New: func() any {
		c := new(driverConnRawCall)
		c.f = func(driverConn any) error {
			c.dc, c.ok = driverConn.(*conn)
			return nil
		}
		return c
	},
}

func getDriverConn(sc SQLConn) (dc *conn, ok bool) {
	c := driverConnRawCallPool.Get().(*driverConnRawCall)
	defer driverConnRawCallPool.Put(c)
	err := sc.Raw(c.f)
	if err != nil {
		return nil, false
	}
	return c.dc, c.ok
}

func QueryBinary(ctx context.Context, sqlconn SQLConn, optScratch []byte, query string, args ...any) (BinaryResults, error) {
	c, ok := getDriverConn(sqlconn)
	if !ok {
		return nil, errors.New("sqlconn is not of expected type")
	}
	st, err := c.prepare(ctx, query, IsPersist(ctx))
	if err != nil {
		return nil, err
	}
	buf := optScratch
	if len(buf) == 0 {
		buf = make([]byte, 128)
	}
	for {
		st.stmt.ResetAndClear()

		// Bind args.
		for colIdx, a := range args {
			rv := reflect.ValueOf(a)
			switch rv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if err := st.stmt.BindInt64(colIdx+1, rv.Int()); err != nil {
					return nil, fmt.Errorf("binding col idx %d to %T (%v): %w", colIdx, a, rv.Int(), err)
				}
			default:
				// TODO(bradfitz): more types, at least strings for stable IDs.
				return nil, fmt.Errorf("unsupported arg type %T", a)
			}
		}

		n, err := st.stmt.StepAllBinary(buf)
		if err == nil {
			return BinaryResults(buf[:n]), nil
		}
		if e, ok := err.(sqliteh.BufferSizeTooSmallError); ok {
			buf = make([]byte, e.EncodedSize)
			continue
		}
		return nil, err
	}
}

// BinaryResults is the result of QueryBinary.
//
// You should not depend on its specific format and parse it via its methods
// instead.
type BinaryResults []byte

type BinaryToken struct {
	StartRow bool
	EndRow   bool
	EndRows  bool
	IsInt    bool // if so, use Int() method
	IsFloat  bool // if so, use Float() method
	IsNull   bool
	IsBytes  bool
	Error    bool

	x     uint64
	Bytes []byte
}

func (t *BinaryToken) String() string {
	switch {
	case t.StartRow:
		return "start-row"
	case t.EndRow:
		return "end-row"
	case t.EndRows:
		return "end-rows"
	case t.IsNull:
		return "null"
	case t.IsInt:
		return fmt.Sprintf("int: %v", t.Int())
	case t.IsFloat:
		return fmt.Sprintf("float: %g", t.Float())
	case t.IsBytes:
		return fmt.Sprintf("bytes: %q", t.Bytes)
	case t.Error:
		return "error"
	default:
		return "unknown"
	}
}

func (t *BinaryToken) Int() int64     { return int64(t.x) }
func (t *BinaryToken) Float() float64 { return math.Float64frombits(t.x) }

func (r *BinaryResults) Next() BinaryToken {
	if len(*r) == 0 {
		return BinaryToken{Error: true}
	}
	first := (*r)[0]
	*r = (*r)[1:]
	switch first {
	default:
		return BinaryToken{Error: true}
	case '(':
		return BinaryToken{StartRow: true}
	case ')':
		return BinaryToken{EndRow: true}
	case 'E':
		return BinaryToken{EndRows: true}
	case 'n':
		return BinaryToken{IsNull: true}
	case 'i', 'f':
		if len(*r) < 8 {
			return BinaryToken{Error: true}
		}
		t := BinaryToken{IsInt: first == 'i', IsFloat: first == 'f'}
		if cpu.IsBigEndian {
			t.x = binary.BigEndian.Uint64((*r)[:8])
		} else {
			t.x = binary.LittleEndian.Uint64((*r)[:8])
		}
		*r = (*r)[8:]
		return t
	case 'b':
		if len(*r) < 8 {
			return BinaryToken{Error: true}
		}
		t := BinaryToken{IsBytes: true}
		var n int64
		if cpu.IsBigEndian {
			n = int64(binary.BigEndian.Uint64((*r)[:8]))
		} else {
			n = int64(binary.LittleEndian.Uint64((*r)[:8]))
		}
		*r = (*r)[8:]
		if int64(len(*r)) < n {
			return BinaryToken{Error: true}
		}
		t.Bytes = (*r)[:n]
		*r = (*r)[n:]
		return t
	}
}
