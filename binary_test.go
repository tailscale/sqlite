// Copyright (c) 2023 Tailscale Inc & AUTHORS All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestQueryBinary(t *testing.T) {
	ctx := WithPersist(context.Background())
	db := openTestDB(t)
	exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, f REAL, txt TEXT, blb BLOB)")
	exec(t, db, "INSERT INTO t VALUES (?, ?, ?, ?)", math.MinInt64, 1.0, "text-a", "blob-a")
	exec(t, db, "INSERT INTO t VALUES (?, ?, ?, ?)", -1, -1.0, "text-b", "blob-b")
	exec(t, db, "INSERT INTO t VALUES (?, ?, ?, ?)", 0, 0, "text-c", "blob-c")
	exec(t, db, "INSERT INTO t VALUES (?, ?, ?, ?)", 20, 2, "text-d", "blob-d")
	exec(t, db, "INSERT INTO t VALUES (?, ?, ?, ?)", math.MaxInt64, nil, "text-e", "blob-e")
	exec(t, db, "INSERT INTO t VALUES (?, ?, ?, ?)", 42, 0.25, "text-f", nil)
	exec(t, db, "INSERT INTO t VALUES (?, ?, ?, ?)", 43, 1.75, "text-g", nil)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	buf, err := QueryBinary(ctx, conn, make([]byte, 100), "SELECT * FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Got %d bytes: %q", len(buf), buf)

	var got []string
	iter := buf
	for len(iter) > 0 {
		t := iter.Next()
		got = append(got, t.String())
		if t.Error {
			break
		}
	}
	want := []string{
		"start-row", "int: -9223372036854775808", "float: 1", "bytes: \"text-a\"", "bytes: \"blob-a\"", "end-row",
		"start-row", "int: -1", "float: -1", "bytes: \"text-b\"", "bytes: \"blob-b\"", "end-row",
		"start-row", "int: 0", "float: 0", "bytes: \"text-c\"", "bytes: \"blob-c\"", "end-row",
		"start-row", "int: 20", "float: 2", "bytes: \"text-d\"", "bytes: \"blob-d\"", "end-row",
		"start-row", "int: 42", "float: 0.25", "bytes: \"text-f\"", "null", "end-row",
		"start-row", "int: 43", "float: 1.75", "bytes: \"text-g\"", "null", "end-row",
		"start-row", "int: 9223372036854775807", "null", "bytes: \"text-e\"", "bytes: \"blob-e\"", "end-row",
		"end-rows",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("wrong results\n got: %q\nwant: %q\n\ndiff:\n%s", got, want, cmp.Diff(want, got))
	}

	allocs := int(testing.AllocsPerRun(10000, func() {
		_, err := QueryBinary(ctx, conn, buf, "SELECT * FROM t")
		if err != nil {
			t.Fatal(err)
		}
	}))
	const maxAllocs = 5 // as of Go 1.20
	if allocs > maxAllocs {
		t.Errorf("allocs = %v; want max %v", allocs, maxAllocs)
	}
}

func BenchmarkQueryBinaryParallel(b *testing.B) {
	ctx := WithPersist(context.Background())
	db := openTestDB(b)
	exec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, f REAL, txt TEXT, blb BLOB)")
	exec(b, db, "INSERT INTO t VALUES (?, ?, ?, ?)", 42, 0.25, "text-f", "some big big big big blob so big like so many bytes even")

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		conn, err := db.Conn(ctx)
		if err != nil {
			b.Error(err)
			return
		}

		var buf = make([]byte, 250)

		for pb.Next() {
			res, err := QueryBinary(ctx, conn, buf, "SELECT id, f, txt, blb FROM t WHERE id=?", 42)
			if err != nil {
				b.Error(err)
				return
			}
			t := res.Next()
			if !t.StartRow {
				b.Errorf("didn't get start row; got %v", t)
				return
			}
			t = res.Next()
			if t.Int() != 42 {
				b.Errorf("got %v; want 42", t)
				return
			}
		}
	})

}
