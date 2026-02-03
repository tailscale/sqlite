package sqlstats

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tailscale/sqlite-exp"
)

func TestActiveTxs(t *testing.T) {
	tracer := &Tracer{}
	db := sql.OpenDB(sqlite.Connector("file:"+t.TempDir()+"/test.db", nil, tracer))
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "CREATE TABLE t (c);"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t (c) VALUES (1);"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(tracer.Handle))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if want := "CREATE TABLE t "; !strings.Contains(s, want) {
		t.Fatalf("want %q, got:\n%s", want, s)
	}
	if want := "INSERT INTO t (c)"; !strings.Contains(s, want) {
		t.Fatalf("want %q, got:\n%s", want, s)
	}
}

func TestNormalizeQuery(t *testing.T) {
	tests := []struct {
		q, want string
	}{
		{"", ""},
		{"SELECT 1", "SELECT 1"},
		{"DELETE FROM foo.Bar WHERE UnixNano in (SELECT id from FOO)", "DELETE FROM foo.Bar WHERE UnixNano in (SELECT id from FOO)"},
		{"DELETE FROM foo.Bar WHERE UnixNano in (1)", "DELETE FROM foo.Bar WHERE UnixNano IN (...)"},
		{"DELETE FROM foo.Bar WHERE UnixNano in (1, 2, 3)", "DELETE FROM foo.Bar WHERE UnixNano IN (...)"},
		{"DELETE FROM foo.Bar WHERE UnixNano in (1,2,3)", "DELETE FROM foo.Bar WHERE UnixNano IN (...)"},
		{"DELETE FROM foo.Bar WHERE UnixNano in (1,2,3 )", "DELETE FROM foo.Bar WHERE UnixNano IN (...)"},
		{"DELETE FROM foo.Bar WHERE UnixNano in ( 1 , 2 , 3 )", "DELETE FROM foo.Bar WHERE UnixNano IN (...)"},
	}
	for _, tt := range tests {
		if got := normalizeQuery(tt.q); got != tt.want {
			t.Errorf("normalizeQuery(%#q) = %#q; want %#q", tt.q, got, tt.want)
		}
	}
}

func TestCollect(t *testing.T) {
	tracer := &Tracer{}
	db := sql.OpenDB(sqlite.Connector("file:"+t.TempDir()+"/test.db", nil, tracer))
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "CREATE TABLE t (c);"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t (c) VALUES (1);"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t (c) VALUES (1);"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	gotStats := tracer.Collect()
	slices.SortFunc(gotStats, func(a, b *QueryStats) int {
		if a.Query < b.Query {
			return -1
		}
		return 1
	})

	// List containing expecting query counts in order.
	wantCount := []int{1, 1, 1, 2}

	if len(gotStats) != len(wantCount) {
		t.Errorf("unexpected number of queries compared to defined count, queries: %d, want: %d", len(gotStats), len(wantCount))
	}

	for idx, query := range gotStats {
		if query.Count != int64(wantCount[idx]) {
			t.Errorf("unexpected query count for %q, got: %d, expected: %d", query.Query, query.Count, wantCount[idx])
		}
	}
}

func TestTracerResetRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tracer := &Tracer{}
	db := sql.OpenDB(sqlite.Connector("file:"+t.TempDir()+"/test.db", nil, tracer))
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "CREATE TABLE t (c);"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Continually reset the tracer.
	go func() {
		for ctx.Err() == nil {
			tracer.Reset()
		}
	}()

	// Continually grab transactions and insert into the database.
	errc := make(chan error)
	go func() {
		defer close(errc)
		var n int64
		for ctx.Err() == nil {
			n++
			tx, err = db.BeginTx(ctx, nil)
			if err != nil {
				errc <- fmt.Errorf("starting tx: %w", err)
				return
			}
			if _, err = tx.Exec("INSERT INTO t VALUES(?)", n); err != nil {
				errc <- fmt.Errorf("insert: %w", err)
				return
			}
			if n%2 == 0 {
				if err := tx.Commit(); err != nil {
					errc <- fmt.Errorf("commit: %w", err)
					return
				}
			}
			tx.Rollback()
		}
	}()

	for err := range errc {
		if ctx.Err() != nil {
			return
		}
		t.Fatalf("unexpected error: %s", err)
	}
}
