package sqlstats

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tailscale/sqlite"
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
