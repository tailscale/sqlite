package sqlitestats

import (
	"context"
	"database/sql"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tailscale/sqlite"
)

func TestActiveTxs(t *testing.T) {
	tracer := &Stats{}
	db := sql.OpenDB(sqlite.Connector("file:"+t.TempDir()+"/test.db", nil, tracer))
	defer db.Close()

	ctx := context.Background()
	tx1, err := db.BeginTx(WithName(ctx, "test-rw"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()
	tx2, err := db.BeginTx(WithName(ctx, "test-rw-closed"), nil)
	if err != nil {
		t.Fatal(err)
	}
	tx2.Rollback()
	tx3, err := db.BeginTx(WithName(ctx, "test-ro"), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx3.Rollback()

	srv := httptest.NewServer(tracer)
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
	if want := "active transactions (2):"; !strings.Contains(s, want) {
		t.Fatalf("want %q, got:\n%s", want, s)
	}
	if want := "test-rw"; !strings.Contains(s, want) {
		t.Fatalf("want %q, got:\n%s", want, s)
	}
	if want := "test-ro"; !strings.Contains(s, want) {
		t.Fatalf("want %q, got:\n%s", want, s)
	}
}
