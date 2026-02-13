//go:build cgo

package sqlite

import (
	"context"
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/tailscale/sqlite/cgosqlite"
)

func TestTmstmpVfs(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	tmstmpDir := dbPath + "-tmstmp"
	if err := os.Mkdir(tmstmpDir, 0755); err != nil {
		log.Fatalf("failed to create tmstmp temp dir: %s", err)
	}

	openAndRun := func(f func(*sql.Conn)) {
		sqlDB := sql.OpenDB(Connector(
			"file:"+dbPath,
			nil, // connInitFunc
			nil, // tracer
		))
		defer sqlDB.Close()

		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer conn.Close()

		f(conn)
	}

	// Write to the database to ensure there's some data, change reserved_bytes and vacuum.
	openAndRun(func(conn *sql.Conn) {
		if _, err := conn.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);"); err != nil {
			t.Fatalf("failed to create table: %v", err)
		}
		// Set 16 bytes of reserved space per the tmstmpvfs requirements.
		if err := SetReserveBytes(conn, "", 16); err != nil {
			t.Fatalf("failed to set reserved bytes: %v", err)
		}
		if err := ExecScript(conn, "VACUUM;"); err != nil {
			t.Fatalf("failed to run VACUUM: %v", err)
		}
	})

	// Now open a new connection and write some data, which should cause tmstmpvfs to
	// create a log file in the tmstmp directory.
	openAndRun(func(conn *sql.Conn) {
		if _, err := conn.ExecContext(ctx, "INSERT INTO test (value) VALUES ('test');"); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	})

	files, err := os.ReadDir(tmstmpDir)
	if err != nil {
		t.Fatalf("failed to read log files: %s", err)
	}
	t.Logf("files in tmstmp dir: %v", files)
	foundFiles := len(files) > 0

	// If tmpstmpvfs is being used, there should be a log file
	// in the tmpstmp directory for the write connection above.
	t.Logf("TimestampVFSEnabled state: %v", cgosqlite.TimestampVFSEnabled())
	if foundFiles != cgosqlite.TimestampVFSEnabled() {
		t.Fatalf("sqlite uses tmpstmpvfs=%v want=%v", foundFiles, cgosqlite.TimestampVFSEnabled())
	}
}
