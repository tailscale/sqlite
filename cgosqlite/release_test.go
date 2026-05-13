//go:build !sqlite_trunk

package cgosqlite

import "testing"

func TestSQLiteVersion(t *testing.T) {
	const want = "3.53.1"
	got := SQLiteVersion()
	if got != want {
		t.Fatalf("wrong version, want %s, got %s", want, got)
	}
}

func TestSQLiteVersionNumber(t *testing.T) {
	const want = 3053001
	got := SQLiteVersionNumber()
	if got != want {
		t.Fatalf("wrong version, want %d, got %d", want, got)
	}
}
