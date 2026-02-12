package cgosqlite

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/tailscale/sqlite/sqliteh"
)

// testAPIArmor provides a common function for testing SQLITE_ENABLE_API_ARMOR.
func testAPIArmor(t *testing.T, wantMisuseError bool) {
	if wantMisuseError && !APIArmorEnabled() {
		t.Fatal("APIArmor is not enabled")
	} else if !wantMisuseError && APIArmorEnabled() {
		t.Fatal("APIArmor is enabled")
	}

	var gotMisuseLog atomic.Bool
	err := SetLogCallback(func(code sqliteh.Code, msg string) {
		if code == sqliteh.SQLITE_MISUSE {
			gotMisuseLog.Store(true)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	db, err := Open(filepath.Join(t.TempDir(), "test.db"), sqliteh.OpenFlagsDefault, "")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Configuring AutoCheckpoint on a closed database should result in a misuse error
	// if and only if [APIArmorEnabled] reports true.
	got := db.AutoCheckpoint(1)
	if wantMisuseError {
		if got == nil || got.Error() != "SQLITE_MISUSE" {
			t.Fatalf("want SQLITE_MISUSE, got %s", got)
		}

		if !gotMisuseLog.Load() {
			t.Fatal("did not get SQLITE_MISUSE in LogCallback")
		}
	} else {
		if got != nil && got.Error() == "SQLITE_MISUSE" {
			t.Fatalf("want no error, got %s", got)
		}

		if gotMisuseLog.Load() {
			t.Fatal("got SQLITE_MISUSE in LogCallback")
		}
	}
}
