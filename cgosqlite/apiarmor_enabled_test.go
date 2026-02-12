//go:build sqlite_enable_api_armor

package cgosqlite

import (
	"testing"
)

func TestAPIArmorEnabled(t *testing.T) {
	testAPIArmor(t, true)
}
