//go:build !sqlite_enable_api_armor

package cgosqlite

import (
	"testing"
)

func TestAPIArmorDisabled(t *testing.T) {
	testAPIArmor(t, false)
}
