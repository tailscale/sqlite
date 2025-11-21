//go:build !cgo

package sqlite

// SetLogCallback sets the global SQLite log callback.
// If callback is nil, logs are discarded.
func SetLogCallback(callback LogCallback) error {
	return nil
}
