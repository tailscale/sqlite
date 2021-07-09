// Package cgosqlite is a low-level interface onto SQLite using cgo.
//
// This package is designed to have as few opinions as possible.
// It wraps the SQLite3 C API with functions that are Go-friendly
// and are designed to make any feature of the underlying C API
// usable. That means not unduly heap allocating where C wouldn't.
//
// Users of this package do not need to use any cgo, which means
// code using cgosqlite can focus on semantic transform of the API,
// not C<->Go transforms.
package cgosqlite
