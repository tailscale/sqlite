package cgosqlite

import _ "unsafe"

// findnull exposes the runtime.findnull function to the cgosqlite package, this
// is a wide instruction optimized page by page null byte search aka fast
// strlen.
//
//go:linkname findnull runtime.findnull
func findnull(*byte) int
