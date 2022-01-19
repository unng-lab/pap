package conn

import "unsafe"

var _ = unsafe.Sizeof(0)

//go:linkname nanotime runtime.nanotime
func nanotime() int64
