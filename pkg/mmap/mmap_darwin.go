//go:build darwin
// +build darwin

package mmap

import (
	"syscall"
	"unsafe"
)

// mmap wraps the mmap system call
func mmap(fd int, offset int64, length int, prot int, flags int) ([]byte, error) {
	return syscall.Mmap(fd, offset, length, prot, flags)
}

// munmap wraps the munmap system call
func munmap(b []byte) error {
	return syscall.Munmap(b)
}

// madvise wraps the madvise system call
func madvise(b []byte, advice int) error {
	// On macOS, we need to use the madvise system call directly
	_, _, err := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if err != 0 {
		return err
	}
	return nil
}

const (
	// Memory protection flags
	ProtRead = syscall.PROT_READ //nolint:stylecheck

	// Memory mapping flags
	MapShared = syscall.MAP_SHARED //nolint:stylecheck

	// Memory advice flags
	MadvSequential = 2 //nolint:stylecheck // Sequential page references
	MadvWillneed   = 3 //nolint:stylecheck // Will need these pages
)
