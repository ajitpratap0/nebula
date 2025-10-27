//go:build linux
// +build linux

package mmap

import (
	"syscall"
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
	return syscall.Madvise(b, advice)
}

const (
	// Memory protection flags
	ProtRead = syscall.PROT_READ //nolint:stylecheck

	// Memory mapping flags
	MapShared = syscall.MAP_SHARED //nolint:stylecheck

	// Memory advice flags
	MadvSequential = syscall.MADV_SEQUENTIAL //nolint:stylecheck
	MadvWillneed   = syscall.MADV_WILLNEED   //nolint:stylecheck
)
