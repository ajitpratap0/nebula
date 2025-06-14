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
	PROT_READ = syscall.PROT_READ

	// Memory mapping flags
	MAP_SHARED = syscall.MAP_SHARED

	// Memory advice flags
	MADV_SEQUENTIAL = syscall.MADV_SEQUENTIAL
	MADV_WILLNEED   = syscall.MADV_WILLNEED
)
