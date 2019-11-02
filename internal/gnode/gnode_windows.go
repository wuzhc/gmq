package gnode

import (
	"fmt"
	"os"
	"syscall"
)

// 参考blot内存映射
func mmap(q *queue, size int) error {
	if err := q.file.Truncate(int64(size)); err != nil {
		return fmt.Errorf("truncate: %s", err)
	}

	// Open a file mapping handle.
	sizelo := uint32(size >> 32)
	sizehi := uint32(size) & 0xffffffff
	h, errno := syscall.CreateFileMapping(syscall.Handle(q.file.Fd()), nil, syscall.PAGE_READONLY, sizelo, sizehi, nil)
	if h == 0 {
		return os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if addr == 0 {
		return os.NewSyscallError("MapViewOfFile", errno)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return os.NewSyscallError("CloseHandle", err)
	}

	// Convert to a byte array.
	q.data = addr
	return nil
}

func unmap(q *queue) error {
	if q.data == nil {
		return nil
	}

	if err := syscall.UnmapViewOfFile(q.data); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
