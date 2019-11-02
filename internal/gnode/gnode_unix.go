package gnode

import (
	"fmt"
	"syscall"
)

func mmap(q *queue, size int) error {
	data, err := syscall.Mmap(int(q.file.Fd()), 0, size, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap %v.queue failed, %v", q.name, err)
	}

	q.data = data
	return nil
}

func unmap(q *queue) error {
	err := syscall.Munmap(q.data)
	q.data = nil

	if err != nil {
		return fmt.Errorf("unmap %v.queue failed. %v", q.name, err)
	} else {
		return nil
	}
}
