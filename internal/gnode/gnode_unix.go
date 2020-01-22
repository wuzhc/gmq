package gnode

import (
	"fmt"
	"os"
	"syscall"
)

func mmapQueueFile(file *os.File, size int, queueFile *queueFile) error {
	data, err := syscall.Mmap(int(file.Fd()), 0, size, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap failed queueFile:%v , %v", file.Name(), err)
	}

	queueFile.buffer = data
	return nil
}

func unmapQueueFile(file *os.File, queueFile *queueFile) error {
	err := syscall.Munmap(queueFile.buffer)
	queueFile.buffer = nil

	if err != nil {
		return fmt.Errorf("unmap failed queueFile:%v , %v", file.Name(), err)
	} else {
		return nil
	}
}

func mmapCommitLog(file *os.File, size int, commitLog *CommitLog) error {
	data, err := syscall.Mmap(int(file.Fd()), 0, size, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap failed commitLog:%v , %v", file.Name(), err)
	}

	commitLog.buffer = data
	return nil
}

func unmapCommitLog(file *os.File, commitLog *CommitLog) error {
	err := syscall.Munmap(commitLog.buffer)
	commitLog.buffer = nil

	if err != nil {
		return fmt.Errorf("unmap failed commitLog:%v , %v", file.Name(), err)
	} else {
		return nil
	}
}
