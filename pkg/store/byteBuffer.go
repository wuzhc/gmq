package store

import (
	"encoding/binary"
	"fmt"
)

type MappedByteBuffer []byte

func (buffer MappedByteBuffer) Size() int {
	return len(buffer)
}

func (buffer MappedByteBuffer) PutUint16(offset int, value uint16) error {
	if buffer.Size() < offset+2 {
		return fmt.Errorf("Index out of bounds")
	}

	binary.BigEndian.PutUint16(buffer[offset:offset+2], value)
	return nil
}

func (buffer MappedByteBuffer) PutUint32(offset int, value uint32) error {
	if buffer.Size() < offset+4 {
		return fmt.Errorf("Index out of bounds")
	}

	binary.BigEndian.PutUint32(buffer[offset:offset+4], value)
	return nil
}

func (buffer MappedByteBuffer) PutUint64(offset int, value uint64) error {
	if buffer.Size() < offset+8 {
		return fmt.Errorf("Index out of bounds")
	}

	binary.BigEndian.PutUint64(buffer[offset:offset+8], value)
	return nil
}

func (buffer MappedByteBuffer) Uint16(offset int) (uint16, error) {
	if buffer.Size() < offset+2 {
		return 0, fmt.Errorf("Index out of bounds")
	}

	return binary.BigEndian.Uint16(buffer[offset : offset+2]), nil
}

func (buffer MappedByteBuffer) Uint32(offset int) (uint32, error) {
	if buffer.Size() < offset+4 {
		return 0, fmt.Errorf("Index out of bounds")
	}

	return binary.BigEndian.Uint32(buffer[offset : offset+4]), nil
}

func (buffer MappedByteBuffer) Uint64(offset int) (uint64, error) {
	if buffer.Size() < offset+8 {
		return 0, fmt.Errorf("Index out of bounds")
	}

	return binary.BigEndian.Uint64(buffer[offset : offset+8]), nil
}
