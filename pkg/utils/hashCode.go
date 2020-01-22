package utils

import "hash/crc32"

func GenerateHashCode(key []byte) int {
	v := int(crc32.ChecksumIEEE(key))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	return 0
}
