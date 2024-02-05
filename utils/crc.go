package utils

import "hash/crc32"

func GenerateCrc(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func CheckCrc(crc uint32, data []byte) bool {
	return GenerateCrc(data) == crc
}
