package model

import "encoding/binary"

/*
	hash type
	metadata:
		key: key
		value: type | expire | version
	data:
		key: key | version | filedId
		value: value
*/

type Hash struct {
	key     []byte
	version int64
}

func NewHash(key []byte, version int64) *Hash {
	return &Hash{
		key:     key,
		version: version,
	}
}

func (h *Hash) MarshalHashKey(fieldId []byte) []byte {
	buf := make([]byte, len(h.key)+len(fieldId)+8)

	// key
	copy(buf[:len(h.key)], h.key)

	// version
	binary.BigEndian.PutUint64(buf[len(h.key):len(h.key)+8], uint64(h.version))

	// field id
	copy(buf[len(h.key)+8:], fieldId)

	return buf
}
