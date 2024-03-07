package model

import (
	"encoding/binary"
	"math"
)

const (
	initialListMark = math.MaxUint64 / 2
)

/*
	list type
		metadata:
			key: key
			value: type | expire | version | head | tail (the data that tail points to is invalid, tail - 1 is valid)
		data:
			key: key | version | idx
			value: value
*/

type List struct {
	version int64
	idx     uint64
}

func NewList(version int64, idx uint64) *List {
	return &List{
		version: version,
		idx:     idx,
	}
}

func (l *List) MarshalListKey(key []byte) []byte {
	buf := make([]byte, len(key)+8+8)

	copy(buf[:len(key)], key)

	binary.BigEndian.PutUint64(buf[len(key):len(key)+8], uint64(l.version))

	binary.BigEndian.PutUint64(buf[len(key)+8:], l.idx)

	return buf
}
