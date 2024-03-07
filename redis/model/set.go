package model

import "encoding/binary"

/*
  	set type
		metadata:
			key: key
			value: type | expire | version
		data:
			key: key | version | member
			value: nil
*/

type Set struct {
	version int64
}

func NewSet(version int64) *Set {
	return &Set{version: version}
}

func (s *Set) MarshalKey(key, member []byte) []byte {
	buf := make([]byte, len(key)+8+len(member))

	copy(buf[:len(key)], key)

	binary.BigEndian.PutUint64(buf[len(key):len(key)+8], uint64(s.version))

	copy(buf[len(key)+8:], member)

	return buf
}
