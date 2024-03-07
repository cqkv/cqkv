package model

import (
	"encoding/binary"
	"time"
)

/*
	string type
	key: key
	value:   type  |   expire   |   payload
*/

type String struct{}

func NewString() *String { return &String{} }

func (str *String) Marshal(ttl time.Duration, payload []byte) []byte {
	buf := make([]byte, binary.MaxVarintLen64)

	buf[0] = StringType
	var idx = 1
	var expire int64
	if ttl > 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	idx = binary.PutVarint(buf, expire)

	copy(buf[idx:], payload)

	return buf[:idx+len(payload)]
}

func (str *String) Unmarshal(data []byte) ([]byte, error) {
	// check data type
	if data[0] != StringType {
		return nil, ErrWrongTypeOp
	}

	// check expire time
	expire, n := binary.Varint(data[1:])
	if expire > 0 && time.Now().UnixNano() > expire {
		return nil, nil
	}

	return data[n+1:], nil
}
