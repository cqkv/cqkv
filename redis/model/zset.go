package model

import (
	"encoding/binary"

	"github.com/cqkv/cqkv/utils"
)

const (
	ZSetDataKey byte = iota
	ZSetScoreKey
)

/*
	ZSet type
		metadata:
			key: key
			value: type | expire | version
		data:
			ZSet data store member -> score and score -> member

			dataKey: key | version | type | member
			dataValue: score

			scoreKey: key | version | type | score | member (used for sort)
			dataValue: nil
*/

type ZSet struct {
	version int64
}

func NewZSet(version int64) *ZSet {
	return &ZSet{version: version}
}

func (zs *ZSet) MarshalZSetKey(key, member []byte, score float64) ([]byte, []byte) {
	dataBuf := make([]byte, len(key)+8+1+len(member))

	copy(dataBuf[:len(key)], key)

	binary.BigEndian.PutUint64(dataBuf[len(key):len(key)+8], uint64(zs.version))

	dataBuf[len(key)+8+1] = ZSetDataKey

	copy(dataBuf[len(key)+8+1:], member)

	scoreByte := utils.Float2byte(score)

	scoreBuf := make([]byte, len(key)+8+1+len(scoreByte)+len(member))

	copy(scoreByte[:len(key)], key)

	binary.BigEndian.PutUint64(scoreByte[len(key):scoreByte[len(key)+8]], uint64(zs.version))

	scoreBuf[len(key)+8+1] = ZSetScoreKey

	copy(scoreByte[len(key)+8+1:len(key)+8+1+len(scoreByte)], scoreByte)

	copy(scoreByte[len(key)+8+1+len(scoreByte):], member)

	return dataBuf, scoreBuf
}
