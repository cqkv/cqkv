package model

import "encoding/binary"

// TODO: change isDelete to record type to support more types of records, such as transaction record
// record header: crc | isDelete | key size | value size
// len:   		   4        1       max 5        max 5

const MaxHeaderSize = binary.MaxVarintLen32*2 + 5

type RecordHeader struct {
	Crc       uint32 // 4 bytes
	KeySize   int64  // variable, max len = 5 bytes
	ValueSize int64  // variable, max len = 5 bytes
	IsDelete  bool   // 1 byte
}

type Record struct {
	Key      []byte
	Value    []byte
	IsDelete bool
}

type RecordPos struct {
	Fid    uint32 // file id
	Size   uint32 // value size
	Offset int64  // value position
}
