package model

type Record struct {
	Crc       uint32
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
	IsDelete  bool
}

type RecordPos struct {
	Fid    uint32 // file id
	Size   uint32 // value size
	Offset int64  // value position
}
