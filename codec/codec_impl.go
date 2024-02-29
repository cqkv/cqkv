package codec

import (
	"encoding/binary"
	"io"

	"github.com/cqkv/cqkv/model"
)

type CodecImpl struct{}

func NewCodecImpl() *CodecImpl {
	return &CodecImpl{}
}

/*
default codec:
	- header: crc(4) + isDelete(1) + keySize(varint) + valueSize(varint) (max 15 bytes)
	- record: key + value (record raw data, you can implement your own codec to marshal/unmarshal record data)
	crc | isDelete | keySize | valueSize | key | value
*/

// MarshalRecordHeader return header data and data size
func (cl *CodecImpl) MarshalRecordHeader(header *model.RecordHeader) ([]byte, int64, error) {
	data := make([]byte, model.MaxHeaderSize)

	// crc
	binary.BigEndian.PutUint32(data[:4], header.Crc)

	// isDelete
	if header.IsDelete {
		data[4] = 1
	}

	// key size and value size
	idx := 5
	idx += binary.PutVarint(data[idx:], header.KeySize)
	idx += binary.PutVarint(data[idx:], header.ValueSize)

	return data, int64(idx), nil
}

func (cl *CodecImpl) UnmarshalRecordHeader(headerData []byte, header *model.RecordHeader) (int64, error) {
	if len(headerData) < 5 {
		return 0, io.EOF
	}

	// get crc
	crc := binary.BigEndian.Uint32(headerData[:4])

	// get isDelete
	var isDelete bool
	switch headerData[4] {
	case 1:
		isDelete = true
	}

	// get key size and value size
	idx := 5
	keySize, n := binary.Varint(headerData[idx:])
	idx += n

	valueSize, n := binary.Varint(headerData[idx:])
	idx += n

	header.Crc = crc
	header.IsDelete = isDelete
	header.KeySize = keySize
	header.ValueSize = valueSize

	return int64(idx), nil
}

// MarshalRecord return record data and the data size
func (cl *CodecImpl) MarshalRecord(record *model.Record) ([]byte, int64, error) {
	data := make([]byte, 0, len(record.Key)+len(record.Value))
	data = append(data, record.Key...)
	data = append(data, record.Value...)
	return data, int64(len(data)), nil
}

func (cl *CodecImpl) UnmarshalRecord(data []byte, header *model.RecordHeader, record *model.Record) error {
	kz, vz := header.KeySize, header.ValueSize
	record.Key = data[:kz]
	record.Value = data[kz : kz+vz]
	return nil
}

func (cl *CodecImpl) MarshalRecordPos(pos *model.RecordPos) ([]byte, error) {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index], nil
}

func (cl *CodecImpl) UnmarshalRecordPos(buf []byte, pos *model.RecordPos) error {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Varint(buf[index:])
	pos.Fid = uint32(fileId)
	pos.Offset = offset
	pos.Size = uint32(size)
	return nil
}
