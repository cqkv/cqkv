package codec

import (
	"github.com/cqkv/cqkv/model"
)

type CodecImpl struct{}

func NewCodecImpl() *CodecImpl {
	return &CodecImpl{}
}

// MarshalRecordHeader return header data and data size
func (cl *CodecImpl) MarshalRecordHeader(header *model.RecordHeader) ([]byte, int64) {
	// TODO
	return nil, 0
}

func (cl *CodecImpl) UnmarshalRecordHeader(headerData []byte, header *model.RecordHeader) (int64, error) {
	// TODO
	return 0, nil
}

// MarshalRecord return record data and the data size
func (cl *CodecImpl) MarshalRecord(record *model.Record) ([]byte, int64) {
	data := make([]byte, len(record.Key)+len(record.Value))
	data = append(data, record.Key...)
	data = append(data, record.Value...)
	return data, int64(len(data))
}

func (cl *CodecImpl) UnmarshalRecord(data []byte, header *model.RecordHeader, record *model.Record) error {
	kz, vz := header.KeySize, header.ValueSize
	record.Key = data[:kz]
	record.Value = data[kz : kz+vz]
	return nil
}
