package codec

import "github.com/cqkv/cqkv/model"

type Codec interface {
	// MarshalRecordHeader return header data and data size
	MarshalRecordHeader(*model.RecordHeader) ([]byte, int64)

	UnmarshalRecordHeader([]byte, *model.RecordHeader) (int64, error)

	// MarshalRecord return record data and the data size
	MarshalRecord(*model.Record) ([]byte, int64)

	UnmarshalRecord([]byte, *model.RecordHeader, *model.Record) error
}
