package codec

import "github.com/cqkv/cqkv/model"

type Codec interface {
	// MarshalRecordHeader return header data and data size
	MarshalRecordHeader(*model.RecordHeader) ([]byte, int64, error)

	UnmarshalRecordHeader([]byte, *model.RecordHeader) (int64, error)

	// MarshalRecord return record data and the data size
	MarshalRecord(*model.Record) ([]byte, int64, error)

	UnmarshalRecord([]byte, *model.RecordHeader, *model.Record) error

	MarshalRecordPos(*model.RecordPos) ([]byte, error)

	UnmarshalRecordPos([]byte, *model.RecordPos) error
}
