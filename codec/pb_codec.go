package codec

import "github.com/cqkv/cqkv/model"

// PbCodec TODO
type PbCodec struct {
}

func NewPbCodec() *PbCodec {
	return nil
}

func (pc *PbCodec) MarshalRecord(*model.Record) ([]byte, int64) {
	return nil, 0
}

func (pc *PbCodec) UnmarshalRecord([]byte, *model.Record) error {
	return nil
}
