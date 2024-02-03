package codec

import "github.com/cqkv/cqkv/model"

// PbCodec TODO
type PbCodec struct {
}

func NewPbCodec() *PbCodec {
	return nil
}

func (pc *PbCodec) Marshal(*model.Record) ([]byte, int64) {
	return nil, 0
}

func (pc *PbCodec) Unmarshal([]byte, *model.Record) error {
	return nil
}
