package codec

import "github.com/cqkv/cqkv/model"

type PbCode struct {
}

func (pc *PbCode) Marshal(*model.Record) ([]byte, error) {
	return nil, nil
}

func (pc *PbCode) Unmarshal([]byte, *model.Record) error {
	return nil
}
