package codec

import "github.com/cqkv/cqkv/model"

type Codec interface {
	// Marshal return data and the data size
	Marshal(*model.Record) ([]byte, int64)

	Unmarshal([]byte, *model.Record) error
}
