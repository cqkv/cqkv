package keydir

import (
	"bytes"

	"github.com/cqkv/cqkv/model"

	"github.com/google/btree"
)

// Keydir defined the keydir interface
// you can use some other data structure once you implement this interface
type Keydir interface {
	Put(key []byte, value *model.RecordPos) bool
	Get(key []byte) *model.RecordPos
	Delete(key []byte) bool
}

// Item implement the btree.Item interface
type Item struct {
	key []byte
	pos *model.RecordPos
}

func (i Item) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*Item).key) == -1
}
