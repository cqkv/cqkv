package keydir

import (
	"github.com/cqkv/cqkv/model"
)

// Keydir defined the keydir interface
// you can use some other data structure once you implement this interface
// keydir should be concurrency-safe!!!
type Keydir interface {
	Put(key []byte, value *model.RecordPos) bool
	Get(key []byte) *model.RecordPos
	Delete(key []byte) bool
	Size() int
	Iterator() Iterator
}

type Iterator interface {
	// Rewind reset the iterator
	Rewind()

	// Valid check the validation for current key, used to stop the iteration
	Valid() bool

	Next()

	Key() []byte

	Value() *model.RecordPos

	Close()
}
