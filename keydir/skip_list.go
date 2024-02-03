package keydir

import "github.com/cqkv/cqkv/model"

// SkipList not implement
type SkipList struct {
}

func NewSkipList() *SkipList {
	return nil
}

func (sl *SkipList) Put(key []byte, value *model.RecordPos) bool {
	return true
}

func (sl *SkipList) Get(key []byte) *model.RecordPos {
	return nil
}

func (sl *SkipList) Delete(key []byte) bool {
	return true
}
