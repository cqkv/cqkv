package keydir

import "github.com/cqkv/cqkv/model"

// SkipList not implement
// TODO
type SkipList struct {
	len int
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

func (sl *SkipList) Iterator() Iterator {
	return nil
}

func (sl *SkipList) Size() int {
	return sl.len
}

func (sl *SkipList) Close() error {
	return nil
}
