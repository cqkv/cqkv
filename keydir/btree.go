package keydir

import (
	"sync"

	"github.com/cqkv/cqkv/model"

	"github.com/google/btree"
)

var _ Keydir = (*BTree)(nil)

// BTree implement the keydir
type BTree struct {
	tree *btree.BTree

	// be cautious!!!
	// lock should be caught before concurrent write
	lock *sync.RWMutex
}

const defaultDegree = 32

func NewBTree(degree int) *BTree {
	if degree <= 0 {
		degree = defaultDegree
	}
	return &BTree{
		tree: btree.New(degree),
		lock: &sync.RWMutex{},
	}
}

func (bt *BTree) Put(key []byte, value *model.RecordPos) bool {
	item := &Item{
		key: key,
		pos: value,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(item)
	return true
}

func (bt *BTree) Get(key []byte) *model.RecordPos {
	item := &Item{
		key: key,
	}
	btItem := bt.tree.Get(item)
	if btItem == nil {
		return nil
	}
	return btItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	item := &Item{
		key: key,
	}
	bt.lock.Lock()
	res := bt.tree.Delete(item)
	bt.lock.Unlock()
	return res != nil
}
