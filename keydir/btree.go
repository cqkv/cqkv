package keydir

import (
	"bytes"
	"fmt"
	"github.com/cqkv/cqkv/model"
	"github.com/google/btree"
	"sync"
	"time"
)

var _ Keydir = (*BTree)(nil)

const defaultDegree = 32

// BTree implement the keydir
type BTree struct {
	tree *btree.BTree

	// be cautious!!!
	// lock should be caught before concurrent write
	lock *sync.RWMutex
}

// Item implement the btree.Item interface
type Item struct {
	key []byte
	pos *model.RecordPos
}

func (i Item) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*Item).key) == -1
}

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
	start := time.Now()
	bt.tree.ReplaceOrInsert(item)
	fmt.Println("btree put time:", time.Since(start))
	fmt.Println()
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

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	bt.tree.Clear(false)
	return nil
}

func (bt *BTree) Iterator() Iterator {
	return bt.newBtreeIterator()
}

type btreeIterator struct {
	values []*Item
	curIdx int
}

func (bt *BTree) newBtreeIterator() *btreeIterator {
	iterator := &btreeIterator{
		values: make([]*Item, bt.tree.Len()),
		curIdx: 0,
	}

	var idx int
	getValues := func(item btree.Item) bool {
		iterator.values[idx] = item.(*Item)
		idx++
		return true
	}

	bt.tree.Ascend(getValues)

	return iterator
}

func (bti *btreeIterator) Rewind() {
	bti.curIdx = 0
}

func (bti *btreeIterator) Next() {
	bti.curIdx++
}

func (bti *btreeIterator) Valid() bool {
	return bti.curIdx < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curIdx].key
}

func (bti *btreeIterator) Value() *model.RecordPos {
	return bti.values[bti.curIdx].pos
}

func (bti *btreeIterator) Close() {

}
