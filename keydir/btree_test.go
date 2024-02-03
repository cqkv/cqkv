package keydir

import (
	"testing"

	"github.com/cqkv/cqkv/model"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree(32)

	res := bt.Put(nil, &model.RecordPos{
		Fid:    1,
		Size:   2,
		Offset: 3,
	})
	assert.True(t, res)

	res = bt.Put([]byte("a"), &model.RecordPos{
		Fid:    1,
		Size:   2,
		Offset: 3,
	})
	assert.True(t, res)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree(32)

	res := bt.Put(nil, &model.RecordPos{
		Fid:    1,
		Size:   2,
		Offset: 3,
	})
	assert.True(t, res)

	pos := bt.Get(nil)
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, uint32(2), pos.Size)
	assert.Equal(t, int64(3), pos.Offset)

	res = bt.Put([]byte("a"), &model.RecordPos{
		Fid:    1,
		Size:   2,
		Offset: 3,
	})
	assert.True(t, res)
	pos = bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos.Fid)

	res = bt.Put([]byte("a"), &model.RecordPos{
		Fid:    2,
		Size:   2,
		Offset: 3,
	})
	assert.True(t, res)

	pos = bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), pos.Fid)
	t.Log(pos)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree(32)

	res := bt.Put(nil, &model.RecordPos{
		Fid:    1,
		Size:   2,
		Offset: 3,
	})
	assert.True(t, res)

	pos := bt.Get(nil)
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, uint32(2), pos.Size)
	assert.Equal(t, int64(3), pos.Offset)

	res = bt.Put([]byte("a"), &model.RecordPos{
		Fid:    1,
		Size:   2,
		Offset: 3,
	})
	assert.True(t, res)
	pos = bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos.Fid)

	res = bt.Put([]byte("a"), &model.RecordPos{
		Fid:    2,
		Size:   2,
		Offset: 3,
	})
	assert.True(t, res)

	pos = bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), pos.Fid)
	t.Log(pos)

	ok := bt.Delete([]byte("a"))
	assert.Equal(t, true, ok)

	ok = bt.Delete([]byte("a"))
	assert.Equal(t, false, ok)
}
