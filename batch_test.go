package cqkv

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func TestWriteBatch(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch()
	assert.NotNil(t, wb)

	// do not commit
	err = wb.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)
	err = wb.Delete([]byte("key2"))
	assert.Nil(t, err)

	_, err = db.Get([]byte("key1"))
	assert.Equal(t, ErrNoRecord, err)

	// commit
	err = wb.Commit()
	assert.Nil(t, err)

	value, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, "value1", string(value))

	// delete valid data
	wb2 := db.NewWriteBatch()
	err = wb2.Delete([]byte("key1"))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get([]byte("key1"))
	assert.Equal(t, ErrNoRecord, err)
}

func TestWriteBatchAfterRestart(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch()
	assert.NotNil(t, wb)
	err = wb.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)
	err = wb.Delete([]byte("key2"))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put([]byte("key3"), []byte("value3"))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	// restart
	_ = db.Close()
	db, err = Open("./tmp/")
	assert.Nil(t, err)
	assert.NotNil(t, db)

	value, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, "value1", string(value))

	assert.Equal(t, uint64(2), wb.db.txSeq)
}

func TestWriteBatchP(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch()
	for i := 0; i < 1000; i++ {
		err = wb.Put([]byte(fmt.Sprintf("key-%v", rand.Int())), []byte(fmt.Sprintf("value-%v", rand.Int())))
		assert.Nil(t, err)
	}

	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))

	err = wb.Commit()
	assert.Nil(t, err)

	keys = db.ListKeys()
	assert.Equal(t, 1000, len(keys))
}
