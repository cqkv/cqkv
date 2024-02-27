package cqkv

import (
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
)

func TestOpen(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	assert.Equal(t, true, reflect.ValueOf(db.options.ioManagerCreator).Pointer() == reflect.ValueOf(defaultIOManagerCreator).Pointer())
}

func TestDB_Put(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)
	pos := db.options.keyDir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key2"), []byte("value2"))
	pos = db.options.keyDir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key"), []byte("value1"))
	assert.Nil(t, err)
	pos = db.options.keyDir.Get([]byte("key"))
	assert.NotNil(t, pos)
}

func TestDB_Get(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	value, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, "value1", string(value))

	err = db.Put([]byte("key2"), []byte("value2"))
	value, err = db.Get([]byte("key2"))
	assert.Nil(t, err)
	assert.Equal(t, "value2", string(value))

	err = db.Put([]byte("key1"), []byte("value3"))
	assert.Nil(t, err)

	value, err = db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, "value3", string(value))
}

func TestDB_Delete(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	value, err := db.Get([]byte("key1"))
	assert.Nil(t, err)
	assert.Equal(t, "value1", string(value))

	err = db.Delete([]byte("key1"))
	assert.Nil(t, err)

	value, err = db.Get([]byte("key1"))
	assert.NotNil(t, err)
	assert.Nil(t, value)
	assert.NotNil(t, err, ErrNoRecord)
}

func TestDB_Close(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)
	pos := db.options.keyDir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key2"), []byte("value2"))
	pos = db.options.keyDir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key"), []byte("value1"))
	assert.Nil(t, err)
	pos = db.options.keyDir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDBOpenWithFiles(t *testing.T) {
	db, err := Open("./tmp/")
	assert.Nil(t, err)
	assert.NotNil(t, db)

	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()

	err = db.Put([]byte("key"), []byte("value"))
	assert.Nil(t, err)
	pos := db.options.keyDir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key2"), []byte("value2"))
	pos = db.options.keyDir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key"), []byte("value1"))
	assert.Nil(t, err)
	pos = db.options.keyDir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Close()
	assert.Nil(t, err)

	db, err = Open("./tmp/")
	assert.Nil(t, err)
	assert.NotNil(t, db)

	value, err := db.Get([]byte("key"))
	assert.Nil(t, err)
	assert.Equal(t, "value1", string(value))
}

func TestDB_ListKey(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	err = db.Put([]byte("key2"), []byte("value2"))
	assert.Nil(t, err)

	keys := db.ListKey()
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, "key1", string(keys[0]))
	assert.Equal(t, "key2", string(keys[1]))
}

func TestDB_Fold(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key1"), []byte("value1"))
	assert.Nil(t, err)

	err = db.Put([]byte("key2"), []byte("value2"))
	assert.Nil(t, err)

	err = db.Fold(func(key, value []byte) error {
		t.Log(string(key), string(value))
		return nil
	})
	assert.Nil(t, err)
}
