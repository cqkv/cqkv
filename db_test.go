package cqkv

import (
	"fmt"
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
	pos := db.options.keydir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key2"), []byte("value2"))
	pos = db.options.keydir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key"), []byte("value1"))
	assert.Nil(t, err)
	pos = db.options.keydir.Get([]byte("key"))
	assert.NotNil(t, pos)
}

func TestDB_Put2(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 10; i++ {
		err = db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		assert.Nil(t, err)
	}

	//f, err := os.OpenFile("./data", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	//if err != nil {
	//	panic(err)
	//}
	//defer f.Close()
	//data := make([]byte, 20)
	//for i := 0; i < 1000; i++ {
	//	start := time.Now()
	//	_, err = f.Write(data)
	//	if err != nil {
	//		panic(err)
	//	}
	//	fmt.Println(time.Since(start))
	//}
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
	pos := db.options.keydir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key2"), []byte("value2"))
	pos = db.options.keydir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key"), []byte("value1"))
	assert.Nil(t, err)
	pos = db.options.keydir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Close()
	assert.Nil(t, err)

	db, err = Open("./tmp/")
	assert.Nil(t, err)
	assert.NotNil(t, db)

	value, err := db.Get([]byte("key"))
	assert.Equal(t, "value1", string(value))
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
	pos := db.options.keydir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key2"), []byte("value2"))
	pos = db.options.keydir.Get([]byte("key"))
	assert.NotNil(t, pos)

	err = db.Put([]byte("key"), []byte("value1"))
	assert.Nil(t, err)
	pos = db.options.keydir.Get([]byte("key"))
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

	keys := db.ListKeys()
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

func TestDB_Close2(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50; i++ {
		err = db.Put([]byte(fmt.Sprintf("key-%v", i)), []byte(fmt.Sprintf("value-%v", i)))
		assert.Nil(t, err)
	}

	for i := 0; i < 25; i++ {
		err = db.Delete([]byte(fmt.Sprintf("key-%v", i)))
		assert.Nil(t, err)
	}

	err = db.Close()
	assert.Nil(t, err)

	db, err = Open("./tmp/")
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	assert.Equal(t, 25, len(keys))
}

func Test(t *testing.T) {
	db, err := Open("./tmp/")
	assert.Nil(t, err)
	assert.NotNil(t, db)

	v, err := db.Get([]byte("key6"))
	if err != nil {
		t.Log(err)
	}
	t.Log(string(v))

	err = db.Delete([]byte("key6"))
	if err != nil {
		t.Log(err)
	}
	v, err = db.Get([]byte("key6"))
	if err != nil {
		t.Log(err)
	}
	t.Log(string(v))
}
