package cqkv

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"testing"
)

func TestDB_Merge_WithNoData(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	done := db.Merge()
	select {
	case err := <-done:
		assert.Nil(t, err)
	}
}

func TestDB_Merge_WithAllValidData(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 100; i++ {
		err := db.Put([]byte(fmt.Sprintf("key-%v", rand.Int())), []byte(fmt.Sprintf("value-%v", rand.Int())))
		assert.Nil(t, err)
	}

	done := db.Merge()
	select {
	case err := <-done:
		assert.Nil(t, err)
	}
	assert.Equal(t, 100, len(db.ListKeys()))
}

func TestDB_Merge_WithSomeInvalidData(t *testing.T) {
	db, err := Open("./tmp/")
	defer func() {
		_ = os.RemoveAll("./tmp/")
	}()
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 10; i++ {
		err = db.Put([]byte(fmt.Sprintf("key-%v", i)), []byte(fmt.Sprintf("value-%v", i)))
		assert.Nil(t, err)
	}

	// delete some keys
	for i := 0; i < 5; i++ {
		err = db.Delete([]byte(fmt.Sprintf("key-%v", i)))
		assert.Nil(t, err)
	}

	done := db.Merge()
	select {
	case err = <-done:
		assert.Nil(t, err)
	}

	err = db.Close()
	assert.Nil(t, err)

	db, err = Open("./tmp/")
	assert.Nil(t, err)
	assert.NotNil(t, db)

	assert.Equal(t, 5, len(db.ListKeys()))
}
