package benchmark

import (
	"errors"
	"github.com/cqkv/cqkv"
	"github.com/cqkv/cqkv/fio"
	"github.com/stretchr/testify/assert"
	"log"
	"strconv"
	"testing"
)

var db *cqkv.DB

func init() {
	var err error
	db, err = cqkv.Open("./tmp/")
	if err != nil {
		panic(err)
	}
}

// Benchmark_Put .
func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put([]byte("key"+strconv.Itoa(i)), []byte("value"+strconv.Itoa(i)))
		assert.Nil(b, err)
	}
}

// Benchmark_Get .
func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put([]byte("key"+strconv.Itoa(i)), []byte("value"+strconv.Itoa(i)))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get([]byte("key" + strconv.Itoa(i)))
		if err != nil || !errors.Is(err, cqkv.ErrNoRecord) {
			b.Fatal(err)
		}
	}
}

// Benchmark_Delete .
func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Delete([]byte("key" + strconv.Itoa(i)))
		assert.Nil(b, err)
	}
}

func Benchmark_IO(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	dataFile, err := fio.NewFIleIO("./data")
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		dataFile.Write([]byte("11111111111111111111"))
	}
}
