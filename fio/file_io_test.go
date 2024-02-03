package fio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFIleIO_Write(t *testing.T) {
	fio, err := NewFIleIO("./data")
	if err != nil {
		assert.Error(t, err, "new file io manager failed")
	}

	n, err := fio.Write([]byte("hello"))
	if err != nil {
		assert.Error(t, err, "fail to write data")
	}
	assert.Equal(t, int(5), n)
}

func TestFIleIO_Read(t *testing.T) {
	fio, err := NewFIleIO("./data")
	if err != nil {
		assert.Error(t, err, "new file io manager failed")
	}

	n, err := fio.Write([]byte("hello"))
	if err != nil {
		assert.Error(t, err, "fail to write data")
	}
	assert.Equal(t, int(5), n)

	buf := make([]byte, 5)
	n, err = fio.Read(buf, 0)
	if err != nil {
		assert.Error(t, err, "fail to read data")
	}
	assert.Equal(t, int(5), n)
}

func TestFIleIO_Sync(t *testing.T) {

}

func TestFIleIO_Close(t *testing.T) {

}
