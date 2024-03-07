package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFIleIO_Write(t *testing.T) {
	fio, err := NewFIleIO("./data")
	defer os.Remove("./data")
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	for i := 0; i < 3; i++ {
		n, err := fio.Write([]byte("hello"))
		assert.Nil(t, err)
		assert.Equal(t, 5, n)
	}
}

func TestFIleIO_Read(t *testing.T) {
	fio, err := NewFIleIO("./data")
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	buf := make([]byte, 5)
	n, err = fio.Read(buf, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
}

func TestFIleIO_Sync(t *testing.T) {
}

func TestFIleIO_Close(t *testing.T) {

}
