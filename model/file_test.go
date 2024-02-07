package model

import (
	"github.com/cqkv/cqkv/fio"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dir := "./tmp"
	ioManager, err := fio.NewFIleIO(dir)
	defer func() {
		_ = os.Remove(dir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, ioManager)

	dataFile := OpenDataFile(0, ioManager)
	assert.NotNil(t, dataFile)
}

func TestDataFile_Write(t *testing.T) {
	dir := "./tmp"
	ioManager, err := fio.NewFIleIO(dir)
	defer func() {
		_ = os.Remove(dir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, ioManager)

	dataFile := OpenDataFile(0, ioManager)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)
	assert.Equal(t, int64(3), dataFile.WriteOffset)

	err = dataFile.Write([]byte("bbb"))
	assert.Nil(t, err)
	assert.Equal(t, int64(6), dataFile.WriteOffset)

	err = dataFile.Write([]byte("ccc"))
	assert.Nil(t, err)
	assert.Equal(t, int64(9), dataFile.WriteOffset)
}

func TestDataFile_ReadRecordHeader(t *testing.T) {
	dir := "./tmp"
	ioManager, err := fio.NewFIleIO(dir)
	defer func() {
		_ = os.Remove(dir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, ioManager)

	dataFile := OpenDataFile(0, ioManager)
	assert.NotNil(t, dataFile)

	header := []byte{0, 0, 0, 123, 1, 130, 2, 4}
	err = dataFile.Write(header)
	assert.Nil(t, err)

	data, err := dataFile.ReadRecordHeader(0)
	assert.Nil(t, err)
	assert.Equal(t, header, data)

	data, err = dataFile.ReadRecordHeader(1)
	assert.Nil(t, err)
	assert.Equal(t, header[1:], data)

	err = dataFile.Write(header)
	assert.Nil(t, err)

	data, err = dataFile.ReadRecordHeader(8)
	assert.Nil(t, err)
	assert.Equal(t, header, data)
}

func TestDataFile_ReadRecord(t *testing.T) {
	dir := "./tmp"
	ioManager, err := fio.NewFIleIO(dir)
	defer func() {
		_ = os.Remove(dir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, ioManager)

	dataFile := OpenDataFile(0, ioManager)
	assert.NotNil(t, dataFile)

	data := []byte{0, 0, 0, 123, 1, 130, 2, 4}
	err = dataFile.Write(data)
	assert.Nil(t, err)

	readData, err := dataFile.ReadRecord(0, 8)
	assert.Nil(t, err)
	assert.Equal(t, data, readData)

	readData, err = dataFile.ReadRecord(1, 7)
	assert.Nil(t, err)
	assert.Equal(t, data[1:], readData)

	readData, err = dataFile.ReadRecord(0, 4)
	assert.Nil(t, err)
	assert.Equal(t, data[:4], readData)
}

func TestDataFile_Sync(t *testing.T) {
	dir := "./tmp"
	ioManager, err := fio.NewFIleIO(dir)
	defer func() {
		_ = os.Remove(dir)
	}()
	assert.Nil(t, err)
	assert.NotNil(t, ioManager)

	dataFile := OpenDataFile(0, ioManager)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}
