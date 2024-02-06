package codec

import (
	"github.com/cqkv/cqkv/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func newCodecImpl() *CodecImpl {
	return NewCodecImpl()
}

func TestCodecImpl_MarshalRecordHeader(t *testing.T) {
	cl := newCodecImpl()
	header := &model.RecordHeader{
		Crc:       123,
		IsDelete:  true,
		KeySize:   1 + 1<<7,
		ValueSize: 2,
	}
	data, size, err := cl.MarshalRecordHeader(header)
	assert.Nil(t, err)
	assert.NotNil(t, data)
	t.Log(data)
	assert.Equal(t, 8, int(size))
}

func TestCodecImpl_UnmarshalRecordHeader(t *testing.T) {
	cl := newCodecImpl()
	header := &model.RecordHeader{}
	data := []byte{0, 0, 0, 123, 1, 130, 2, 4}
	size, err := cl.UnmarshalRecordHeader(data, header)
	assert.Nil(t, err)
	assert.Equal(t, int64(8), size)
	assert.Equal(t, uint32(123), header.Crc)
	assert.Equal(t, true, header.IsDelete)
	assert.Equal(t, int64(1+1<<7), header.KeySize)
	assert.Equal(t, int64(2), header.ValueSize)
}

func TestCodecImpl_MarshalRecord(t *testing.T) {
	cl := newCodecImpl()
	record := &model.Record{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	data, size, err := cl.MarshalRecord(record)
	assert.Nil(t, err)
	assert.NotNil(t, data)
	t.Log(data)
	assert.Equal(t, 8, int(size))
}

func TestCodecImpl_UnmarshalRecord(t *testing.T) {
	cl := newCodecImpl()
	header := &model.RecordHeader{
		KeySize:   3,
		ValueSize: 5,
	}
	record := &model.Record{}
	data := []byte("keyvalue")
	err := cl.UnmarshalRecord(data, header, record)
	assert.Nil(t, err)
	assert.Equal(t, []byte("key"), record.Key)
	assert.Equal(t, []byte("value"), record.Value)
}
