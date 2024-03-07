package model

import (
	"encoding/binary"
	"time"
)

const (
	metadataMaxSize = 1 + binary.MaxVarintLen64*2
	extraListSize   = binary.MaxVarintLen64 * 2
)

type Metadata struct {
	DataType   RdsType
	Expire     int64
	Version    int64
	Head, Tail uint64 // for list
}

func NewMetadata(dataType RdsType, ttl time.Duration) *Metadata {
	var expire int64
	if ttl > 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	meta := &Metadata{
		DataType: dataType,
		Expire:   expire,
		Version:  time.Now().UnixNano(),
	}

	if dataType == ListType {
		meta.Head = initialListMark
		meta.Tail = initialListMark
	}

	return meta
}

func MarshalMetadata(mt *Metadata) []byte {
	sz := metadataMaxSize
	if mt.DataType == ListType {
		sz += extraListSize
	}

	buf := make([]byte, sz)
	buf[0] = mt.DataType

	idx := 1
	idx += binary.PutVarint(buf[idx:], mt.Expire)

	idx += binary.PutVarint(buf[idx:], mt.Version)

	if mt.DataType == ListType {
		idx += binary.PutUvarint(buf[idx:], mt.Head)
		idx += binary.PutUvarint(buf[idx:], mt.Tail)
	}

	return buf[:idx]
}

func UnmarshalMetadata(buf []byte) *Metadata {
	dataType := buf[0]

	idx := 1
	expire, n := binary.Varint(buf[idx:])
	idx += n

	version, n := binary.Varint(buf[idx:])
	idx += n

	meta := &Metadata{
		DataType: dataType,
		Expire:   expire,
		Version:  version,
	}

	if dataType == ListType {
		head, n := binary.Uvarint(buf[idx:])
		idx += n
		tail, _ := binary.Uvarint(buf[idx:])
		meta.Head = head
		meta.Tail = tail
	}

	return meta
}
