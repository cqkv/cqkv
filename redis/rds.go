package redis

import (
	"errors"
	"time"

	"github.com/cqkv/cqkv"
	"github.com/cqkv/cqkv/redis/model"
)

type RdsServer struct {
	db *cqkv.DB
}

func NewRdsServer(dir string, ops ...cqkv.Option) (*RdsServer, error) {
	db, err := cqkv.Open(dir, ops...)
	if err != nil {
		return nil, err
	}
	return &RdsServer{db: db}, nil
}

func (rds *RdsServer) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RdsServer) Type(key []byte) (string, error) {
	meta, err := rds.db.Get(key)
	if err != nil {
		if errors.Is(err, cqkv.ErrNoRecord) {
			return "none", nil
		}
		return "", err
	}

	var t string
	switch meta[0] {
	case model.StringType:
		t = "string"
	case model.HashType:
		t = "hash"
	case model.ListType:
		t = "list"
	case model.SetType:
		t = "set"
	case model.ZSetType:
		t = "zset"
	default:
		t = "none"
	}

	return t, err
}

func (rds *RdsServer) Set(key []byte, value []byte, ttl time.Duration) error {
	str := model.NewString()

	v := str.Marshal(ttl, value)

	return rds.db.Put(key, v)
}

func (rds *RdsServer) Get(key []byte) ([]byte, error) {
	encodeValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	str := model.NewString()
	return str.Unmarshal(encodeValue)
}

// HSet only the field id is not exist, return true
func (rds *RdsServer) HSet(key, fieldId, value []byte) (bool, error) {
	// get metadata
	meta, err := rds.getMetadata(key, model.HashType)
	if err != nil && !errors.Is(err, cqkv.ErrNoRecord) {
		return false, err
	}

	var metaExist = true
	if errors.Is(err, cqkv.ErrNoRecord) {
		meta = model.NewMetadata(model.HashType, 0)
		metaExist = false
	}

	h := model.NewHash(key, meta.Version)
	hk := h.MarshalHashKey(fieldId)

	// exist to indicate whether the field id exist
	var exist = true
	if _, err = rds.db.Get(hk); errors.Is(err, cqkv.ErrNoRecord) {
		exist = false
	}

	// use write batch to maintain data consistency
	wb := rds.db.NewWriteBatch()
	if !metaExist {
		// write metadata first
		_ = wb.Put(key, model.MarshalMetadata(meta))
	}
	_ = wb.Put(hk, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RdsServer) HGet(key, fieldId []byte) ([]byte, error) {
	// get metadata
	meta, err := rds.getMetadata(key, model.HashType)
	if err != nil {
		return nil, err
	}

	h := model.NewHash(key, meta.Version)
	hk := h.MarshalHashKey(fieldId)

	return rds.db.Get(hk)
}

// HDel only the fileId exist, return true
func (rds *RdsServer) HDel(key, fieldId []byte) (bool, error) {
	meta, err := rds.getMetadata(key, model.HashType)
	if err != nil {
		if errors.Is(err, cqkv.ErrNoRecord) {
			return false, nil
		}
		return false, err
	}

	h := model.NewHash(key, meta.Version)
	hk := h.MarshalHashKey(fieldId)

	if _, err = rds.db.Get(hk); err != nil {
		if errors.Is(err, cqkv.ErrNoRecord) {
			return false, nil
		}
		return false, err
	}

	err = rds.db.Delete(hk)
	if err != nil {
		return false, err
	}
	return true, err
}

// SAdd only the member is not exist, return true
func (rds *RdsServer) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.getMetadata(key, model.SetType)
	if err != nil {
		if errors.Is(err, cqkv.ErrNoRecord) {
			return false, nil
		}
		return false, err
	}

	var metaExist = true
	if errors.Is(err, cqkv.ErrNoRecord) {
		meta = model.NewMetadata(model.SetType, 0)
		metaExist = false
	}

	s := model.NewSet(meta.Version)
	sk := s.MarshalKey(key, member)

	// exist to indicate whether the member exist
	var exist = true
	if _, err = rds.db.Get(sk); errors.Is(err, cqkv.ErrNoRecord) {
		exist = false
	}

	wb := rds.db.NewWriteBatch()
	if !metaExist {
		_ = wb.Put(key, model.MarshalMetadata(meta))
	}
	if !exist {
		_ = wb.Put(sk, nil)
	}

	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, err
}

func (rds *RdsServer) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.getMetadata(key, model.SetType)
	if err != nil {
		return false, err
	}

	s := model.NewSet(meta.Version)
	sk := s.MarshalKey(key, member)

	if _, err = rds.db.Get(sk); err != nil {
		if errors.Is(err, cqkv.ErrNoRecord) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (rds *RdsServer) SRem(key, member []byte) (bool, error) {
	meta, err := rds.getMetadata(key, model.SetType)
	if err != nil {
		if errors.Is(err, cqkv.ErrNoRecord) {
			return false, nil
		}
		return false, err
	}

	s := model.NewSet(meta.Version)
	sk := s.MarshalKey(key, member)

	if _, err = rds.db.Get(sk); errors.Is(err, cqkv.ErrNoRecord) {
		return false, nil
	}

	err = rds.db.Delete(sk)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (rds *RdsServer) LPush(key, element []byte) (uint32, error) {
	return rds.push(key, element, true)
}

func (rds *RdsServer) RPush(key, element []byte) (uint32, error) {
	return rds.push(key, element, false)
}

func (rds *RdsServer) LPop(key []byte) ([]byte, error) {
	return rds.pop(key, true)
}

func (rds *RdsServer) RPop(key []byte) ([]byte, error) {
	return rds.pop(key, false)
}

func (rds *RdsServer) push(key, element []byte, isLeft bool) (uint32, error) {
	meta, err := rds.getMetadata(key, model.ListType)
	if err != nil && errors.Is(err, cqkv.ErrNoRecord) {
		return 0, err
	}

	if errors.Is(err, cqkv.ErrNoRecord) {
		meta = model.NewMetadata(model.ListType, 0)
	}

	var idx uint64
	if isLeft {
		idx = meta.Head - 1
		meta.Head--
	} else {
		idx = meta.Tail
		meta.Tail++
	}

	l := model.NewList(meta.Version, idx)
	lk := l.MarshalListKey(key)

	wb := rds.db.NewWriteBatch()
	_ = wb.Put(key, model.MarshalMetadata(meta))
	_ = wb.Put(lk, element)

	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return uint32(meta.Tail - meta.Head), nil
}

func (rds *RdsServer) pop(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.getMetadata(key, model.ListType)
	if err != nil {
		return nil, err
	}

	if meta.Tail-meta.Head == 0 {
		return nil, nil
	}

	var idx uint64
	if isLeft {
		idx = meta.Head
		meta.Head++
	} else {
		idx = meta.Tail - 1
		meta.Tail--
	}

	l := model.NewList(meta.Version, idx)
	lk := l.MarshalListKey(key)

	element, err := rds.db.Get(lk)
	if err != nil {
		return nil, err
	}

	// update metadata
	if err = rds.db.Put(key, model.MarshalMetadata(meta)); err != nil {
		return nil, err
	}

	return element, nil
}

func (rds *RdsServer) ZAdd(key, member []byte, score float64) (bool, error) {
	// TODO
	_, err := rds.getMetadata(key, model.ZSetType)
	if err != nil && !errors.Is(err, cqkv.ErrNoRecord) {
		return false, err
	}

	var metaExist = true
	if errors.Is(err, cqkv.ErrNoRecord) {
		//meta = model.NewMetadata(model.ZSetType, 0)
		metaExist = false
	}

	if !metaExist {
	}

	return true, nil
}

func (rds *RdsServer) ZScore() {
	// TODO
}

func (rds *RdsServer) getMetadata(key []byte, dataType model.RdsType) (*model.Metadata, error) {
	metaData, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	meta := model.UnmarshalMetadata(metaData)

	// wrong data type
	if dataType != meta.DataType {
		return nil, model.ErrWrongTypeOp
	}

	// key is expired
	if meta.Expire > 0 && meta.Expire > time.Now().UnixNano() {
		return nil, cqkv.ErrNoRecord
	}

	return meta, nil
}
