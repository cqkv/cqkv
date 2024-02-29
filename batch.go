package cqkv

import (
	"encoding/binary"
	"github.com/cqkv/cqkv/model"
	"sync"
	"sync/atomic"
)

var (
	txFinishKey = []byte("cqkv-tx-finish")
)

const (
	noTransactionSeq uint64 = 0
)

// WriteBatch is the option for write batch
// the isolation level of the write batch is serializable
type WriteBatch struct {
	mu *sync.Mutex

	db            *DB
	options       *writeBatchOptions
	pendingWrites map[string]*model.Record
}

func (db *DB) NewWriteBatch(options ...WriteBatchOption) *WriteBatch {
	opts := defaultWriteBatchOptions

	for _, opt := range options {
		opt(opts)
	}

	return &WriteBatch{
		mu:            new(sync.Mutex),
		options:       opts,
		db:            db,
		pendingWrites: make(map[string]*model.Record),
	}
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if len(wb.pendingWrites) == wb.options.maxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// store record temporarily
	record := &model.Record{Key: key, Value: value}
	wb.pendingWrites[string(key)] = record
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if len(wb.pendingWrites) == wb.options.maxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// if the data does not exist, return directly
	recordPos := wb.db.options.keydir.Get(key)
	if recordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// store record temporarily
	record := &model.Record{Key: key, IsDelete: true}
	wb.pendingWrites[string(key)] = record
	return nil
}

func (wb *WriteBatch) Commit() error {
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len(wb.pendingWrites) > wb.options.maxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	seq := atomic.AddUint64(&wb.db.txSeq, 1)

	positions := make(map[string]*model.RecordPos)
	for _, record := range wb.pendingWrites {
		// write record to the file
		pos, err := wb.db.appendRecord(&model.Record{
			Key:      addTxSeqPrefix(record.Key, seq),
			Value:    record.Value,
			IsDelete: record.IsDelete,
		})
		if err != nil {
			return err
		}
		// update keydir must after all the records are written to the file
		// store the position of the record temporarily
		positions[string(record.Key)] = pos
	}

	// after all the records are written to the file
	// write a special record to the file to indicate the end of the transaction
	finishRecord := &model.Record{
		Key:   addTxSeqPrefix(txFinishKey, seq),
		Value: nil,
	}
	if _, err := wb.db.appendRecord(finishRecord); err != nil {
		return err
	}

	// sync the file
	if wb.options.sync && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// update keydir
	for _, record := range wb.pendingWrites {
		if record.IsDelete {
			wb.db.options.keydir.Delete(record.Key)
		} else {
			wb.db.options.keydir.Put(record.Key, positions[string(record.Key)])
		}
	}

	wb.pendingWrites = make(map[string]*model.Record)
	return nil
}

func addTxSeqPrefix(key []byte, seq uint64) []byte {
	encSeq := make([]byte, binary.MaxVarintLen64)

	n := binary.PutUvarint(encSeq, seq)
	return append(encSeq[:n], key...)
}

func parseTxSeqPrefix(key []byte) ([]byte, uint64) {
	seq, n := binary.Uvarint(key)
	return key[n:], seq
}
