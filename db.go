package cqkv

import (
	"github.com/cqkv/cqkv/model"
	"sync"
)

type DB struct {
	lock sync.Locker

	activeFile *model.DataFile            // data will append to active data file
	olderFiles map[uint32]*model.DataFile // older files, read only

	options options
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	// append record in active data file
	record := &model.Record{
		Key:   key,
		Value: value,
	}
	// write to file
	pos, err := db.appendRecordWithLock(record)
	if err != nil {
		return err
	}

	// TODO: update keydir

	return nil
}

func (db *DB) appendRecordWithLock(record *model.Record) (*model.RecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.appendRecord(record)
}

func (db *DB) appendRecord(record *model.Record) (*model.RecordPos, error) {
	// create data file if there is no active data file
	if db.activeFile == nil {
		if err := db.setActiveDatafile(); err != nil {
			return nil, err
		}
	}

	// codec record
	data, size := db.options.codec.Marshal(record)
	if size > db.options.dataFileSize {
		return nil, ErrBigValue
	}

	// active file size + record size exceed the limit size
	// close current active file, create a new active size
	if db.activeFile.WriteOffset+size > db.options.dataFileSize {
		// sync current active file data
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		if err := db.setActiveDatafile(); err != nil {
			return nil, err
		}
	}

	// TODO: write data to file

	return nil, nil
}

func (db *DB) setActiveDatafile() error {
	var initialFileId uint32

	oldActiveFile := db.activeFile
	if oldActiveFile != nil {
		initialFileId = oldActiveFile.Fid + 1
		// save old data file
		db.olderFiles[oldActiveFile.Fid] = oldActiveFile
	}

	dataFile := &model.DataFile{
		Fid:       initialFileId,
		IOManager: nil,
	}

	var err error
	dataFile.IOManager, err = db.options.iOManagerCreator(initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}
