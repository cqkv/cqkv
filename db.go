package cqkv

import (
	"encoding/binary"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cqkv/cqkv/fio"
	"github.com/cqkv/cqkv/model"
	"github.com/cqkv/cqkv/utils"
)

type DB struct {
	mu *sync.RWMutex

	activeFile *model.DataFile            // data will append to active data file
	olderFiles map[uint32]*model.DataFile // older files, read only
	fileIds    []uint32                   // only used in loading keydir

	options *options
}

// Open a bitcask instance with option params
func Open(dirPath string, ops ...Option) (*DB, error) {
	return newDB(dirPath, ops)
}

func newDB(dirPath string, o []Option) (*DB, error) {
	// create options
	ops := defaultOptions
	if dirPath != "" {
		ops.dirPath = dirPath
	}
	fileLock := fio.NewFlock(dirPath)
	ops.fileLock = fileLock

	for _, fn := range o {
		fn(ops)
	}

	// if ioManager is fio.FileIO, check dir
	if reflect.ValueOf(ops.ioManagerCreator).Pointer() != reflect.ValueOf(defaultIOManagerCreator).Pointer() {
		// check file lock
		if ops.fileLock == fileLock {
			return nil, ErrNeedFileLock
		}
	} else {
		if _, err := os.Stat(dirPath); !os.IsExist(err) {
			// create dir
			if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
				return nil, err
			}
		}

		// check whether current dir is used
		success, err := ops.fileLock.TryLock()
		if err != nil {
			return nil, err
		}
		if !success {
			return nil, ErrDirIsUsing
		}

		if _, err = os.ReadDir(dirPath); err != nil {
			return nil, err
		}
	}

	db := &DB{
		mu:         &sync.RWMutex{},
		activeFile: nil,
		olderFiles: make(map[uint32]*model.DataFile),
		options:    ops,
	}

	// TODO: load merge file

	// load data files
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// load keydir
	if err := db.loadKeydirFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
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
	pos, err := db.appendRecordWithLock(record)
	if err != nil {
		return err
	}

	if !db.options.keyDir.Put(key, pos) {
		return ErrUpdateKeydir
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	// get pos from keydir
	pos := db.options.keyDir.Get(key)
	if pos == nil {
		return nil, ErrNoRecord
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// get record from file
	record, err := db.get(pos)
	if err != nil {
		return nil, err
	}

	// record has deleted
	if record.IsDelete {
		return nil, ErrNoRecord
	}

	return record.Value, nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return nil
	}

	// if key is not in keydir, return
	if pos := db.options.keyDir.Get(key); pos == nil {
		return nil
	}

	// create record that isDelete is true
	record := &model.Record{
		Key:      key,
		IsDelete: true,
	}

	// write to data file
	if _, err := db.appendRecordWithLock(record); err != nil {
		return err
	}

	// update keydir
	ok := db.options.keyDir.Delete(key)
	if !ok {
		return ErrUpdateKeydir
	}

	return nil
}

func (db *DB) Close() error {
	defer func() {
		_ = db.options.fileLock.Unlock()
	}()

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, dataFile := range db.olderFiles {
		if err := dataFile.Close(); err != nil {
			return err
		}
	}
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	// sync active data file
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

func (db *DB) ListKey() [][]byte {
	// get iterator
	iterator := db.options.keyDir.Iterator()
	defer iterator.Close()

	keys := make([][]byte, 0, db.options.keyDir.Size())
	// iterate keydir
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys = append(keys, iterator.Key())
	}

	return keys
}

func (db *DB) Fold(handler func(key, value []byte) error) error {
	// get iterator
	iterator := db.options.keyDir.Iterator()
	defer iterator.Close()

	// iterate keydir
	db.mu.RLock()
	defer db.mu.RUnlock()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		pos := iterator.Value()
		record, err := db.get(pos)
		if err != nil {
			return err
		}

		if err = handler(key, record.Value); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Merge() error {
	// TODO
	return nil
}

func (db *DB) appendRecordWithLock(record *model.Record) (*model.RecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendRecord(record)
}

func (db *DB) appendRecord(record *model.Record) (*model.RecordPos, error) {
	// create data file if there is no active data file
	if db.activeFile == nil {
		if err := db.setActiveDatafile(); err != nil {
			return nil, err
		}
	}

	// marshal record
	data, size, err := db.marshalRecord(record)
	if err != nil {
		return nil, err
	}

	// value is too big
	if size > db.options.dataFileSize {
		return nil, ErrBigValue
	}

	// active file size + record size exceed the limit size
	// close current active file, create a new active size
	if db.activeFile.WriteOffset+size > db.options.dataFileSize {
		// set new	active data file
		if err = db.setActiveDatafile(); err != nil {
			return nil, err
		}
	}

	// write data to file
	writeOff := db.activeFile.WriteOffset
	if err = db.activeFile.Write(data); err != nil {
		return nil, err
	}

	// check whether to sync
	times := db.activeFile.WriteTimes
	if times%db.options.syncFre == 0 {
		if err = db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// create record position
	pos := &model.RecordPos{
		Fid:    db.activeFile.Fid,
		Size:   uint32(size),
		Offset: writeOff,
	}

	return pos, nil
}

func (db *DB) marshalRecord(record *model.Record) ([]byte, int64, error) {
	// create header
	header := &model.RecordHeader{
		IsDelete:  record.IsDelete,
		KeySize:   int64(len(record.Key)),
		ValueSize: int64(len(record.Value)),
	}

	// marshal header
	headerData, headerSize, err := db.options.codec.MarshalRecordHeader(header)
	if err != nil {
		return nil, 0, err
	}

	// marshal record
	recordData, recordSize, err := db.options.codec.MarshalRecord(record)
	if err != nil {
		return nil, 0, err
	}

	// merge header and record
	size := headerSize + recordSize
	data := make([]byte, size)
	copy(data[:headerSize], headerData[:headerSize])
	copy(data[headerSize:], recordData)

	// generate crc
	crc := utils.GenerateCrc(data[4:])
	binary.BigEndian.PutUint32(data[:4], crc)

	return data, size, nil
}

func (db *DB) setActiveDatafile() error {
	var initialFileId uint32

	oldActiveFile := db.activeFile
	if oldActiveFile != nil {
		initialFileId = oldActiveFile.Fid + 1
		// save old data file
		// old data file is read only, sync to disk first
		if err := oldActiveFile.Sync(); err != nil {
			return err
		}
		db.olderFiles[oldActiveFile.Fid] = oldActiveFile
	}

	dataFile := &model.DataFile{
		Fid: initialFileId,
	}

	var err error
	dataFile.IoManager, err = db.options.ioManagerCreator(db.options.dirPath, initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

func (db *DB) get(pos *model.RecordPos) (*model.Record, error) {
	var dataFile *model.DataFile
	if pos.Fid == db.activeFile.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}

	if dataFile == nil {
		return nil, ErrNoDataFile
	}

	record, _, err := db.getRecordFromDataFile(dataFile, pos.Offset)
	return record, err
}

func (db *DB) getRecordFromDataFile(dataFile *model.DataFile, offset int64) (*model.Record, int64, error) {
	// get primitive header data
	headerData, err := dataFile.ReadRecordHeader(offset)
	if err != nil {
		return nil, 0, err
	}

	recordHeader := new(model.RecordHeader)
	// unmarshal record header
	headerSize, err := db.options.codec.UnmarshalRecordHeader(headerData, recordHeader)
	if err != nil {
		return nil, 0, err
	}

	if recordHeader.Crc == 0 && recordHeader.KeySize == 0 && recordHeader.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	// get primitive record data
	keySize, valueSize := recordHeader.KeySize, recordHeader.ValueSize
	kvSize := keySize + valueSize

	data, err := dataFile.ReadRecord(offset+headerSize, kvSize)
	if err != nil {
		return nil, 0, err
	}

	// unmarshal record
	record := new(model.Record)
	if err = db.options.codec.UnmarshalRecord(data, recordHeader, record); err != nil {
		return nil, 0, err
	}
	record.IsDelete = recordHeader.IsDelete

	// check crc
	if !utils.CheckCrc(recordHeader.Crc, append(headerData[4:headerSize], data[:]...)) {
		return nil, 0, ErrWrongCrc
	}

	return record, headerSize + kvSize, nil
}

func (db *DB) loadDataFiles() error {
	// TODO: optimize to support various storage instance
	dir := db.options.dirPath
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var fileIds []uint32
	for _, entry := range entries {
		entryName := entry.Name()
		// data file's suffix is '.cq'
		if strings.HasSuffix(entryName, model.DataFileSuffix) {
			split := strings.Split(entryName, ".")
			id, err := strconv.Atoi(split[0])
			if err != nil {
				return ErrDataFileCorrupted
			}
			fileIds = append(fileIds, uint32(id))
		}
	}

	// the id of data file is incremented
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] < fileIds[j]
	})
	db.fileIds = fileIds // only used in loading keydir

	for i, id := range fileIds {
		// get io manager
		ioManager, err := db.options.ioManagerCreator(dir, id)
		if err != nil {
			return err
		}
		dataFile := model.OpenDataFile(id, ioManager)
		// the latest data file is active data file
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[id] = dataFile
		}
	}
	return nil
}

func (db *DB) loadKeydirFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	// get datafiles
	for _, fid := range db.fileIds {
		var dataFile *model.DataFile
		if fid == db.activeFile.Fid {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fid]
		}

		if dataFile == nil {
			return ErrNoDataFile
		}

		// read data file
		var offset int64
		for {
			record, size, err := db.getRecordFromDataFile(dataFile, offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// put pos into keydir
			pos := &model.RecordPos{
				Fid:    fid,
				Size:   uint32(size),
				Offset: offset,
			}

			// record may be deleted
			var ok bool
			if record.IsDelete {
				ok = db.options.keyDir.Delete(record.Key)
			} else {
				ok = db.options.keyDir.Put(record.Key, pos)
			}
			if !ok {
				return ErrUpdateKeydir
			}

			// update offset
			offset += size
		}

		// update active file write offset
		if fid == db.activeFile.Fid {
			db.activeFile.WriteOffset = offset
		}
	}

	return nil
}
