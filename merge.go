package cqkv

import (
	"bytes"
	"github.com/cqkv/cqkv/model"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	mergeDirPathSuffix = "-cqkv-merge"
	mergeFinishedKey   = "merge.finished"
)

// Merge clear the invalid data and generate the hint file.
// it is asynchronous.
func (db *DB) Merge() chan error {
	done := make(chan error)
	go db.doMerge(done)
	return done
}

func (db *DB) doMerge(done chan<- error) {
	if db.activeFile == nil {
		sendNil(done)
		return
	}

	db.mu.Lock()
	if db.isMerging {
		db.mu.Unlock()
		sendError(done, ErrMergeIsProgress)
		return
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// sync the current active file
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		sendError(done, err)
		return
	}

	// change the active file to read-only
	db.olderFiles[db.activeFile.Fid] = db.activeFile
	// set new active file
	if err := db.setActiveDatafile(); err != nil {
		db.mu.Unlock()
		sendError(done, err)
		return
	}

	noMergeFid := db.activeFile.Fid
	mergeFiles := make([]*model.DataFile, 0, len(db.olderFiles))
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// old files is read-only, so no need to lock
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].Fid < mergeFiles[j].Fid
	})

	// create a new bitcask dir for the merge
	mergeDirPath := db.getMergeDirPath()
	// remove the old merge dir
	if _, err := os.Stat(mergeDirPath); err == nil {
		if err = os.RemoveAll(mergeDirPath); err != nil {
			sendError(done, err)
			return
		}
	}

	// create a new merge dir
	if err := os.MkdirAll(mergeDirPath, os.ModePerm); err != nil {
		sendError(done, err)
		return
	}

	mergeDb, err := Open(mergeDirPath)
	if err != nil {
		sendError(done, err)
		return
	}

	// write valid records to the merge db and generate the hint file
	hintIoManage, err := mergeDb.options.ioManagerCreator(model.GetDataFileName(mergeDirPath, model.HintFileType, 0))
	if err != nil {
		sendError(done, err)
		return
	}
	defer hintIoManage.Close()
	hintFile := model.OpenDataFile(0, hintIoManage)
	for _, dataFile := range mergeFiles {
		// read data file
		var offset int64
		for {
			// read record from the data file
			record, size, err := db.getRecordFromDataFile(dataFile, offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				sendError(done, err)
			}

			// check if the record is valid
			realKey, _ := parseTxSeqPrefix(record.Key)
			pos := db.options.keydir.Get(realKey)
			if pos != nil &&
				pos.Fid == dataFile.Fid &&
				pos.Offset == offset {
				// clear transaction flag
				record.Key = addTxSeqPrefix(realKey, noTransactionSeq)

				// write the record to the merge db
				if _, err = mergeDb.appendRecord(record); err != nil {
					sendError(done, err)
					return
				}

				posRecordData, err := db.marshalPosRecord(realKey, pos)
				if err != nil {
					sendError(done, err)
					return
				}

				if err = hintFile.Write(posRecordData); err != nil {
					sendError(done, err)
					return
				}
			}

			offset += size
		}
	}

	// sync the hint file and the merge db
	if err = hintFile.Sync(); err != nil {
		sendError(done, err)
		return
	}
	if err = mergeDb.Sync(); err != nil {
		sendError(done, err)
		return
	}

	// write the merge finished file
	if err = db.writeMergeFinishedFile(mergeDirPath, noMergeFid); err != nil {
		sendError(done, err)
		return
	}

	sendNil(done)
}

func (db *DB) marshalPosRecord(key []byte, pos *model.RecordPos) ([]byte, error) {
	// write the record pos to the hint file
	posRecordValue, err := db.options.codec.MarshalRecordPos(pos)
	if err != nil {
		return nil, err
	}
	posRecord := &model.Record{
		Key:   key,
		Value: posRecordValue,
	}
	posRecordData, _, err := db.marshalRecord(posRecord)
	if err != nil {
		return nil, err
	}

	return posRecordData, nil
}

func (db *DB) writeMergeFinishedFile(mergeDirPath string, fid uint32) error {
	// write the merge finished file
	mergeFinishedIoManager, err := db.options.ioManagerCreator(model.GetDataFileName(mergeDirPath, model.MergeFinishedFileType, 0))
	if err != nil {
		return err
	}
	defer mergeFinishedIoManager.Close()
	mergeFinishedDataFile := model.OpenDataFile(0, mergeFinishedIoManager)
	// the merge finished file store the noMerge id
	mergeFinishedRecord := &model.Record{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(fid))),
	}
	mergeFinishedRecordData, _, err := db.marshalRecord(mergeFinishedRecord)
	if err != nil {
		return err
	}
	if err = mergeFinishedDataFile.Write(mergeFinishedRecordData); err != nil {
		return err
	}

	if err = mergeFinishedDataFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergeDirPath() string {
	dir := path.Dir(path.Clean(db.options.dirPath))
	base := path.Base(db.options.dirPath)
	return path.Join(dir, base+mergeDirPathSuffix)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergeDirPath()
	// merge dir not exist
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// check whether the merge is finished
	var finished bool
	mergeFileNames := make([]string, 0)
	for _, entry := range dirEntries {
		if entry.Name() == model.MergeFinishedFileName {
			finished = true
			mergeFileNames = append(mergeFileNames, entry.Name())
		}

		if strings.HasSuffix(entry.Name(), model.DataFileSuffix) || strings.HasSuffix(entry.Name(), model.HintFileSuffix) {
			mergeFileNames = append(mergeFileNames, entry.Name())
		}
	}

	if !finished {
		return nil
	}

	noMergedFileId, err := db.getNotMergeFid(mergePath)
	if err != nil {
		return err
	}

	// remove old files
	var fid uint32
	for ; fid < noMergedFileId; fid++ {
		fileName := model.GetDataFileName(db.options.dirPath, model.DataFileType, fid)
		if _, err = os.Stat(fileName); err == nil {
			if err = os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// copy the merge files to the db
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.dirPath, fileName)
		if err = os.Rename(srcPath, dstPath); err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func (db *DB) getNotMergeFid(dir string) (uint32, error) {
	mergeFinishedIoManager, err := db.options.ioManagerCreator(model.GetDataFileName(dir, model.MergeFinishedFileType, 0))
	if err != nil {
		return 0, err
	}
	defer mergeFinishedIoManager.Close()
	mergeFinishedDataFile := model.OpenDataFile(0, mergeFinishedIoManager)
	// read the merge finished file
	mergeFinishedRecord, _, err := db.getRecordFromDataFile(mergeFinishedDataFile, 0)
	if err != nil {
		return 0, err
	}

	if bytes.Compare(mergeFinishedRecord.Key, []byte(mergeFinishedKey)) != 0 {
		return 0, ErrInvalidMergeFinishedFile
	}

	fid, err := strconv.Atoi(string(mergeFinishedRecord.Value))
	if err != nil {
		return 0, err
	}

	return uint32(fid), err
}

func (db *DB) loadKeydirFromHintFile() error {
	hintFileName := model.GetDataFileName(db.options.dirPath, model.HintFileType, 0)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	hintFileIoManager, err := db.options.ioManagerCreator(hintFileName)
	if err != nil {
		return err
	}
	defer hintFileIoManager.Close()

	hintFile := model.OpenDataFile(0, hintFileIoManager)

	var offset int64
	for {
		// read record from the hint file
		record, size, err := db.getRecordFromDataFile(hintFile, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		pos := new(model.RecordPos)
		if err = db.options.codec.UnmarshalRecordPos(record.Value, pos); err != nil {
			return err
		}

		// put the index to the db
		db.options.keydir.Put(record.Key, pos)
		offset += size
	}

	return nil
}

func sendError(done chan<- error, err error) {
	done <- err
}

func sendNil(done chan<- error) {
	done <- nil
}
