package model

import (
	"fmt"
	"github.com/cqkv/cqkv/fio"
	"path/filepath"
)

const (
	DataFileType          = "data"
	HintFileType          = "hint"
	MergeFinishedFileType = "merge-finished"

	DataFileSuffix        = ".cq"
	HintFileSuffix        = ".hint"
	MergeFinishedFileName = "cqkv-merge-finished"
)

type DataFile struct {
	Fid         uint32
	WriteOffset int64 // only active data file use this field
	WriteTimes  int64
	IoManager   fio.IOManager
}

func OpenDataFile(fid uint32, ioManager fio.IOManager) *DataFile {
	return &DataFile{
		Fid:       fid,
		IoManager: ioManager,
	}
}

func GetDataFileName(dirPath, fileType string, fid uint32) string {
	var filePath string
	switch fileType {
	case DataFileType:
		filePath = filepath.Join(dirPath, fmt.Sprintf("%09d%s", fid, DataFileSuffix))
	case HintFileType:
		filePath = filepath.Join(dirPath, fmt.Sprintf("cqkv%s", HintFileSuffix))
	case MergeFinishedFileType:
		filePath = filepath.Join(dirPath, MergeFinishedFileName)
	}
	return filePath
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// Write binary data into file
func (df *DataFile) Write(data []byte) error {
	size, err := df.IoManager.Write(data)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(size)
	return nil
}

// ReadRecordHeader return the primitive data, data size and error
func (df *DataFile) ReadRecordHeader(offset int64) ([]byte, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, err
	}

	var headerBuf int64 = MaxHeaderSize
	if headerBuf+offset > fileSize {
		headerBuf = fileSize - offset
	}

	return df.readNBytes(offset, headerBuf)
}

func (df *DataFile) ReadRecord(off, size int64) (data []byte, err error) {
	return df.readNBytes(off, size)
}

func (df *DataFile) readNBytes(offset, n int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := df.IoManager.Read(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}
