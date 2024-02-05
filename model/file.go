package model

import "github.com/cqkv/cqkv/fio"

const (
	DataFileSuffix = ".cq"
)

type DataFile struct {
	Fid         uint32
	WriteOffset int64 // only active data file use this field
	WriteTimes  int64
	fio.IOManager
}

func OpenDataFile(fid uint32, ioManager fio.IOManager) *DataFile {
	return &DataFile{
		Fid:       fid,
		IOManager: ioManager,
	}
}

func (df *DataFile) Sync() error {
	return nil
}

// Write binary data into file
func (df *DataFile) Write(data []byte) error {
	// TODO
	return nil
}

// ReadData return the primitive data, data size and error
func (df *DataFile) ReadData(off int64) ([]byte, int64, error) {
	// TODO
	return nil, 0, nil
}
