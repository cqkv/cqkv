package model

import "github.com/cqkv/cqkv/fio"

type DataFile struct {
	Fid         uint32
	WriteOffset int64
	fio.IOManager
}

func (df *DataFile) Sync() error {
	return nil
}
