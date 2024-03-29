package fio

import (
	"fmt"
	"os"
	"time"
)

// FileIO is the default implement for IOManager
type FileIO struct {
	fd *os.File
}

func NewFIleIO(file string) (*FileIO, error) {
	fd, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(buf []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(buf, offset)
}
func (fio *FileIO) Write(data []byte) (int, error) {
	start := time.Now()
	defer func() {
		fmt.Println("write time111:", time.Since(start))
	}()
	return fio.fd.Write(data)
}
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
func (fio *FileIO) Size() (int64, error) {
	info, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
