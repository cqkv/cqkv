package fio

import "os"

// FileIO is the default implement for IOManager
type FileIO struct {
	fd *os.File
}

func NewFIleIO(file string) (*FileIO, error) {
	fd, err := os.OpenFile(file, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(buf []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(buf, offset)
}
func (fio *FileIO) Write(data []byte) (int, error) {
	return fio.fd.Write(data)
}
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
