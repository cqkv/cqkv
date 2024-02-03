package cqkv

import (
	"fmt"
	"github.com/cqkv/cqkv/codec"
	"path/filepath"

	"github.com/cqkv/cqkv/fio"
)

type options struct {
	dirPath      string
	dataFileSize int64

	iOManagerCreator func(fid uint32) (fio.IOManager, error)
	codec            codec.Codec
}

type Option func(*options)

func WithIOManagerCreator(fn func(fid uint32) (fio.IOManager, error)) Option {
	return func(o *options) {
		o.iOManagerCreator = fn
	}
}

var dirPath string

var defaultIOManagerCreator = func(fid uint32) (fio.IOManager, error) {
	return fio.NewFIleIO(filepath.Join(dirPath, fmt.Sprintf("%09d", fid)))
}

func WithDirPath(dirPath string) Option {
	return func(o *options) {
		o.dirPath = dirPath
	}
}

func WithDataFileSize(size int64) Option {
	return func(o *options) {
		o.dataFileSize = size
	}
}

func WithCodec(codec codec.Codec) Option {
	return func(o *options) {
		o.codec = codec
	}
}
