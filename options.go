package cqkv

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cqkv/cqkv/codec"
	"github.com/cqkv/cqkv/fio"
	"github.com/cqkv/cqkv/keydir"
	"github.com/cqkv/cqkv/model"
)

type options struct {
	dirPath      string
	dataFileSize int64
	// syncFre indicate the frequency to sync
	syncFre int64

	ioManagerCreator func(dirPath string, fid uint32) (fio.IOManager, error)
	fileLock         fio.FileLocker

	codec codec.Codec

	keyDir keydir.Keydir

	fastOpen bool
}

var defaultOptions = &options{
	dirPath:          os.TempDir(),
	dataFileSize:     1024 * 1024 * 256, // 256mb
	syncFre:          32,
	ioManagerCreator: defaultIOManagerCreator,
	codec:            codec.NewCodecImpl(),
	keyDir:           keydir.NewBTree(32),
}

type Option func(*options)

// WithIOManagerCreator should be used with file lock
func WithIOManagerCreator(fn func(dirPath string, fid uint32) (fio.IOManager, error)) Option {
	return func(o *options) {
		if fn == nil {
			panic(ErrNoIOManager)
		}
		o.ioManagerCreator = fn
	}
}

func WithFileLock(lock fio.FileLocker) Option {
	return func(o *options) {
		o.fileLock = lock
	}
}

var defaultIOManagerCreator = func(dirPath string, fid uint32) (fio.IOManager, error) {
	return fio.NewFIleIO(filepath.Join(dirPath, fmt.Sprintf("%09d%s", fid, model.DataFileSuffix)))
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

func WithBTreeKeydir(degree int) Option {
	return func(o *options) {
		o.keyDir = keydir.NewBTree(degree)
	}
}

func WithSkipListKeydir() Option {
	return func(o *options) {
		o.keyDir = keydir.NewSkipList()
	}
}

// WithFastOpen use mmap to reduce open time
func WithFastOpen() Option {
	return func(o *options) {
		o.fastOpen = true
	}
}

type WriteBatchOption func(*writeBatchOptions)

type writeBatchOptions struct {
	maxBatchNum int

	// sync indicate whether to sync after write
	sync bool
}

var defaultWriteBatchOptions = &writeBatchOptions{
	maxBatchNum: 1024,
	sync:        false,
}

func WithMaxBatchNum(num int) WriteBatchOption {
	return func(o *writeBatchOptions) {
		o.maxBatchNum = num
	}
}

func WithSync(sync bool) WriteBatchOption {
	return func(o *writeBatchOptions) {
		o.sync = sync
	}
}
