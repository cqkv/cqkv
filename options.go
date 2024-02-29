package cqkv

import (
	"github.com/cqkv/cqkv/codec"
	"github.com/cqkv/cqkv/fio"
	"github.com/cqkv/cqkv/keydir"
	"os"
)

type options struct {
	dirPath      string
	dataFileSize int64
	// syncFre indicate the frequency to sync
	syncFre int64

	ioManagerCreator func(filePath string) (fio.IOManager, error)
	fileLock         fio.FileLocker

	codec codec.Codec

	keydir      keydir.Keydir
	keydirType  string
	btreeDegree int

	fastOpen bool
}

var defaultOptions = &options{
	dirPath:          os.TempDir(),
	dataFileSize:     1024 * 1024 * 256, // 256mb
	syncFre:          101,
	ioManagerCreator: defaultIOManagerCreator,
	codec:            codec.NewCodecImpl(),
	keydir:           keydir.NewBTree(32),
	keydirType:       keydir.BtreeTypeKeydir,
	btreeDegree:      32,
}

type Option func(*options)

// WithIOManagerCreator should be used with file lock
func WithIOManagerCreator(fn func(filePath string) (fio.IOManager, error)) Option {
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

var defaultIOManagerCreator = func(filePath string) (fio.IOManager, error) {
	return fio.NewFIleIO(filePath)
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
		o.keydir = keydir.NewBTree(degree)
		o.keydirType = keydir.BtreeTypeKeydir
		o.btreeDegree = degree
	}
}

func WithSkipListKeydir() Option {
	return func(o *options) {
		o.keydir = keydir.NewSkipList()
		o.keydirType = keydir.SkipListTypeKeydir
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
