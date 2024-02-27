package cqkv

import (
	"fmt"
)

var (
	ErrEmptyKey = addPrefix("the key is empty")
	ErrBigValue = addPrefix("value is too big")
	ErrNoRecord = addPrefix("no record in keydir")
	ErrWrongCrc = addPrefix("wrong crc value, data may be corrupted")

	ErrNoDataFile        = addPrefix("no data file")
	ErrNoIOManager       = addPrefix("no io manager")
	ErrDirIsUsing        = addPrefix("direction is using")
	ErrNeedFileLock      = addPrefix("need file lock")
	ErrDataFileCorrupted = addPrefix("data file may be corrupted")

	ErrUpdateKeydir = addPrefix("update keydir failed")

	ErrExceedMaxBatchNum = addPrefix("exceed max batch num")
)

func addPrefix(errStr string) error {
	return fmt.Errorf("cqkv err: %s", errStr)
}
