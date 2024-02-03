package cqkv

import (
	"fmt"
)

var (
	ErrEmptyKey = addPrefix("the key is empty")
	ErrBigValue = addPrefix("value is too big")
	ErrNoRecord = addPrefix("no record in keydir")

	ErrNoDataFile        = addPrefix("no data file")
	ErrNoIOManager       = addPrefix("no io manager")
	ErrDirIsUsing        = addPrefix("direction is using")
	ErrNeedFileLock      = addPrefix("need file lock")
	ErrDataFileCorrupted = addPrefix("data file may be corrupted")

	ErrUpdateKeydir = addPrefix("update keydir failed")
)

func addPrefix(errStr string) error {
	return fmt.Errorf("cqkv err: %s", errStr)
}
