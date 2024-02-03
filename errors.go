package cqkv

import (
	"fmt"
)

var (
	ErrEmptyKey = addPrefix("the key is empty")
	ErrBigValue = addPrefix("value is too big")

	ErrNoIOManager = addPrefix("no io manager")
)

func addPrefix(errStr string) error {
	return fmt.Errorf("cqkv err: %s", errStr)
}
