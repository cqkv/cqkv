package fio

import (
	"path/filepath"

	"github.com/gofrs/flock"
)

type FileLocker interface {
	TryLock() (bool, error)
	Unlock() error
}

const FlockName = "flock"

func NewFlock(dirPath string) *flock.Flock {
	return flock.New(filepath.Join(dirPath, FlockName))
}
