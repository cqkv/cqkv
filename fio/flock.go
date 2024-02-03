package fio

import (
	"path/filepath"

	"github.com/gofrs/flock"
)

const flockName = "flock"

func NewFlock(dirPath string) *flock.Flock {
	return flock.New(filepath.Join(dirPath, flockName))
}
