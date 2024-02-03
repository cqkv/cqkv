package fio

// IOManager can be custom in options
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type FileLocker interface {
	TryLock() (bool, error)
	Unlock() error
}
