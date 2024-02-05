package fio

// IOManager can be custom in options
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Size() (int64, error)
}
