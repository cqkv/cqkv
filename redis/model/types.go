package model

import "errors"

type RdsType = byte

const (
	StringType RdsType = iota
	HashType
	SetType
	ZSetType
	ListType
)

var (
	ErrWrongTypeOp = errors.New("(error) WRONGTYPE Operation against a key holding the wrong kind of value")
)
