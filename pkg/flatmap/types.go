package flatmap

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type VTypeT interface {
	Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT
}

type VType[VT VTypeT] interface {
	Init(buf []byte, i flatbuffers.UOffsetT)
	Table() flatbuffers.Table
	UnPackTo(t VT)
	UnPack() VT
}
type VListType[VT VTypeT, V VType[VT]] interface {
	Init(buf []byte, i flatbuffers.UOffsetT)
	Children(V, int) bool
	ChildrenLength() int
}

type DeltaItem[K comparable] struct {
	Keys []K
	Data []byte
}

// (Note: for enums we treat them as int8.)
type FlatConfig[K comparable, VT VTypeT, V VType[VT], VList VListType[VT, V]] struct {
	// Public fields
	NewV            func() V
	NewVList        func() VList
	GetKeysFromV    func(v V) []K
	CheckVForDelete func(v V) bool
	UpdateSeconds   uint
	SnapShotMode    SnapshotMode
	Logger          Logger
	LogLevel        LogLevel
}

type ShardSnapshot[K comparable] struct {
	Path   []K
	Keys   []K
	Buffer []byte
}
