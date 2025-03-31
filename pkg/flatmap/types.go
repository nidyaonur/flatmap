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

type FieldConfig struct {
	Name         string
	Type         TypeEnum
	DefaultValue string
	Meta         string
	EnumName     string
}
