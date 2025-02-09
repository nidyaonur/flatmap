package flatmap

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type VType interface {
	Init(buf []byte, i flatbuffers.UOffsetT)
	Table() flatbuffers.Table
}
type VListType[V VType] interface {
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
}
