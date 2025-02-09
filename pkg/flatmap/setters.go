package flatmap

import flatbuffers "github.com/google/flatbuffers/go"

// Scalar setters

func AddBoolField(builder *flatbuffers.Builder, slot int, value bool) {
	builder.PrependBoolSlot(slot, value, false)
}

func AddByteField(builder *flatbuffers.Builder, slot int, value byte) {
	builder.PrependByteSlot(slot, value, 0)
}

func AddInt8Field(builder *flatbuffers.Builder, slot int, value int8) {
	builder.PrependInt8Slot(slot, value, 0)
}

func AddInt16Field(builder *flatbuffers.Builder, slot int, value int16) {
	builder.PrependInt16Slot(slot, value, 0)
}

func AddInt32Field(builder *flatbuffers.Builder, slot int, value int32) {
	builder.PrependInt32Slot(slot, value, 0)
}

func AddInt64Field(builder *flatbuffers.Builder, slot int, value int64) {
	builder.PrependInt64Slot(slot, value, 0)
}

func AddUint8Field(builder *flatbuffers.Builder, slot int, value uint8) {
	builder.PrependByteSlot(slot, byte(value), 0)
}

func AddUint16Field(builder *flatbuffers.Builder, slot int, value uint16) {
	builder.PrependUint16Slot(slot, value, 0)
}

func AddUint32Field(builder *flatbuffers.Builder, slot int, value uint32) {
	builder.PrependUint32Slot(slot, value, 0)
}

func AddUint64Field(builder *flatbuffers.Builder, slot int, value uint64) {
	builder.PrependUint64Slot(slot, value, 0)
}

func AddFloat32Field(builder *flatbuffers.Builder, slot int, value float32) {
	builder.PrependFloat32Slot(slot, value, 0)
}

func AddFloat64Field(builder *flatbuffers.Builder, slot int, value float64) {
	builder.PrependFloat64Slot(slot, value, 0)
}

func AddOffsetField(builder *flatbuffers.Builder, slot int, offset flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(slot, offset, 0)
}

// Vector setters

// func AddBoolVectorField(builder *flatbuffers.Builder, slot int, values []bool) {
// 	offset := builder.StartVector(1, len(values), 1)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependBool(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddByteVectorField(builder *flatbuffers.Builder, slot int, values []byte) {
// 	offset := builder.StartVector(1, len(values), 1)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependByte(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddInt8VectorField(builder *flatbuffers.Builder, slot int, values []int8) {
// 	// Each int8 is 1 byte; use alignment 1.
// 	offset := builder.StartVector(1, len(values), 1)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependInt8(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddInt16VectorField(builder *flatbuffers.Builder, slot int, values []int16) {
// 	offset := builder.StartVector(2, len(values), 2)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependInt16(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddInt32VectorField(builder *flatbuffers.Builder, slot int, values []int32) {
// 	offset := builder.StartVector(4, len(values), 4)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependInt32(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddInt64VectorField(builder *flatbuffers.Builder, slot int, values []int64) {
// 	offset := builder.StartVector(8, len(values), 8)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependInt64(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddUint8VectorField(builder *flatbuffers.Builder, slot int, values []uint8) {
// 	offset := builder.StartVector(1, len(values), 1)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependByte(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddUint16VectorField(builder *flatbuffers.Builder, slot int, values []uint16) {
// 	offset := builder.StartVector(2, len(values), 2)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependUint16(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddUint32VectorField(builder *flatbuffers.Builder, slot int, values []uint32) {
// 	offset := builder.StartVector(4, len(values), 4)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependUint32(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddUint64VectorField(builder *flatbuffers.Builder, slot int, values []uint64) {
// 	offset := builder.StartVector(8, len(values), 8)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependUint64(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddFloat32VectorField(builder *flatbuffers.Builder, slot int, values []float32) {
// 	offset := builder.StartVector(4, len(values), 4)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependFloat32(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddFloat64VectorField(builder *flatbuffers.Builder, slot int, values []float64) {
// 	offset := builder.StartVector(8, len(values), 8)
// 	for i := len(values) - 1; i >= 0; i-- {
// 		builder.PrependFloat64(values[i])
// 	}
// 	builder.EndVector(len(values))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

// func AddStringVectorField(builder *flatbuffers.Builder, slot int, values []string) {
// 	// Create individual strings first.
// 	stringOffsets := make([]flatbuffers.UOffsetT, len(values))
// 	for i, s := range values {
// 		stringOffsets[i] = builder.CreateString(s)
// 	}
// 	// Each offset is 4 bytes; use alignment 4.
// 	offset := builder.StartVector(4, len(stringOffsets), 4)
// 	for i := len(stringOffsets) - 1; i >= 0; i-- {
// 		builder.PrependUOffsetT(stringOffsets[i])
// 	}
// 	builder.EndVector(len(stringOffsets))
// 	builder.PrependUOffsetTSlot(slot, offset, 0)
// }

func StartVector(builder *flatbuffers.Builder, numElems int, elemSize int) flatbuffers.UOffsetT {
	return builder.StartVector(elemSize, numElems, elemSize)
}

func EndVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.EndVector(numElems)
}
