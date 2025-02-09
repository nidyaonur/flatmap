package flatmap

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

func (sn *FlatNode[K, V, VList]) FillStrOffsets(v V, strOffsets map[string]flatbuffers.UOffsetT, fieldConfigs []FieldConfig) {
	for _, fc := range fieldConfigs {
		switch fc.Type {
		case TypeEnumString:
			val := string(sn.Callbacks.StringGetters[fc.Name](v))
			if _, ok := strOffsets[val]; !ok {
				strOffsets[val] = sn.Builder.CreateString(val)
			}
		case TypeEnumStringList:
			listLength := sn.Callbacks.LengthGetters[fc.Name](v)
			for j := 0; j < listLength; j++ {
				val := string(sn.Callbacks.StringListGetters[fc.Name](v, j))
				if _, ok := strOffsets[val]; !ok {
					strOffsets[val] = sn.Builder.CreateString(val)
				}
			}
		}
	}
}

func (sn *FlatNode[K, V, VList]) FillVectorOffsetsMap(vList []V, strOffsets map[string]flatbuffers.UOffsetT) []map[string]flatbuffers.UOffsetT {
	vectorOffsetsMapList := make([]map[string]flatbuffers.UOffsetT, 0, len(vList))

	for i := 0; i < len(vList); i++ {
		vObj := vList[i]
		vectorOffsetsMap := make(map[string]flatbuffers.UOffsetT)
		for _, fc := range sn.TableConfigs[sn.VName] {
			lengthGetter, ok := sn.Callbacks.LengthGetters[fc.Name]
			if !ok {
				continue
			}
			listLength := lengthGetter(vObj)

			if listLength > 0 {
				switch fc.Type {
				case TypeEnumStringList:
					_ = StartVector(sn.Builder, listLength, 4) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependUOffsetT(strOffsets[string(sn.Callbacks.StringListGetters[fc.Name](vObj, j))])
					}
				case TypeEnumUint64List:
					_ = StartVector(sn.Builder, listLength, 8) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependUint64(sn.Callbacks.Uint64ListGetters[fc.Name](vObj, j))
					}
				case TypeEnumInt64List:
					_ = StartVector(sn.Builder, listLength, 8) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependInt64(sn.Callbacks.Int64ListGetters[fc.Name](vObj, j))
					}
				case TypeEnumUint32List:
					_ = StartVector(sn.Builder, listLength, 4) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependUint32(sn.Callbacks.Uint32ListGetters[fc.Name](vObj, j))
					}
				case TypeEnumInt32List:
					_ = StartVector(sn.Builder, listLength, 4) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependInt32(sn.Callbacks.Int32ListGetters[fc.Name](vObj, j))
					}
				case TypeEnumUint16List:
					_ = StartVector(sn.Builder, listLength, 2) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.Callbacks.Uint16ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependUint16(val)
					}
				case TypeEnumInt16List:
					_ = StartVector(sn.Builder, listLength, 2) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.Callbacks.Int16ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependInt16(val)
					}
				case TypeEnumUint8List:
					_ = StartVector(sn.Builder, listLength, 1) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.Callbacks.Uint8ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependByte(val)
					}
				case TypeEnumInt8List:
					_ = StartVector(sn.Builder, listLength, 1) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.Callbacks.Int8ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependInt8(val)
					}
				case TypeEnumFloat64List:
					_ = StartVector(sn.Builder, listLength, 8) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.Callbacks.Float64ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependFloat64(val)
					}
				case TypeEnumFloat32List:
					_ = StartVector(sn.Builder, listLength, 4) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.Callbacks.Float32ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependFloat32(val)
					}
				}
				vectorOffsetsMap[fc.Name] = EndVector(sn.Builder, listLength)
			} else {
				vectorOffsetsMap[fc.Name] = 0
			}
		}
		vectorOffsetsMapList = append(vectorOffsetsMapList, vectorOffsetsMap)
	}
	return vectorOffsetsMapList
}

func (sn *FlatNode[K, V, VList]) FillVFields(v V, strOffsets map[string]flatbuffers.UOffsetT, vectorOffsetsMap map[string]flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	sn.VStart(sn.Builder)
	for slot, fc := range sn.TableConfigs[sn.VName] {
		switch fc.Type {
		case TypeEnumUint64:
			AddUint64Field(sn.Builder, slot, sn.Callbacks.Uint64Getters[fc.Name](v))
		case TypeEnumInt64:
			AddInt64Field(sn.Builder, slot, sn.Callbacks.Int64Getters[fc.Name](v))
		case TypeEnumUint32:
			AddUint32Field(sn.Builder, slot, sn.Callbacks.Uint32Getters[fc.Name](v))
		case TypeEnumInt32:
			AddInt32Field(sn.Builder, slot, sn.Callbacks.Int32Getters[fc.Name](v))
		case TypeEnumUint16:
			AddUint16Field(sn.Builder, slot, sn.Callbacks.Uint16Getters[fc.Name](v))
		case TypeEnumInt16:
			AddInt16Field(sn.Builder, slot, sn.Callbacks.Int16Getters[fc.Name](v))
		case TypeEnumUint8:
			AddByteField(sn.Builder, slot, sn.Callbacks.ByteGetters[fc.Name](v))
		case TypeEnumBool:
			AddBoolField(sn.Builder, slot, sn.Callbacks.BoolGetters[fc.Name](v))
		case TypeEnumFloat64:
			AddFloat64Field(sn.Builder, slot, sn.Callbacks.Float64Getters[fc.Name](v))
		case TypeEnumFloat32:
			AddFloat32Field(sn.Builder, slot, sn.Callbacks.Float32Getters[fc.Name](v))
		case TypeEnumString:
			AddOffsetField(sn.Builder, slot, strOffsets[string(sn.Callbacks.StringGetters[fc.Name](v))])
		case TypeEnumInt8:
			AddInt8Field(sn.Builder, slot, sn.Callbacks.Int8Getters[fc.Name](v))
		case TypeEnumUint64List, TypeEnumInt64List, TypeEnumUint32List, TypeEnumInt32List, TypeEnumUint16List, TypeEnumInt16List, TypeEnumUint8List, TypeEnumInt8List, TypeEnumFloat64List, TypeEnumFloat32List, TypeEnumStringList:
			AddOffsetField(sn.Builder, slot, vectorOffsetsMap[fc.Name])
		}
	}
	return sn.End(sn.Builder)
}
