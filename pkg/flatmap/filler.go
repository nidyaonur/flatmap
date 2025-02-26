package flatmap

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

func (sn *FlatNode[K, V, VList]) FillStrOffsets(v V, strOffsets map[string]flatbuffers.UOffsetT, fieldConfigs []FieldConfig) {
	for _, fc := range fieldConfigs {
		switch fc.Type {
		case TypeEnumString:
			val := string(sn.conf.StringGetters[fc.Name](v))
			if _, ok := strOffsets[val]; !ok {
				strOffsets[val] = sn.Builder.CreateString(val)
			}
		case TypeEnumStringList:
			listLength := sn.conf.LengthGetters[fc.Name](v)
			for j := 0; j < listLength; j++ {
				val := string(sn.conf.StringListGetters[fc.Name](v, j))
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
		for _, fc := range sn.conf.tableConfigs[sn.conf.vName] {
			lengthGetter, ok := sn.conf.LengthGetters[fc.Name]
			if !ok {
				continue
			}
			listLength := lengthGetter(vObj)

			if listLength > 0 {
				switch fc.Type {
				case TypeEnumStringList:
					_ = StartVector(sn.Builder, listLength, 4) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependUOffsetT(strOffsets[string(sn.conf.StringListGetters[fc.Name](vObj, j))])
					}
				case TypeEnumUint64List:
					_ = StartVector(sn.Builder, listLength, 8) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependUint64(sn.conf.Uint64ListGetters[fc.Name](vObj, j))
					}
				case TypeEnumInt64List:
					_ = StartVector(sn.Builder, listLength, 8) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependInt64(sn.conf.Int64ListGetters[fc.Name](vObj, j))
					}
				case TypeEnumUint32List:
					_ = StartVector(sn.Builder, listLength, 4) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependUint32(sn.conf.Uint32ListGetters[fc.Name](vObj, j))
					}
				case TypeEnumInt32List:
					_ = StartVector(sn.Builder, listLength, 4) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						sn.Builder.PrependInt32(sn.conf.Int32ListGetters[fc.Name](vObj, j))
					}
				case TypeEnumUint16List:
					_ = StartVector(sn.Builder, listLength, 2) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.conf.Uint16ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependUint16(val)
					}
				case TypeEnumInt16List:
					_ = StartVector(sn.Builder, listLength, 2) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.conf.Int16ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependInt16(val)
					}
				case TypeEnumUint8List:
					_ = StartVector(sn.Builder, listLength, 1) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.conf.Uint8ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependByte(val)
					}
				case TypeEnumInt8List:
					_ = StartVector(sn.Builder, listLength, 1) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.conf.Int8ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependInt8(val)
					}
				case TypeEnumByteList:
					_ = StartVector(sn.Builder, listLength, 1) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.conf.ByteListGetters[fc.Name](vObj, j)
						sn.Builder.PrependByte(val)
					}
				case TypeEnumFloat64List:
					_ = StartVector(sn.Builder, listLength, 8) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.conf.Float64ListGetters[fc.Name](vObj, j)
						sn.Builder.PrependFloat64(val)
					}
				case TypeEnumFloat32List:
					_ = StartVector(sn.Builder, listLength, 4) // internal offset, not needed
					for j := listLength - 1; j >= 0; j-- {
						val := sn.conf.Float32ListGetters[fc.Name](vObj, j)
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
	for slot, fc := range sn.conf.tableConfigs[sn.conf.vName] {
		switch fc.Type {
		case TypeEnumUint64:
			AddUint64Field(sn.Builder, slot, sn.conf.Uint64Getters[fc.Name](v))
		case TypeEnumInt64:
			AddInt64Field(sn.Builder, slot, sn.conf.Int64Getters[fc.Name](v))
		case TypeEnumUint32:
			AddUint32Field(sn.Builder, slot, sn.conf.Uint32Getters[fc.Name](v))
		case TypeEnumInt32:
			AddInt32Field(sn.Builder, slot, sn.conf.Int32Getters[fc.Name](v))
		case TypeEnumUint16:
			AddUint16Field(sn.Builder, slot, sn.conf.Uint16Getters[fc.Name](v))
		case TypeEnumInt16:
			AddInt16Field(sn.Builder, slot, sn.conf.Int16Getters[fc.Name](v))
		case TypeEnumUint8, TypeEnumByte:
			AddByteField(sn.Builder, slot, sn.conf.ByteGetters[fc.Name](v))
		case TypeEnumBool:
			AddBoolField(sn.Builder, slot, sn.conf.BoolGetters[fc.Name](v))
		case TypeEnumFloat64:
			AddFloat64Field(sn.Builder, slot, sn.conf.Float64Getters[fc.Name](v))
		case TypeEnumFloat32:
			AddFloat32Field(sn.Builder, slot, sn.conf.Float32Getters[fc.Name](v))
		case TypeEnumString:
			AddOffsetField(sn.Builder, slot, strOffsets[string(sn.conf.StringGetters[fc.Name](v))])
		case TypeEnumInt8:
			AddInt8Field(sn.Builder, slot, sn.conf.Int8Getters[fc.Name](v))
		case TypeEnumUint64List, TypeEnumInt64List, TypeEnumUint32List, TypeEnumInt32List, TypeEnumUint16List, TypeEnumInt16List, TypeEnumUint8List, TypeEnumInt8List, TypeEnumFloat64List, TypeEnumFloat32List, TypeEnumStringList, TypeEnumByteList:
			AddOffsetField(sn.Builder, slot, vectorOffsetsMap[fc.Name])
		}
	}
	return sn.End(sn.Builder)
}
