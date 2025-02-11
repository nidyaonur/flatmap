package flatmap

type NodeEnum int

const (
	NodeUndecided NodeEnum = iota
	NodeNonLeaf
	NodeLeaf
)

type TypeEnum int

// Enums
const (
	TypeEnumBool TypeEnum = iota
	TypeEnumByte
	TypeEnumInt8
	TypeEnumInt16
	TypeEnumInt32
	TypeEnumInt64
	TypeEnumUint8
	TypeEnumUint16
	TypeEnumUint32
	TypeEnumUint64
	TypeEnumFloat32
	TypeEnumFloat64
	TypeEnumString
	TypeEnumBoolList
	TypeEnumByteList
	TypeEnumInt8List
	TypeEnumInt16List
	TypeEnumInt32List
	TypeEnumInt64List
	TypeEnumUint8List
	TypeEnumUint16List
	TypeEnumUint32List
	TypeEnumUint64List
	TypeEnumFloat32List
	TypeEnumFloat64List
	TypeEnumStringList
)

func (e TypeEnum) String() string {
	return [...]string{"bool", "byte", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "string",
		"[]bool", "[]byte", "[]int8", "[]int16", "[]int32", "[]int64", "[]uint8", "[]uint16", "[]uint32", "[]uint64", "[]float32", "[]float64", "[]string"}[e]
}

func GetTypeEnum(s string) TypeEnum {
	switch s {
	case "bool":
		return TypeEnumBool
	case "byte":
		return TypeEnumByte
	case "int8":
		return TypeEnumInt8
	case "int16":
		return TypeEnumInt16
	case "int32":
		return TypeEnumInt32
	case "int64":
		return TypeEnumInt64
	case "uint8":
		return TypeEnumUint8
	case "uint16":
		return TypeEnumUint16
	case "uint32":
		return TypeEnumUint32
	case "uint64":
		return TypeEnumUint64
	case "float32":
		return TypeEnumFloat32
	case "float64":
		return TypeEnumFloat64
	case "string":
		return TypeEnumString
	case "[]bool":
		return TypeEnumBoolList
	case "[]byte":
		return TypeEnumByteList
	case "[]int8":
		return TypeEnumInt8List
	case "[]int16":
		return TypeEnumInt16List
	case "[]int32":
		return TypeEnumInt32List
	case "[]int64":
		return TypeEnumInt64List
	case "[]uint8":
		return TypeEnumUint8List
	case "[]uint16":
		return TypeEnumUint16List
	case "[]uint32":
		return TypeEnumUint32List
	case "[]uint64":
		return TypeEnumUint64List
	case "[]float32":
		return TypeEnumFloat32List
	case "[]float64":
		return TypeEnumFloat64List
	case "[]string":
		return TypeEnumStringList
	default:
		return -1
	}
}
