package parser

import "strings"

func fbTypeToGoType(fbType string, enumSet map[string]bool) string {
	// 1) Handle vectors: [Something]
	if strings.HasPrefix(fbType, "[") && strings.HasSuffix(fbType, "]") {
		innerType := fbType[1 : len(fbType)-1] // drop the brackets
		return "[]" + fbTypeToGoType(innerType, enumSet)
	}

	// 2) If it's recognized as an enum
	if enumSet[fbType] {
		// user wants all enums => int8
		return "int8"
	}

	switch fbType {
	case "bool":
		return "bool"
	case "byte", "ubyte", "int8", "uint8":
		return "byte"
	case "short", "int16":
		return "int16"
	case "ushort", "uint16":
		return "uint16"
	case "int", "int32":
		return "int32"
	case "uint", "uint32":
		return "uint32"
	case "long", "int64":
		return "int64"
	case "ulong", "uint64":
		return "uint64"
	case "float", "float32":
		return "float32"
	case "double", "float64":
		return "float64"
	case "string":
		return "string"
	default:
		// user-defined types or nested arrays:
		return fbType
	}
}

// SnakeToPascal converts a snake_case string to PascalCase
func snakeToPascal(s string) string {
	words := strings.Split(s, "_")
	for i := range words {
		if len(words[i]) > 0 {
			words[i] = strings.ToUpper(words[i][:1]) + words[i][1:]
		}
	}
	return strings.Join(words, "")
}
