package parser

import "strings"

func fbTypeToGoType(fbType string, enumSet map[string]bool) (string, bool) {
	// 1) Handle vectors: [Something]
	if strings.HasPrefix(fbType, "[") && strings.HasSuffix(fbType, "]") {
		innerType := fbType[1 : len(fbType)-1] // drop the brackets
		goType, isEnum := fbTypeToGoType(innerType, enumSet)
		return "[]" + goType, isEnum
	}

	// 2) If it's recognized as an enum
	if enumSet[fbType] {
		// user wants all enums => int8
		return "byte", true
	}

	switch fbType {
	case "bool":
		return "bool", false
	case "byte", "ubyte", "int8", "uint8":
		return "byte", false
	case "short", "int16":
		return "int16", false
	case "ushort", "uint16":
		return "uint16", false
	case "int", "int32":
		return "int32", false
	case "uint", "uint32":
		return "uint32", false
	case "long", "int64":
		return "int64", false
	case "ulong", "uint64":
		return "uint64", false
	case "float", "float32":
		return "float32", false
	case "double", "float64":
		return "float64", false
	case "string":
		return "string", false
	default:
		// user-defined types or nested arrays:
		return fbType, false
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
