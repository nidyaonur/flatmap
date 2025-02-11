package flatmap

import (
	"fmt"
	"reflect"
)

func (sn *FlatNode[K, V, VList]) FillGettersWithReflect() error {
	vInstance := sn.conf.NewV()
	vType := reflect.TypeOf(vInstance)
	if vType.Kind() != reflect.Ptr {
		return fmt.Errorf("v must be a pointer")
	}

	// Use the name of the underlying type as key into TableConfigs.
	vElem := vType.Elem()
	typeName := vElem.Name()
	tc, ok := sn.conf.tableConfigs[typeName]
	if !ok {
		return fmt.Errorf("TableConfigs not found for %s", typeName)
	}
	sn.conf.vName = typeName
	sn.conf.fieldCount = len(tc)

	// Iterate over all methods of V.
	for i := 0; i < vType.NumMethod(); i++ {
		method := vType.Method(i)
		// Check if this method corresponds to any field in the config.
		for _, fieldCfg := range tc {
			name := fieldCfg.Name
			isLengthMethod := name+"Length" == method.Name
			if !isLengthMethod && name != method.Name {
				continue
			}
			if isLengthMethod {
				getterIface := method.Func.Interface()
				lengthGetter, ok := getterIface.(func(V) int)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) int", method.Name)
				}
				if sn.conf.LengthGetters == nil {
					sn.conf.LengthGetters = make(map[string]func(V) int)
				}
				sn.conf.LengthGetters[name] = lengthGetter
				continue
			}

			switch fieldCfg.Type {
			case TypeEnumBool:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) bool)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) bool", method.Name)
				}
				if sn.conf.BoolGetters == nil {
					sn.conf.BoolGetters = make(map[string]func(V) bool)
				}
				sn.conf.BoolGetters[name] = getter
			case TypeEnumByte:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) byte)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) byte", method.Name)
				}
				if sn.conf.ByteGetters == nil {
					sn.conf.ByteGetters = make(map[string]func(V) byte)
				}
				sn.conf.ByteGetters[name] = getter
			case TypeEnumInt8:
				// For enums we treat them as int8.
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) int8)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) int8", method.Name)
				}
				if sn.conf.Int8Getters == nil {
					sn.conf.Int8Getters = make(map[string]func(V) int8)
				}
				sn.conf.Int8Getters[name] = getter
			case TypeEnumInt16:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) int16)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) int16", method.Name)
				}
				if sn.conf.Int16Getters == nil {
					sn.conf.Int16Getters = make(map[string]func(V) int16)
				}
				sn.conf.Int16Getters[name] = getter
			case TypeEnumInt32:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) int32)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) int32", method.Name)
				}
				if sn.conf.Int32Getters == nil {
					sn.conf.Int32Getters = make(map[string]func(V) int32)
				}
				sn.conf.Int32Getters[name] = getter
			case TypeEnumInt64:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) int64)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) int64", method.Name)
				}
				if sn.conf.Int64Getters == nil {
					sn.conf.Int64Getters = make(map[string]func(V) int64)
				}
				sn.conf.Int64Getters[name] = getter
			case TypeEnumUint8:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) uint8)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) uint8", method.Name)
				}
				if sn.conf.Uint8Getters == nil {
					sn.conf.Uint8Getters = make(map[string]func(V) uint8)
				}
				sn.conf.Uint8Getters[name] = getter
			case TypeEnumUint16:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) uint16)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) uint16", method.Name)
				}
				if sn.conf.Uint16Getters == nil {
					sn.conf.Uint16Getters = make(map[string]func(V) uint16)
				}
				sn.conf.Uint16Getters[name] = getter
			case TypeEnumUint32:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) uint32)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) uint32", method.Name)
				}
				if sn.conf.Uint32Getters == nil {
					sn.conf.Uint32Getters = make(map[string]func(V) uint32)
				}
				sn.conf.Uint32Getters[name] = getter
			case TypeEnumUint64:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) uint64)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) uint64", method.Name)
				}
				if sn.conf.Uint64Getters == nil {
					sn.conf.Uint64Getters = make(map[string]func(V) uint64)
				}
				sn.conf.Uint64Getters[name] = getter
			case TypeEnumFloat32:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) float32)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) float32", method.Name)
				}
				if sn.conf.Float32Getters == nil {
					sn.conf.Float32Getters = make(map[string]func(V) float32)
				}
				sn.conf.Float32Getters[name] = getter
			case TypeEnumFloat64:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V) float64)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) float64", method.Name)
				}
				if sn.conf.Float64Getters == nil {
					sn.conf.Float64Getters = make(map[string]func(V) float64)
				}
				sn.conf.Float64Getters[name] = getter
			case TypeEnumString:
				getterIface := method.Func.Interface()
				// For strings, we expect the generated code to return a byte slice.
				getter, ok := getterIface.(func(V) []byte)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V) []byte", method.Name)
				}
				if sn.conf.StringGetters == nil {
					sn.conf.StringGetters = make(map[string]func(V) []byte)
				}
				sn.conf.StringGetters[name] = getter
			case TypeEnumBoolList:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) bool)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) bool", method.Name)
				}
				if sn.conf.BoolListGetters == nil {
					sn.conf.BoolListGetters = make(map[string]func(V, int) bool)
				}
				sn.conf.BoolListGetters[name] = getter

			case TypeEnumByteList:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) byte)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) byte", method.Name)
				}
				if sn.conf.ByteListGetters == nil {
					sn.conf.ByteListGetters = make(map[string]func(V, int) byte)
				}
				sn.conf.ByteListGetters[name] = getter
			case TypeEnumInt8List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) int8)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) int8", method.Name)
				}
				if sn.conf.Int8ListGetters == nil {
					sn.conf.Int8ListGetters = make(map[string]func(V, int) int8)
				}
				sn.conf.Int8ListGetters[name] = getter
			case TypeEnumInt16List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) int16)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) int16", method.Name)
				}
				if sn.conf.Int16ListGetters == nil {
					sn.conf.Int16ListGetters = make(map[string]func(V, int) int16)
				}
				sn.conf.Int16ListGetters[name] = getter
			case TypeEnumInt32List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) int32)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) int32", method.Name)
				}
				if sn.conf.Int32ListGetters == nil {
					sn.conf.Int32ListGetters = make(map[string]func(V, int) int32)
				}
				sn.conf.Int32ListGetters[name] = getter
			case TypeEnumInt64List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) int64)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) int64", method.Name)
				}
				if sn.conf.Int64ListGetters == nil {
					sn.conf.Int64ListGetters = make(map[string]func(V, int) int64)
				}
				sn.conf.Int64ListGetters[name] = getter
			case TypeEnumUint8List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) uint8)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) uint8", method.Name)
				}
				if sn.conf.Uint8ListGetters == nil {
					sn.conf.Uint8ListGetters = make(map[string]func(V, int) uint8)
				}
				sn.conf.Uint8ListGetters[name] = getter
			case TypeEnumUint16List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) uint16)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) uint16", method.Name)
				}
				if sn.conf.Uint16ListGetters == nil {
					sn.conf.Uint16ListGetters = make(map[string]func(V, int) uint16)
				}
				sn.conf.Uint16ListGetters[name] = getter
			case TypeEnumUint32List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) uint32)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) uint32", method.Name)
				}
				if sn.conf.Uint32ListGetters == nil {
					sn.conf.Uint32ListGetters = make(map[string]func(V, int) uint32)
				}
				sn.conf.Uint32ListGetters[name] = getter
			case TypeEnumUint64List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) uint64)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) uint64", method.Name)
				}
				if sn.conf.Uint64ListGetters == nil {
					sn.conf.Uint64ListGetters = make(map[string]func(V, int) uint64)
				}
				sn.conf.Uint64ListGetters[name] = getter
			case TypeEnumFloat32List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) float32)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) float32", method.Name)
				}
				if sn.conf.Float32ListGetters == nil {
					sn.conf.Float32ListGetters = make(map[string]func(V, int) float32)
				}
				sn.conf.Float32ListGetters[name] = getter
			case TypeEnumFloat64List:
				getterIface := method.Func.Interface()
				getter, ok := getterIface.(func(V, int) float64)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) float64", method.Name)
				}
				if sn.conf.Float64ListGetters == nil {
					sn.conf.Float64ListGetters = make(map[string]func(V, int) float64)
				}
				sn.conf.Float64ListGetters[name] = getter
			case TypeEnumStringList:
				getterIface := method.Func.Interface()
				// For strings, we expect the generated code to return a byte slice.
				getter, ok := getterIface.(func(V, int) []byte)
				if !ok {
					return fmt.Errorf("unable to assert method %s to func(V, int) []byte", method.Name)
				}
				if sn.conf.StringListGetters == nil {
					sn.conf.StringListGetters = make(map[string]func(V, int) []byte)
				}
				sn.conf.StringListGetters[name] = getter
			}

		}
	}
	return nil
}
