package flatmap

/*
   {
     "name": "ScalarListField",
     "fbtype": "[ulong]",
     "gotype": "[]uint64",
     "defaultValue": "",
     "meta": "",
   }
*/

// type FlatConfig[K comparable, V VType, VList VListType[V]] struct {
// 	// The level of the node in the shard tree
// 	level int
// 	// parentWg *sync.WaitGroup

// 	// The initial data to populate the shard with
// 	// InitialBuffer []byte
// 	// InitialKeys   [][]K
// 	// keyIndexes    []int
// 	// The function to read a message into a V

// 	// The function to extract keys from a value of type V
// 	// GetKeys func(data []byte) []K

// 	// Callbacks
// 	conf conf[K, V, VList]

// 	// TableConfigs map[string][]FieldConfig
// 	// vName        string
// 	// fieldCount   int
// }

func (fc *FlatConfig[K, VT, V, VList]) Validate() error {
	// if fc.UpdateSeconds <= 0 {
	// 	return fmt.Errorf("UpdateSeconds must be greater than 0")
	// }
	// if fc.Callbacks.GetRoot == nil {
	// 	return fmt.Errorf("GetRoot function must be provided")
	// }
	// if fc.Callbacks.VListStartChildrenVector == nil {
	// 	return fmt.Errorf("VListStartChildrenVector function must be provided")
	// }
	// if fc.Callbacks.VListStart == nil {
	// 	return fmt.Errorf("VListStart function must be provided")
	// }
	// if fc.Callbacks.VListEnd == nil {
	// 	return fmt.Errorf("VListEnd function must be provided")
	// }
	// if fc.Callbacks.VListAddChildren == nil {
	// 	return fmt.Errorf("VListAddChildren function must be provided")
	// }
	return nil
}
