package flatmap

import "fmt"

func (fc *FlatConfig[K, VT, V, VList]) Validate() error {
	if fc.NewV == nil {
		return fmt.Errorf("NewV is nil")
	}
	if fc.NewVList == nil {
		return fmt.Errorf("NewVList is nil")
	}
	if fc.GetKeysFromV == nil {
		return fmt.Errorf("GetKeysFromV is nil")
	}
	return nil
}
