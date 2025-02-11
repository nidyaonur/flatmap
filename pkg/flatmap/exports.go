package flatmap

import "fmt"

// Get retrieves a value from the shard tree given a set of keys. DO NOT PASS A NIL VALUE
func (sn *FlatNode[K, V, VList]) Get(keys []K, v V) bool {
	// example call Get([]uint64{mp_id: 1, cmp_id: 2, c_id: 3})
	if len(keys) == 0 {
		return false
	}
	if sn.nodeType == NodeNonLeaf {
		child, ok := sn.children[keys[sn.level]]
		if !ok {
			return false
		}
		return child.Get(keys, v)
	}

	index, ok := sn.indexes[keys[sn.level]]
	if !ok {
		return false
	}
	if index < 0 {
		return false
	}
	if !sn.Vlist.Children(v, int(index)) {
		return false
	}

	return true
}

// Get retrieves a value from the shard tree given a set of keys.
func (sn *FlatNode[K, V, VList]) GetBatch(keys []K) (vList VList, found bool) {
	// example call Get([]uint64{mp_id: 1, cmp_id: 2, c_id: 3})
	if len(keys) == 0 {
		return
	}
	if sn.nodeType == NodeNonLeaf {
		child, ok := sn.children[keys[sn.level]]
		if !ok {
			return
		}
		return child.GetBatch(keys)
	}
	return sn.Vlist, true
}

func (sn *FlatNode[K, V, VList]) Set(v DeltaItem[K]) error {
	if len(v.Keys) == 0 {
		return fmt.Errorf("no keys provided")
	}
	if sn.nodeType == NodeNonLeaf {
		child, ok := sn.children[v.Keys[sn.level]]
		if ok {
			return child.Set(v)
		}
	}
	sn.rwMutex.Lock()
	sn.pendingDelta = append(sn.pendingDelta, v)
	sn.rwMutex.Unlock()
	return nil
}

func (sn *FlatNode[K, V, VList]) Delete(keys []K) {
	if len(keys) == 0 {
		return
	}
	if sn.nodeType == NodeLeaf {
		if _, ok := sn.indexes[keys[sn.level]]; !ok {
			return
		}
		sn.rwMutex.Lock()
		sn.deleted[keys[sn.level]] = true
		sn.rwMutex.Unlock()
		return
	}
	child, ok := sn.children[keys[sn.level]]
	if !ok {
		return
	}
	child.Delete(keys)
}
