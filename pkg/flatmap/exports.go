package flatmap

import "fmt"

// Get retrieves a value from the shard tree given a set of keys. DO NOT PASS A NIL VALUE
func (sn *FlatNode[K, VT, V, VList]) Get(keys []K, v V) bool {
	// example call Get([]uint64{mp_id: 1, cmp_id: 2, c_id: 3})
	if len(keys) == 0 {
		return false
	}
	if sn.nodeType == NodeUndecided {
		return false
	}
	if sn.nodeType == NodeNonLeaf {
		child, ok := sn.children[keys[sn.level]]
		if !ok {
			return false
		}
		return child.Get(keys, v)
	}
	view := *sn.viewPtr // never nil

	index, ok := view.indexes[keys[sn.level]]
	if !ok {
		return false
	}
	if index < 0 {
		return false
	}
	if !view.Vlist.Children(v, int(index)) {
		return false
	}

	return true
}

// Get retrieves a value from the shard tree given a set of keys.
func (sn *FlatNode[K, VT, V, VList]) GetBatch(keys []K) (vList VList, found bool) {
	// example call Get([]uint64{mp_id: 1, cmp_id: 2, c_id: 3})
	if len(keys) == 0 && sn.nodeType != NodeLeaf {
		return
	}
	if sn.nodeType == NodeUndecided {
		return
	}
	if sn.nodeType == NodeNonLeaf {
		child, ok := sn.children[keys[sn.level]]
		if !ok {
			return
		}
		return child.GetBatch(keys)
	}
	return sn.viewPtr.Vlist, true
}

func (sn *FlatNode[K, VT, V, VList]) GetSnapshot(keys []K, deepCopy bool) *ShardSnapshot[K] {
	if len(keys) == 0 {
		return nil
	}
	if sn.nodeType != NodeLeaf {
		child, ok := sn.children[keys[sn.level]]
		if ok {
			return child.GetSnapshot(keys, deepCopy)
		}
	}
	sn.rwMutex.RLock()
	defer sn.rwMutex.RUnlock()
	view := *sn.viewPtr // never nil
	// check if shard is not empty
	if len(view.indexes) == 0 {
		return nil
	}
	keyList := make([]K, 0, len(view.indexes))
	for k := range view.indexes {
		keyList = append(keyList, k)
	}
	if !deepCopy {
		return &ShardSnapshot[K]{
			Path:   keys,
			Keys:   keyList,
			Buffer: sn.ReadBuffer, // This will be valid until it will be cycled to -> BackupBuffer -> WriteBuffer
		}
	}
	dest := make([]byte, len(sn.ReadBuffer))
	copy(dest, sn.ReadBuffer)
	return &ShardSnapshot[K]{
		Path:   keys,
		Keys:   keyList,
		Buffer: dest,
	}

}

func (sn *FlatNode[K, VT, V, VList]) Set(v DeltaItem[K]) error {
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

func (sn *FlatNode[K, VT, V, VList]) SetSnapshot(v *ShardSnapshot[K]) error {
	if len(v.Path) == 0 {
		return fmt.Errorf("no keys provided")
	}
	if sn.conf.SnapShotMode == SnapshotModeProducer {
		return fmt.Errorf("snapshot mode is producer")
	}
	if sn.nodeType == NodeNonLeaf {
		child, ok := sn.children[v.Path[sn.level]]
		if ok {
			return child.SetSnapshot(v)
		}
	}
	sn.rwMutex.Lock()
	sn.shardSnapshot = v
	sn.rwMutex.Unlock()
	return nil
}

func (sn *FlatNode[K, VT, V, VList]) Delete(keys []K) {
	if len(keys) == 0 {
		return
	}
	if sn.nodeType == NodeLeaf {
		if _, ok := sn.viewPtr.indexes[keys[sn.level]]; !ok {
			return
		}
		sn.rwMutex.Lock()
		sn.deleted[keys[sn.level]] = struct{}{}
		sn.rwMutex.Unlock()
		return
	}
	child, ok := sn.children[keys[sn.level]]
	if !ok {
		return
	}
	child.Delete(keys)
}
