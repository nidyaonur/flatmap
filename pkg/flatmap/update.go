package flatmap

import (
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
)

func (sn *FlatNode[K, VT, V, VList]) PeriodicUpdate() {
	for {
		time.Sleep(time.Duration(sn.conf.UpdateSeconds) * time.Second)
		if len(sn.pendingDelta) == 0 && len(sn.deleted) == 0 {
			continue
		}
		sn.Update(nil)
	}
}

func (sn *FlatNode[K, VT, V, VList]) FeedDeltaBulk(deltaList []DeltaItem[K]) {
	if len(deltaList) == 0 {
		return
	}
	sn.Update(deltaList)
}

func (sn *FlatNode[K, VT, V, VList]) DecideNodeType() {
	if sn.nodeType == NodeUndecided { //decide on whether or not this should be leaf
		var keyLen int
		if sn.shardSnapshot != nil {
			sn.nodeType = NodeLeaf
			sn.EnsureCapacity()
			return
		}
		if len(sn.pendingDelta) == 0 {
			return
		}
		if len(sn.pendingDelta) != 0 && len(sn.pendingDelta[0].Keys) != 0 {
			keyLen = len(sn.pendingDelta[0].Keys)
		}
		if keyLen == 0 {
			panic("invalid data") //TODO: gracefully handle it
		}
		if sn.level+1 == keyLen {
			sn.nodeType = NodeLeaf
		} else {
			sn.nodeType = NodeNonLeaf
		}
		// Initialize appropriate data structures based on node type
		sn.EnsureCapacity()
	}
}

func (sn *FlatNode[K, VT, V, VList]) InitializeWithGroupedShardBuffers(snapshots []*ShardSnapshot[K]) {
	if sn.conf.SnapShotMode == SnapshotModeProducer {
		return
	}
	for _, ss := range snapshots { // this would have been horrible before go 1.21
		if sn.level == len(ss.Path) { // path contains the keys for the current level
			sn.nodeType = NodeLeaf
		} else {
			sn.nodeType = NodeNonLeaf
		}
		if sn.nodeType == NodeNonLeaf {
			if sn.children == nil {
				sn.children = make(map[K]*FlatNode[K, VT, V, VList])
			}
			if _, ok := sn.children[ss.Path[sn.level]]; !ok {
				sn.children[ss.Path[sn.level]] = NewFlatNode(sn.conf, sn.level+1)
			}
			sn.children[ss.Path[sn.level]].InitializeWithGroupedShardBuffers([]*ShardSnapshot[K]{ss})
		} else {
			sn.rwMutex.Lock()
			defer sn.rwMutex.Unlock()
			sn.shardSnapshot = ss
		}
	}
}

func (sn *FlatNode[K, VT, V, VList]) Update(bulkDelta []DeltaItem[K]) {
	sn.rwMutex.Lock()
	defer sn.rwMutex.Unlock()

	sn.appendBulkDeltaIfNeeded(bulkDelta)

	// Decide Node type
	sn.DecideNodeType()
	if sn.nodeType == NodeUndecided { //There is no data to decide
		return
	}

	// if there is any data in this node, there is only two possibilities
	// 1. this is a leaf node and the data is in the pending buffer to be written
	// 2. this is an internal node there is no child node created for the specific key, and set method could not delegate the "data write" to the child node
	if sn.nodeType == NodeLeaf {
		sn.updateLeafNode()
	} else {
		sn.updateNonLeafNode()
	}
}

func (sn *FlatNode[K, VT, V, VList]) appendBulkDeltaIfNeeded(bulkDelta []DeltaItem[K]) {
	if len(bulkDelta) == 0 {
		return
	}

	// Pre-allocate capacity if needed to avoid reallocations
	if cap(sn.pendingDelta)-len(sn.pendingDelta) < len(bulkDelta) {
		newCap := len(sn.pendingDelta) + len(bulkDelta)
		newSlice := make([]DeltaItem[K], len(sn.pendingDelta), newCap)
		copy(newSlice, sn.pendingDelta)
		sn.pendingDelta = newSlice
	}
	sn.pendingDelta = append(sn.pendingDelta, bulkDelta...)
}

func (sn *FlatNode[K, VT, V, VList]) updateLeafNode() {
	if sn.shardSnapshot != nil && sn.conf.SnapShotMode == SnapshotModeConsumer {
		sn.initializeLeafFromSnapshot()
	}

	pendingKeys := sn.collectPendingKeys()

	// if it is the first time, we need to initialize the buffers
	var childrenLen int
	if sn.Builder == nil {
		sn.initializeBuffers(pendingKeys)
	} else {
		childrenLen = sn.Vlist.ChildrenLength()
	}

	// Process and update the data
	sn.processLeafData(pendingKeys, childrenLen)
}

func (sn *FlatNode[K, VT, V, VList]) initializeLeafFromSnapshot() {
	vList := sn.GetRootAsVList(sn.shardSnapshot.Buffer)
	sn.Vlist = vList
	sn.shardSnapshot = nil
	sn.pendingDelta = make([]DeltaItem[K], 0, 16) // Provide initial capacity
	sn.deleted = make(map[K]struct{})
	for i, k := range sn.shardSnapshot.Keys {
		sn.indexes[k] = i
	}
	sn.pendingKeys = make(map[K]struct{}, len(sn.shardSnapshot.Keys))
	sn.ReadBuffer = sn.shardSnapshot.Buffer
}

func (sn *FlatNode[K, VT, V, VList]) collectPendingKeys() map[K]int {
	expectedSize := len(sn.pendingDelta)
	var pendingKeys map[K]int

	// Create map with appropriate initial capacity
	if expectedSize > 0 {
		pendingKeys = make(map[K]int, expectedSize)
	} else {
		pendingKeys = make(map[K]int)
	}

	for i := len(sn.pendingDelta) - 1; i >= 0; i-- { // to take the latest data when there are duplicates
		if _, ok := pendingKeys[sn.pendingDelta[i].Keys[sn.level]]; ok {
			continue
		}
		pendingKeys[sn.pendingDelta[i].Keys[sn.level]] = i
	}

	return pendingKeys
}

func (sn *FlatNode[K, VT, V, VList]) initializeBuffers(pendingKeys map[K]int) {
	// Size buffers according to expected data size
	initialSize := max(1024, estimateBufferSize(len(pendingKeys)))
	sn.Builder = flatbuffers.NewBuilder(initialSize)
	sn.ReadBuffer = make([]byte, 0, initialSize)
	sn.WriteBuffer = make([]byte, 0, initialSize)
	sn.BackupBuffer = make([]byte, 0, initialSize)
}

func (sn *FlatNode[K, VT, V, VList]) processLeafData(pendingKeys map[K]int, childrenLen int) {
	// reuse backup buffer
	sn.Builder.Bytes = sn.WriteBuffer
	sn.Builder.Reset()

	// Estimate proper capacity for maps and slices
	totalItems := len(pendingKeys) + childrenLen
	newIndexes := make(map[K]int, totalItems)
	newOffsets := make([]flatbuffers.UOffsetT, 0, totalItems)

	// Process existing children first
	sn.processExistingChildren(newIndexes, &newOffsets, childrenLen, pendingKeys)

	// Then process pending deltas
	sn.processPendingDeltas(newIndexes, &newOffsets, pendingKeys)

	// Build and update the flatbuffer
	sn.buildAndUpdateFlatBuffer(newIndexes, newOffsets)
}

func (sn *FlatNode[K, VT, V, VList]) processExistingChildren(
	newIndexes map[K]int,
	newOffsets *[]flatbuffers.UOffsetT,
	childrenLen int,
	pendingKeys map[K]int,
) {
	// Create a reusable object for VT rather than creating one per iteration
	var vt VT
	var vObj V = sn.conf.NewV()

	for i := range childrenLen {
		created := sn.Vlist.Children(vObj, i)
		if !created {
			continue
		}
		keys := sn.conf.GetKeysFromV(vObj)
		if _, ok := sn.deleted[keys[sn.level]]; ok {
			continue
		}
		if _, ok := pendingKeys[keys[sn.level]]; ok {
			continue
		}

		if len(*newOffsets) == 0 {
			vt = vObj.UnPack()
		} else {
			vObj.UnPackTo(vt)
		}
		newIndexes[keys[sn.level]] = len(*newOffsets)
		*newOffsets = append(*newOffsets, vt.Pack(sn.Builder))
	}
}

func (sn *FlatNode[K, VT, V, VList]) processPendingDeltas(
	newIndexes map[K]int,
	newOffsets *[]flatbuffers.UOffsetT,
	pendingKeys map[K]int,
) {
	deleteFuncSet := sn.conf.CheckVForDelete != nil
	var vt VT
	var vObj V = sn.conf.NewV()

	for _, i := range pendingKeys {
		delta := sn.pendingDelta[i]
		sn.GetRootAsV(delta.Data, vObj)
		if deleteFuncSet && sn.conf.CheckVForDelete(vObj) {
			continue
		}
		if len(*newOffsets) == 0 {
			vt = vObj.UnPack()
		} else {
			vObj.UnPackTo(vt)
		}
		newIndexes[delta.Keys[sn.level]] = len(*newOffsets)
		*newOffsets = append(*newOffsets, vt.Pack(sn.Builder))
	}
}

func (sn *FlatNode[K, VT, V, VList]) buildAndUpdateFlatBuffer(
	newIndexes map[K]int,
	newOffsets []flatbuffers.UOffsetT,
) {
	sn.VListStartChildrenVector(sn.Builder, len(newOffsets))
	for i := len(newOffsets) - 1; i >= 0; i-- {
		sn.Builder.PrependUOffsetT(newOffsets[i])
	}
	vVector := sn.Builder.EndVector(len(newOffsets))
	sn.VListStart(sn.Builder)
	sn.VListAddChildren(sn.Builder, vVector)
	vListOffset := sn.End(sn.Builder)
	sn.Builder.Finish(vListOffset)

	// Update buffers with properly sized allocations if needed
	finishedBytes := sn.Builder.FinishedBytes()
	finishedSize := len(finishedBytes)

	// Grow buffers if they're significantly smaller than needed
	if cap(sn.BackupBuffer) < finishedSize*2/3 {
		newSize := finishedSize * 2
		sn.BackupBuffer = make([]byte, 0, newSize)
	}

	oldRead := sn.ReadBuffer
	sn.ReadBuffer = finishedBytes
	sn.WriteBuffer = sn.BackupBuffer
	sn.BackupBuffer = oldRead

	sn.indexes = newIndexes
	sn.Vlist = sn.GetRootAsVList(sn.ReadBuffer)
	// If the finished buffer is %75 or more full(1.5GB), indicate that
	if len(newIndexes) > 0 { // 1.5GB
		firstElem := sn.conf.NewV()
		sn.Vlist.Children(firstElem, 0)
		keys := sn.conf.GetKeysFromV(firstElem)
		floatSize := float64(finishedSize) / (1024 * 1024 * 1024)
		logLevel := InfoLevel
		if floatSize > 1.5 { // Crash if we go over 2GB
			logLevel = WarnLevel
		}
		sn.logf(logLevel, "Finished bytes size: %.2f GB, level: %d, element count: %d, keys: %v\n",
			floatSize, sn.level, len(newIndexes), keys[:sn.level])
	}

	// Clear without reallocation
	sn.pendingDelta = sn.pendingDelta[:0]
}

func (sn *FlatNode[K, VT, V, VList]) updateNonLeafNode() {
	// Group and distribute deltas to child nodes
	groupedDeltas := sn.groupDeltasByNextLevelKey()

	// Clear pending deltas early
	sn.pendingDelta = sn.pendingDelta[:0]

	// Create or update child nodes
	newKeys := sn.prepareChildNodes(groupedDeltas)

	// Process child nodes in parallel
	sn.processChildNodesInParallel(newKeys, groupedDeltas)
}

func (sn *FlatNode[K, VT, V, VList]) groupDeltasByNextLevelKey() map[K][]DeltaItem[K] {
	groupedDelta := make(map[K][]DeltaItem[K])
	for idx := range sn.pendingDelta {
		delta := sn.pendingDelta[idx]
		shardKey := delta.Keys[sn.level]
		if _, ok := groupedDelta[shardKey]; !ok {
			// Estimate typical group size to reduce reallocations
			groupedDelta[shardKey] = make([]DeltaItem[K], 0, 4)
		}
		groupedDelta[shardKey] = append(groupedDelta[shardKey], delta)
	}
	return groupedDelta
}

func (sn *FlatNode[K, VT, V, VList]) prepareChildNodes(groupedDeltas map[K][]DeltaItem[K]) []K {
	// Pre-allocate newKeys slice based on expected number of new children
	numNewKeys := 0
	for key := range groupedDeltas {
		if _, ok := sn.children[key]; !ok {
			numNewKeys++
		}
	}

	newKeys := make([]K, 0, numNewKeys)
	for key := range groupedDeltas {
		if _, ok := sn.children[key]; !ok {
			sn.children[key] = NewFlatNode(sn.conf, sn.level+1)
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}

func (sn *FlatNode[K, VT, V, VList]) processChildNodesInParallel(newKeys []K, groupedDeltas map[K][]DeltaItem[K]) {
	for _, key := range newKeys {
		sn.initializationWG.Add(1)
		go func(key K) {
			sn.children[key].FeedDeltaBulk(groupedDeltas[key])
			sn.initializationWG.Done()
		}(key)
	}

	sn.initializationWG.Wait()
}
