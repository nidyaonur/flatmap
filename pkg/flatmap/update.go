package flatmap

import (
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
)

func (sn *FlatNode[K, V, VList]) PeriodicUpdate() {
	for {
		time.Sleep(time.Duration(sn.conf.UpdateSeconds) * time.Second)
		if len(sn.pendingDelta) == 0 && len(sn.deleted) == 0 {
			continue
		}
		sn.Update(nil)

	}
}

func (sn *FlatNode[K, V, VList]) FeedDeltaBulk(deltaList []DeltaItem[K]) {
	if len(deltaList) == 0 {
		return
	}
	sn.Update(deltaList)
}

func (sn *FlatNode[K, V, VList]) DecideNodeType() {
	if sn.nodeType == NodeUndecided { //decide on whether or not this should be leaf
		var keyLen int
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
	}
}

func (sn *FlatNode[K, V, VList]) Update(bulkDelta []DeltaItem[K]) {
	sn.rwMutex.Lock()
	defer sn.rwMutex.Unlock()
	if len(bulkDelta) != 0 {
		sn.pendingDelta = append(sn.pendingDelta, bulkDelta...)
	}
	// Decide Node type
	sn.DecideNodeType()
	if sn.nodeType == NodeUndecided { //There is no data to decide
		return
	}

	// if there is any data in this node, there is only two possibilities
	// 1. this is a leaf node and the data is in the pending buffer to be written
	// 2. this is an internal node there is no child node created for the specific key, and set method could not delegate the "data write" to the child node
	if sn.nodeType == NodeLeaf {
		// populate pending keys
		var pendingKeys map[K]int = make(map[K]int, len(sn.pendingDelta))
		for i := len(sn.pendingDelta) - 1; i >= 0; i-- { // to take the latest data when there are duplicates
			if _, ok := pendingKeys[sn.pendingDelta[i].Keys[sn.level]]; ok {
				continue
			}
			pendingKeys[sn.pendingDelta[i].Keys[sn.level]] = i
		}
		// if it is the first time, we need to initialize the buffers
		var childrenLen int
		if sn.Builder == nil {
			sn.Builder = flatbuffers.NewBuilder(1024)
			sn.ReadBuffer = make([]byte, 0, 1024)
			sn.WriteBuffer = make([]byte, 0, 1024)
			sn.BackupBuffer = make([]byte, 0, 1024)

		} else {
			childrenLen = sn.Vlist.ChildrenLength()
		}

		// reuse backup buffer
		sn.Builder.Bytes = sn.WriteBuffer
		sn.Builder.Reset()
		// copy the read buffer to the write buffer
		newIndexes := make(map[K]int64)
		vList := make([]V, 0, len(pendingKeys)+childrenLen)

		strOffsets := make(map[string]flatbuffers.UOffsetT)
		vFieldConfig := sn.conf.tableConfigs[sn.conf.vName]

		for i := 0; i < childrenLen; i++ { //TODO: assign to a variable
			vObj := sn.conf.NewV()
			created := sn.Vlist.Children(vObj, i)
			if !created {
				// log.Println("Failed to create V object")
				continue
			}
			keys := sn.conf.GetKeysFromV(vObj)
			if _, ok := sn.deleted[keys[sn.level]]; ok {
				continue
			}
			if _, ok := pendingKeys[keys[sn.level]]; ok {
				continue
			}
			vList = append(vList, vObj)
			newIndexes[keys[sn.level]] = int64(len(vList) - 1)

			sn.FillStrOffsets(vObj, strOffsets, vFieldConfig)
		}
		// listVector := sn.GetRootAsVList(sn.pendingBuffer.Bytes())
		vLen := len(vList)
		for _, i := range pendingKeys {
			delta := sn.pendingDelta[i]
			newIndexes[delta.Keys[sn.level]] = int64(vLen)
			vObj := sn.GetRootAsV(delta.Data)
			vList = append(vList, vObj)
			sn.FillStrOffsets(vObj, strOffsets, vFieldConfig)
			// fmt.Println(delta.Keys[sn.level])
			vLen++

		}
		vectorOffsets := sn.FillVectorOffsetsMap(vList, strOffsets)
		vOffsets := make([]flatbuffers.UOffsetT, len(vList))
		for i, v := range vList {
			vOffsets[i] = sn.FillVFields(v, strOffsets, vectorOffsets[i])
		}

		sn.VListStartChildrenVector(sn.Builder, len(vOffsets))
		for i := len(vOffsets) - 1; i >= 0; i-- {
			sn.Builder.PrependUOffsetT(vOffsets[i])
		}
		vVector := sn.Builder.EndVector(len(vOffsets))
		sn.VListStart(sn.Builder)
		sn.VListAddChildren(sn.Builder, vVector)
		vListOffset := sn.End(sn.Builder)
		sn.Builder.Finish(vListOffset)
		oldRead := sn.ReadBuffer
		sn.ReadBuffer = sn.Builder.FinishedBytes()
		sn.WriteBuffer = sn.BackupBuffer
		sn.BackupBuffer = oldRead
		sn.indexes = newIndexes // TODO: we can reuse it in the same way as the triple buffer
		sn.Vlist = sn.GetRootAsVList(sn.ReadBuffer)
		sn.pendingDelta = make([]DeltaItem[K], 0)

	} else {
		// group the data by the next level of keys
		groupedDelta := make(map[K][]DeltaItem[K])
		for idx := range sn.pendingDelta {
			delta := sn.pendingDelta[idx]
			shardKey := delta.Keys[sn.level]
			if _, ok := groupedDelta[shardKey]; !ok { //TODO: check if this is necessary
				groupedDelta[shardKey] = make([]DeltaItem[K], 0)
			}
			groupedDelta[shardKey] = append(groupedDelta[shardKey], delta)
		}
		sn.pendingDelta = make([]DeltaItem[K], 0)
		newKeys := []K{}
		for key := range groupedDelta {
			if _, ok := sn.children[key]; !ok {
				sn.children[key] = NewFlatNode(sn.conf, nil, sn.level+1)
				newKeys = append(newKeys, key)
			}
		}
		for _, key := range newKeys {
			sn.initializationWG.Add(1)
			go func(key K) {
				sn.children[key].FeedDeltaBulk(groupedDelta[key])
				sn.initializationWG.Done()
			}(key)
		}

		sn.initializationWG.Wait()
	}

}
