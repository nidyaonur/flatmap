package flatmap

import (
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
)

func (sn *FlatNode[K, V, VList]) PeriodicUpdate() {
	for {
		time.Sleep(time.Duration(sn.UpdateSeconds) * time.Second)
		sn.rwMutex.RLock()
		if len(sn.pendingKeys) == 0 && len(sn.deleted) == 0 {
			sn.rwMutex.Unlock()
			continue
		}
		sn.rwMutex.Unlock()
		sn.Update()
	}
}

func (sn *FlatNode[K, V, VList]) FeedBuffer(buffer []byte, keysList [][]K) error {
	if len(buffer) == 0 {
		return nil
	}
	if keysList == nil || len(keysList) == 0 {
		// fill keysList with the keys from the buffer
		vList := sn.GetRootAsVList(buffer)
		keysList = make([][]K, vList.ChildrenLength())
		for i := 0; i < vList.ChildrenLength(); i++ {
			vObj := sn.Callbacks.NewV()
			created := vList.Children(vObj, i)
			if !created {
				// log.Println("Failed to create V object")
				continue
			}
			keysList[i] = sn.GetKeysFromV(vObj)
		}
	}
	sn.rwMutex.Lock()
	defer sn.rwMutex.Unlock()
	sn.pendingBuffer.Write(buffer)
	sn.pendingKeys = keysList

}

func (sn *FlatNode[K, V, VList]) Update() {
	sn.rwMutex.Lock()
	defer sn.rwMutex.Unlock()
	// if there is any data in this node, there is only two possibilities
	// 1. this is a leaf node and the data is in the pending buffer to be written
	// 2. this is an internal node there is no child node created for the specific key, and set method could not delegate the "data write" to the child node
	if sn.isLeaf {
		// reuse backup buffer
		sn.Builder.Bytes = sn.WriteBuffer
		sn.Builder.Reset()
		// copy the read buffer to the write buffer
		newIndexes := make(map[K]int64)
		childrenLen := sn.Vlist.ChildrenLength()
		vList := make([]V, 0, len(sn.pendingKeys)+childrenLen)
		i := 0

		strOffsets := make(map[string]flatbuffers.UOffsetT)
		vFieldConfig := sn.TableConfigs[sn.VName]
		for ; i < childrenLen; i++ { //TODO: assign to a variable
			vObj := sn.Callbacks.NewV()
			created := sn.Vlist.Children(vObj, i)
			if !created {
				// log.Println("Failed to create V object")
				continue
			}
			keys := sn.GetKeysFromV(vObj)
			if _, ok := sn.deleted[keys[sn.level]]; ok {
				continue
			}
			if _, ok := sn.pendingKeyMap[keys[sn.level]]; ok {
				continue
			}
			vList = append(vList, vObj)
			newIndexes[keys[sn.level]] = int64(len(vList) - 1)

			sn.FillStrOffsets(vObj, strOffsets, vFieldConfig)
		}
		i = len(vList)
		listVector := sn.GetRootAsVList(sn.pendingBuffer.Bytes())
		for idx, keys := range sn.pendingKeys {
			newIndexes[keys[sn.level]] = int64(i)
			vObj := sn.Callbacks.NewV()
			created := listVector.Children(vObj, int(sn.pendingOffsets[idx]))
			if !created {
				// log.Println("Failed to create V object")
				continue
			}
			vList = append(vList, vObj)
			i++
			sn.FillStrOffsets(vObj, strOffsets, vFieldConfig)

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
		sn.indexes = newIndexes
		sn.Vlist = sn.GetRootAsVList(sn.ReadBuffer)
	} else {
		// group the data by the next level of keys
		groupedData := make(map[K][]int)
		for i, iKey := range sn.pendingKeys {
			groupedData[iKey[sn.level]] = append(groupedData[iKey[sn.level]], i)
		}
		for key := range groupedData {
			childConfig := FlatConfig[K, V, VList]{
				Level:         sn.level + 1,
				InitialBuffer: sn.pendingBuffer.Bytes(),
				InitialKeys:   sn.pendingKeys,
				keyIndexes:    groupedData[key],
				UpdateSeconds: sn.UpdateSeconds,
				Callbacks:     sn.Callbacks,
				parentWg:      sn.initializationWG,
				TableConfigs:  sn.TableConfigs,
				vName:         sn.VName,
				fieldCount:    sn.FieldCount,
			}
			sn.children[key] = NewFlatNode(childConfig, nil)
		}

	}
}
