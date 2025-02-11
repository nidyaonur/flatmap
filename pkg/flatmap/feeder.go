package flatmap

// // FeedSingleBuffer feeds a single buffer to the FlatNode.
// //
// // keysList is optional but when provided, it will increase the performance greatly
// //
// // keyIndexes is used internally, so it should not be provided, if provided(correctly), it will have little to no effect on the performance
// func (sn *FlatNode[K, V, VList]) FeedSingleBuffer(buffer []byte, keysList [][]K) error {
// 	if len(buffer) == 0 {
// 		return nil
// 	}
// 	keys := initialKeys[0]
// 	sn.isLeaf = sn.level+1 == len(keys)
// 	traverseKeyIndexes := false
// 	traverseLen := len(initialKeys)
// 	if len(keyIndexes) != 0 {
// 		traverseKeyIndexes = true
// 		traverseLen = len(keyIndexes)
// 	}
// 	if sn.isLeaf {
// 		sn.initializationWG.Add(1)
// 		go func() {
// 			// Initialize buffer
// 			// sn.ReadBuffer = bytes.NewBuffer(make([]byte, 0, 1024))
// 			// sn.WriteBuffer = bytes.NewBuffer(make([]byte, 0, 1024))
// 			indexes := make(map[K]int64)
// 			sn.Builder = flatbuffers.NewBuilder(1024 * traverseLen)

// 			listVector := sn.GetRootAsVList(initialBuffer)
// 			vList := make([]V, traverseLen)

// 			// Preset all String values
// 			strOffsets := make(map[string]flatbuffers.UOffsetT)
// 			for i := 0; i < traverseLen; i++ {
// 				vObj := sn.Callbacks.NewV() // TODO: find a way to reuse the object
// 				// Serialize the value
// 				index := i
// 				if traverseKeyIndexes {
// 					index = keyIndexes[i]
// 				}
// 				got := listVector.Children(vObj, index)
// 				if !got {
// 					continue
// 				}
// 				sn.FillStrOffsets(vObj, strOffsets, sn.TableConfigs[sn.VName])

// 				vList[i] = vObj
// 			}
// 			vectorOffsetsMapList := sn.FillVectorOffsetsMap(vList, strOffsets)
// 			vOffsets := make([]flatbuffers.UOffsetT, len(vList))
// 			for i, v := range vList {
// 				vOffsets[i] = sn.FillVFields(v, strOffsets, vectorOffsetsMapList[i])
// 				index := i
// 				if traverseKeyIndexes {
// 					index = keyIndexes[i]
// 				}
// 				indexes[initialKeys[index][sn.level]] = int64(i) // get the key from the initial keys using the mapping from keyIndexes
// 			}
// 			sn.VListStartChildrenVector(sn.Builder, len(vOffsets))
// 			for i := len(vOffsets) - 1; i >= 0; i-- {
// 				sn.Builder.PrependUOffsetT(vOffsets[i])
// 			}
// 			vVector := sn.Builder.EndVector(len(vOffsets))
// 			sn.VListStart(sn.Builder)
// 			sn.VListAddChildren(sn.Builder, vVector)
// 			vListOffset := sn.End(sn.Builder)

// 			sn.Builder.Finish(vListOffset)
// 			sn.ReadBuffer = sn.Builder.FinishedBytes()
// 			sn.indexes = indexes
// 			sn.Vlist = sn.GetRootAsVList(sn.ReadBuffer)
// 			sn.initializationWG.Done()
// 		}()

// 	} else {
// 		// group the data by the next level of keys
// 		groupedData := make(map[K][]int)
// 		for i := 0; i < traverseLen; i++ {
// 			index := i
// 			if traverseKeyIndexes {
// 				index = keyIndexes[i]
// 			}
// 			groupedData[initialKeys[index][sn.level]] = append(groupedData[initialKeys[index][sn.level]], index)
// 		}
// 		for key := range groupedData {
// 			childConfig := FlatConfig[K, V, VList]{
// 				Level:         sn.level + 1,
// 				InitialBuffer: initialBuffer,
// 				InitialKeys:   initialKeys,
// 				keyIndexes:    groupedData[key],
// 				UpdateSeconds: sn.UpdateSeconds,
// 				Callbacks:     sn.Callbacks,
// 				parentWg:      sn.initializationWG,
// 				TableConfigs:  sn.TableConfigs,
// 				vName:         sn.VName,
// 				fieldCount:    sn.FieldCount,
// 			}
// 			sn.children[key] = NewFlatNode(childConfig, nil)
// 		}
// 	}
// 	return nil
// }
