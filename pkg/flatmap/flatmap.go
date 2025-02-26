package flatmap

import (
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
)

// CallbackConfig holds functions to create new V/VList values as well as getter/setter functions.
// (Note: for enums we treat them as int8.)
type FlatConfig[K comparable, V VType, VList VListType[V]] struct {
	// Public fields
	NewV          func() V
	NewVList      func() VList
	GetKeysFromV  func(v V) []K
	UpdateSeconds int

	// Private fields
	fieldCount     int
	tableConfigs   map[string][]FieldConfig
	EnumByteGetter func(enumName string, getter interface{}) (func(V) byte, error)
	vName          string

	// Sorry for the long list of fields. We need to store all the getters for each field :(
	BoolGetters        map[string]func(v V) bool
	ByteGetters        map[string]func(v V) byte
	Int8Getters        map[string]func(v V) int8 // for enums
	Int16Getters       map[string]func(v V) int16
	Int32Getters       map[string]func(v V) int32
	Int64Getters       map[string]func(v V) int64
	Uint8Getters       map[string]func(v V) uint8
	Uint16Getters      map[string]func(v V) uint16
	Uint32Getters      map[string]func(v V) uint32
	Uint64Getters      map[string]func(v V) uint64
	Float32Getters     map[string]func(v V) float32
	Float64Getters     map[string]func(v V) float64
	StringGetters      map[string]func(v V) []byte
	BoolListGetters    map[string]func(v V, idx int) bool
	ByteListGetters    map[string]func(v V, idx int) byte
	Int8ListGetters    map[string]func(v V, idx int) int8
	Int16ListGetters   map[string]func(v V, idx int) int16
	Int32ListGetters   map[string]func(v V, idx int) int32
	Int64ListGetters   map[string]func(v V, idx int) int64
	Uint8ListGetters   map[string]func(v V, idx int) uint8
	Uint16ListGetters  map[string]func(v V, idx int) uint16
	Uint32ListGetters  map[string]func(v V, idx int) uint32
	Uint64ListGetters  map[string]func(v V, idx int) uint64
	Float32ListGetters map[string]func(v V, idx int) float32
	Float64ListGetters map[string]func(v V, idx int) float64
	StringListGetters  map[string]func(v V, idx int) []byte
	LengthGetters      map[string]func(v V) int
}

// FlatNode represents a node in the sharded map/tree structure.
type FlatNode[K comparable, V VType, VList VListType[V]] struct {
	// Whether this node is a leaf or an internal node
	nodeType NodeEnum

	// The level of the node in the shard tree
	level int

	// FieldCount int

	// Children in the shard tree keyed by hashes
	children map[K]*FlatNode[K, V, VList]

	// Triple buffer containing read/write/backup buffers
	ReadBuffer   []byte
	Vlist        VList
	WriteBuffer  []byte
	BackupBuffer []byte
	Builder      *flatbuffers.Builder

	// Metadata for reads, e.g. storing offsets/sizes of items
	indexes map[K]int64

	pendingDelta []DeltaItem[K]

	rwMutex          sync.RWMutex
	initializationWG *sync.WaitGroup

	deleted map[K]bool

	conf *FlatConfig[K, V, VList]
}

func NewFlatNode[K comparable, V VType, VList VListType[V]](
	conf *FlatConfig[K, V, VList],
	tableConfigs map[string][]map[string]string,
	level int,
) *FlatNode[K, V, VList] {
	if tableConfigs != nil {
		structuredTableConfigs := make(map[string][]FieldConfig)
		for k, v := range tableConfigs {
			fc := make([]FieldConfig, len(v))
			for i, f := range v {
				fc[i] = FieldConfig{
					Name:         f["name"],
					Type:         GetTypeEnum(f["type"]),
					DefaultValue: f["defaultValue"],
					Meta:         f["meta"],
					EnumName:     f["enumName"],
				}
			}
			structuredTableConfigs[k] = fc
		}
		conf.tableConfigs = structuredTableConfigs
	}
	sn := &FlatNode[K, V, VList]{
		children:         make(map[K]*FlatNode[K, V, VList]),
		indexes:          make(map[K]int64),
		level:            level,
		conf:             conf,
		rwMutex:          sync.RWMutex{},
		initializationWG: &sync.WaitGroup{},
		// FieldCount:       conf.fieldCount,
	}

	go sn.PeriodicUpdate()
	// sn.InitializeReceiversWithReflect()
	err := sn.FillGettersWithReflect()
	if err != nil {
		panic(err)
	}
	// sn.FillInitialData(conf.InitialBuffer, conf.InitialKeys, conf.keyIndexes)
	return sn
}

// func (sn *FlatNode[K, V, VList]) FillInitialData(initialBuffer []byte, initialKeys [][]K, keyIndexes []int) error {
// 	if len(initialBuffer) == 0 {
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

func (sn *FlatNode[K, V, VList]) GetRootAsV(buf []byte) V {
	n := flatbuffers.GetUOffsetT(buf[0:])
	x := sn.conf.NewV()
	x.Init(buf, n)
	return x
}

func (sn *FlatNode[K, V, VList]) GetRootAsVList(buf []byte) VList {
	n := flatbuffers.GetUOffsetT(buf[0:])
	x := sn.conf.NewVList()
	x.Init(buf, n)
	return x
}

func (sn *FlatNode[K, V, VList]) VStart(builder *flatbuffers.Builder) {
	builder.StartObject(sn.conf.fieldCount)
}

func (sn *FlatNode[K, V, VList]) VListStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}

func (sn *FlatNode[K, V, VList]) End(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

func (sn *FlatNode[K, V, VList]) VListStartChildrenVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}

func (sn *FlatNode[K, V, VList]) VListAddChildren(builder *flatbuffers.Builder, children flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(children), 0)
}
