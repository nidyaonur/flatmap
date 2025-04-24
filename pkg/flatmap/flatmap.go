package flatmap

import (
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
)

type SnapshotMode int

const (
	SnapshotModeConsumer SnapshotMode = iota // Consumer mode: means it will maintain snapshots on update and will accept snapshots from outside
	SnapshotModeProducer                     // Producer mode: means it will not maintain snapshots on update and will not produce snapshots
)

type View[K comparable, VT VTypeT, V VType[VT], VList VListType[VT, V]] struct {
	indexes map[K]int
	Vlist   VList // Reference to decoded list for faster access
}

// FlatNode represents a node in the sharded map/tree structure.
type FlatNode[K comparable, VT VTypeT, V VType[VT], VList VListType[VT, V]] struct {
	// Whether this node is a leaf or an internal node
	nodeType NodeEnum

	// The level of the node in the shard tree
	level int

	// Children in the shard tree keyed by hashes - only allocated for non-leaf nodes
	children map[K]*FlatNode[K, VT, V, VList]

	// Triple buffer containing read/write/backup buffers
	ReadBuffer   []byte // Stores current data for reading
	WriteBuffer  []byte // Buffer for building new content
	BackupBuffer []byte // Used for rotation to minimize allocations
	Builder      *flatbuffers.Builder

	// Metadata for reads, e.g. storing offsets/sizes of items
	viewPtr *View[K, VT, V, VList]

	// Use pointer for slices that may be empty much of the time
	pendingDelta []DeltaItem[K]
	pendingKeys  map[K]struct{}

	initializationWG *sync.WaitGroup

	// Use pointer type for optional/sparse data
	shardSnapshot *ShardSnapshot[K]

	// Use a more efficient mutex implementation
	rwMutex sync.RWMutex

	// For deleted entries tracking
	deleted map[K]struct{}

	conf *FlatConfig[K, VT, V, VList]
}

func NewFlatNode[K comparable, VT VTypeT, V VType[VT], VList VListType[VT, V]](
	conf *FlatConfig[K, VT, V, VList],
	level int,
) *FlatNode[K, VT, V, VList] {
	// Initialize only the required fields based on node type
	sn := &FlatNode[K, VT, V, VList]{
		level:            level,
		conf:             conf,
		rwMutex:          sync.RWMutex{},
		initializationWG: &sync.WaitGroup{},
		pendingDelta:     make([]DeltaItem[K], 0, 16), // Provide initial capacity
		pendingKeys:      make(map[K]struct{}, 16),    // Provide initial capacity
		// Initialize the builder with a default size

		// Allocate maps lazily when they're needed
		viewPtr: &View[K, VT, V, VList]{
			indexes: make(map[K]int, 16), // Provide initial capacity
		},
	}
	if conf.Logger == nil {
		conf.Logger = &noLogger{}
	}

	// Start periodic update in a separate goroutine
	go sn.PeriodicUpdate()
	return sn
}

// EnsureCapacity ensures that the node has adequate capacity for its data structures
func (sn *FlatNode[K, VT, V, VList]) EnsureCapacity() {
	// Initialize children map for non-leaf nodes only when needed
	if sn.nodeType == NodeNonLeaf && sn.children == nil {
		sn.children = make(map[K]*FlatNode[K, VT, V, VList])
	}

	// Initialize other maps only when needed
	if sn.nodeType == NodeLeaf {
		if sn.deleted == nil {
			sn.deleted = make(map[K]struct{})
		}
	}
}

func (sn *FlatNode[K, VT, V, VList]) GetRootAsV(buf []byte, x V) {
	n := flatbuffers.GetUOffsetT(buf[0:])
	x.Init(buf, n)
}

func (sn *FlatNode[K, VT, V, VList]) GetRootAsVList(buf []byte) VList {
	n := flatbuffers.GetUOffsetT(buf[0:])
	x := sn.conf.NewVList()
	x.Init(buf, n)
	return x
}

func (sn *FlatNode[K, VT, V, VList]) VListStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}

func (sn *FlatNode[K, VT, V, VList]) End(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}

func (sn *FlatNode[K, VT, V, VList]) VListStartChildrenVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}

func (sn *FlatNode[K, VT, V, VList]) VListAddChildren(builder *flatbuffers.Builder, children flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(children), 0)
}
