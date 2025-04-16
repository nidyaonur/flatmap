# FlatMap

A lock-free, logically sharded, FlatBuffers wrapper map implementation for Go.

## Overview

FlatMap provides a high-performance, memory-efficient approach to managing structured data with minimal GC pressure. It implements a triple buffer strategy to swap buffers, enabling fast concurrent reads and iterations without impacting garbage collection.

### Key Features

- Lock-free concurrent access with minimal contention
- Logically sharded data organization
- Minimal GC pressure during operations (zero allocations)
- Hierarchical (n-level) map structure
- Optimized for parallel access
- Snapshot capability for consistent point-in-time views

## Installation

```bash
go get github.com/nidyaonur/flatmap
```

## Usage Examples

### Basic Usage

```go
package main

import (
    "github.com/google/flatbuffers/go"
    "github.com/nidyaonur/flatmap/pkg/flatmap"
    "your/flatbuffers/schema"
)

func main() {
    // Create a FlatMap configuration
    conf := &flatmap.FlatConfig[int, *schema.DataT, *schema.Data, *schema.DataList]{
        UpdateSeconds: 15, // Buffer swap interval
        NewV: func() *schema.Data {
            return &schema.Data{}
        },
        NewVList: func() *schema.DataList {
            return &schema.DataList{}
        },
        GetKeysFromV: func(d *schema.Data) []int {
            return []int{int(d.Id())}
        },
    }
    
    // Initialize the FlatMap
    flatMap := flatmap.NewFlatNode(conf, 0)
    
    // Add items
    builder := flatbuffers.NewBuilder(1024)
    // [Build your flatbuffer object here]
    // ...
    schema.DataStart(builder)
    schema.DataAddId(builder, 123)
    // [Add other fields]
    data := schema.DataEnd(builder)
    builder.Finish(data)
    
    // Set item in the map
    flatMap.Set(flatmap.DeltaItem[int]{
        Keys: []int{123},
        Data: builder.FinishedBytes(),
    })
    
    // Retrieve item from the map
    item := &schema.Data{}
    if ok := flatMap.Get([]int{123}, item); ok {
        // Use the item
        id := item.Id()
        // ...
    }
}
```

### Two-Level Map

```go
// Initialize two-level map (bucket -> key)
twoLevelMap := flatmap.NewFlatNode(conf, 0)

// Set item with bucket and key
twoLevelMap.Set(flatmap.DeltaItem[int]{
    Keys: []int{bucketId, itemId},
    Data: builder.FinishedBytes(),
})

// Get item by bucket and key
twoLevelMap.Get([]int{bucketId, itemId}, item)

// Get all items in a bucket
bucketItems, ok := twoLevelMap.GetBatch([]int{bucketId})
```

### Batch Initialization with Snapshot

```go
// Create snapshot buffer
snapshot := &flatmap.ShardSnapshot[int]{
    Keys:   []int{1, 2, 3}, // Item keys
    Path:   []int{},        // Empty for root level
    Buffer: serializedListBuffer, // Serialized DataList buffer
}

// Initialize with snapshot (much more efficient than item-by-item)
flatMap.InitializeWithGroupedShardBuffers([]*flatmap.ShardSnapshot[int]{snapshot})
```

## Limitations

- Single shards cannot exceed 2GB (FlatBuffers limitation)
- Performance depends on proper field access patterns
- Must follow FlatBuffers best practices for optimal performance

## Performance

FlatMap is designed for specific use cases where GC pressure and concurrent access are important:

- **Zero Allocation Operations**: All operations show 0 B/op and 0 allocs/op
- **Superior Parallel Performance**: Outperforms standard maps in concurrent scenarios
- **Consistent Memory Behavior**: Stable memory usage during operations

### When to Use FlatMap

- High-concurrency workloads
- Applications sensitive to GC pauses
- Systems requiring consistent point-in-time views (snapshots)
- Hierarchical data structures with logical sharding

## Benchmark Results

Latest results on Apple M4 Pro:

| Implementation | Memory | Heap Objects | Single Read | Parallel Read |
|----------------|--------|--------------|-------------|---------------|
| FlatMap | 72.4 MB | 600,562 | 12.94 ns/op | - |
| FlatMap w/Snapshot | 103.6 MB | 26 | - | - |
| Standard Map | 24.6 MB | 697,557 | 5.59 ns/op | - |
| Two-Level FlatMap | 99.3 MB | 655,082 | 28.08 ns/op | 13.05 ns/op |
| Two-Level Std Map | 32.9 MB | 901,571 | 15.54 ns/op | 14.23 ns/op |

For detailed benchmarks run:

```bash
go test -bench=. -benchmem ./example
```

## License

[License details here]