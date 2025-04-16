package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/nidyaonur/flatmap/example/books"
	"github.com/nidyaonur/flatmap/pkg/flatmap"
)

var (
	// Global variables for FlatMap
	globalFlatMap                    *flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList]
	globalTwoLevelFlatMap            *flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList]
	globalFlatMapWithSnapshot        *flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList]
	globaTwoLevelFlatMapWithSnapshot *flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList]

	// Global variables for standard Go map
	globalStdMap         map[int]*books.BookT
	globalTwoLevelStdMap map[int]map[int]*books.BookT

	// Global variables for sync.Map
	globalSyncMap *sync.Map

	// Test sizes and configuration
	largeTestSize = 100000
	numBuckets    = 100
)

// measureMapMemory captures memory stats before and after map creation
func measureMapMemory(before *runtime.MemStats) runtime.MemStats {
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	fmt.Printf("Memory allocation: %d bytes\n", after.HeapAlloc-before.HeapAlloc)
	fmt.Printf("Total allocations: %d\n", after.Mallocs-before.Mallocs)
	fmt.Printf("Heap objects: %d\n", after.HeapObjects-before.HeapObjects)
	fmt.Printf("Heap in use: %d bytes\n", after.HeapInuse-before.HeapInuse)
	fmt.Printf("Heap system: %d bytes\n", after.HeapSys-before.HeapSys)
	fmt.Printf("---------------------------------\n")

	return after
}

// TestMain initializes all data structures needed for benchmarks
func TestMain(m *testing.M) {
	// Measure memory usage for each map type
	var memBefore runtime.MemStats

	// Initialize single-level FlatMap
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	var err error
	globalFlatMap, err = constructFlatBookMap(largeTestSize)
	if err != nil {
		panic("Failed to construct global flat map for benchmarks")
	}
	fmt.Printf("FlatMap (%d items) memory statistics:\n", largeTestSize)
	measureMapMemory(&memBefore)

	// Initialize single-level FlatMap with a snapshot
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	globalFlatMapWithSnapshot, err = constructFlatBookMapWithSnapshot(largeTestSize)
	if err != nil {
		panic("Failed to construct global flat map with snapshot for benchmarks")
	}
	fmt.Printf("FlatMap with Snapshot (%d items) memory statistics:\n", largeTestSize)
	measureMapMemory(&memBefore)

	// Initialize single-level standard Map
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	globalStdMap = constructStandardMap(largeTestSize)
	fmt.Printf("Standard Map (%d items) memory statistics:\n", largeTestSize)
	measureMapMemory(&memBefore)

	// Initialize single-level sync.Map
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	globalSyncMap = constructSyncMap(largeTestSize)
	fmt.Printf("sync.Map (%d items) memory statistics:\n", largeTestSize)
	measureMapMemory(&memBefore)

	// Initialize two-level FlatMap
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	globalTwoLevelFlatMap, err = constructTwoLevelFlatBookMap(largeTestSize)
	if err != nil {
		panic("Failed to construct two-level flat map for benchmarks")
	}
	fmt.Printf("Two-Level FlatMap (%d items) memory statistics:\n", largeTestSize)
	measureMapMemory(&memBefore)

	// Initialize two-level FlatMap with a snapshot
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	globaTwoLevelFlatMapWithSnapshot, err = constructTwoLevelFlatMapWithSnapshot(largeTestSize)
	if err != nil {
		panic("Failed to construct two-level flat map with snapshot for benchmarks")
	}
	fmt.Printf("Two-Level FlatMap with Snapshot (%d items) memory statistics:\n", largeTestSize)
	measureMapMemory(&memBefore)

	// Initialize two-level standard Map
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	globalTwoLevelStdMap = constructTwoLevelStandardMap(largeTestSize)
	fmt.Printf("Two-Level Standard Map (%d items) memory statistics:\n", largeTestSize)
	measureMapMemory(&memBefore)

	// Run all tests
	os.Exit(m.Run())
}

// Initialize a two-level flat map (bucket -> key structure)
func constructTwoLevelFlatBookMap(testSize int) (*flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList], error) {
	deltaItems := make([]flatmap.DeltaItem[int], testSize)
	builder := flatbuffers.NewBuilder(1024)
	for i := range testSize {
		id := uint64(i + 1)
		bucket := int(id % uint64(numBuckets))

		title := builder.CreateString("Book Title")
		listField1 := builder.CreateString("Book SField")
		listField2 := builder.CreateString("Book SField")
		_ = books.BookStartListFieldVector(builder, 2)
		builder.PrependUOffsetT(listField2)
		builder.PrependUOffsetT(listField1)
		listFieldVector := builder.EndVector(2)
		_ = books.BookStartScalarListFieldVector(builder, 2)
		builder.PrependUint64(uint64(id + 1))
		builder.PrependUint64(uint64(id))
		scalarListFieldVector := builder.EndVector(2)
		books.BookStart(builder)
		books.BookAddId(builder, uint64(id))
		books.BookAddTitle(builder, title)
		books.BookAddPageCount(builder, uint64(id))
		books.BookAddScalarListField(builder, scalarListFieldVector)
		books.BookAddListField(builder, listFieldVector)
		book := books.BookEnd(builder)
		builder.Finish(book)

		// Use two-level key structure: [bucket, id]
		deltaItems[i] = flatmap.DeltaItem[int]{
			Keys: []int{bucket, int(id)},
			Data: builder.FinishedBytes(),
		}
		builder.Reset()
	}

	flatConf := &flatmap.FlatConfig[int, *books.BookT, *books.Book, *books.BookList]{
		UpdateSeconds: 15,
		NewV: func() *books.Book {
			return &books.Book{}
		},
		NewVList: func() *books.BookList {
			return &books.BookList{}
		},
		GetKeysFromV: func(b *books.Book) []int {
			id := int(b.Id())
			return []int{id % numBuckets, id}
		},
	}
	flatMap := flatmap.NewFlatNode(flatConf, 0)
	flatMap.FeedDeltaBulk(deltaItems)
	// deallocate deltas
	deltaItems = nil
	time.Sleep(30 * time.Second) // Allow some time for the map to process the deltas
	return flatMap, nil
}

func constructTwoLevelFlatMapWithSnapshot(testSize int) (*flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList], error) {
	builderMap := make(map[int]*flatbuffers.Builder, numBuckets)
	offsetMap := make(map[int][]flatbuffers.UOffsetT, numBuckets)
	snapshotMap := make([]*flatmap.ShardSnapshot[int], numBuckets)
	for i := range numBuckets {
		builderMap[i] = flatbuffers.NewBuilder(1024 * testSize / numBuckets)
		offsetMap[i] = make([]flatbuffers.UOffsetT, 0, testSize/numBuckets)
		snapshotMap[i] = &flatmap.ShardSnapshot[int]{
			Keys: make([]int, 0, testSize/numBuckets),
			Path: []int{i},
		}
	}
	var builder *flatbuffers.Builder
	for i := range testSize {
		id := uint64(i + 1)
		bucket := int(id % uint64(numBuckets))
		builder = builderMap[bucket]
		title := builder.CreateSharedString("Book Title")
		listField1 := builder.CreateSharedString("Book SField")
		listField2 := builder.CreateSharedString("Book SField")
		_ = books.BookStartListFieldVector(builder, 2)
		builder.PrependUOffsetT(listField2)
		builder.PrependUOffsetT(listField1)
		listFieldVector := builder.EndVector(2)
		_ = books.BookStartScalarListFieldVector(builder, 2)
		builder.PrependUint64(uint64(id + 1))
		builder.PrependUint64(uint64(id))
		scalarListFieldVector := builder.EndVector(2)
		books.BookStart(builder)
		books.BookAddId(builder, uint64(id))
		books.BookAddTitle(builder, title)
		books.BookAddPageCount(builder, uint64(id))
		books.BookAddScalarListField(builder, scalarListFieldVector)
		books.BookAddListField(builder, listFieldVector)
		book := books.BookEnd(builder)
		offsetMap[bucket] = append(offsetMap[bucket], book)
		snapshotMap[bucket].Keys = append(snapshotMap[bucket].Keys, int(id))
	}
	for i := range numBuckets {
		builder := builderMap[i]
		childrenOffsets := offsetMap[i]
		books.BookListStartChildrenVector(builder, len(childrenOffsets))
		for j := len(childrenOffsets) - 1; j >= 0; j-- {
			builder.PrependUOffsetT(childrenOffsets[j])
		}
		childrenOffset := builder.EndVector(len(childrenOffsets))
		books.BookListStart(builder)
		books.BookListAddChildren(builder, childrenOffset)
		bookListOffset := books.BookListEnd(builder)
		builder.Finish(bookListOffset)
		snapshotMap[i].Buffer = builder.FinishedBytes()
	}

	flatConf := &flatmap.FlatConfig[int, *books.BookT, *books.Book, *books.BookList]{
		UpdateSeconds: 15,
		NewV: func() *books.Book {
			return &books.Book{}
		},
		NewVList: func() *books.BookList {
			return &books.BookList{}
		},
		GetKeysFromV: func(b *books.Book) []int {
			id := int(b.Id())
			return []int{id % numBuckets, id}
		},
	}
	flatMap := flatmap.NewFlatNode(flatConf, 0)
	flatMap.InitializeWithGroupedShardBuffers(snapshotMap)
	time.Sleep(30 * time.Second) // Allow some time for the map to process the deltas
	return flatMap, nil
}

// Initialize a two-level standard map (bucket -> key structure)
func constructTwoLevelStandardMap(testSize int) map[int]map[int]*books.BookT {
	stdMap := make(map[int]map[int]*books.BookT, numBuckets)

	// Initialize buckets
	for i := range numBuckets {
		stdMap[i] = make(map[int]*books.BookT)
	}

	for i := range testSize {
		id := i + 1
		bucket := id % numBuckets

		book := &books.BookT{
			Id:              uint64(id),
			Title:           "Book Title " + fmt.Sprintf("%d", id),
			PageCount:       uint64(id),
			ScalarListField: []uint64{uint64(id), uint64(id + 1)},
			ListField:       []string{"Book SField " + fmt.Sprintf("%d", id), "Book SField " + fmt.Sprintf("%d", id+1)},
		}

		// Store in two-level structure
		stdMap[bucket][id] = book
	}

	return stdMap
}

// Single-Level FlatMap Benchmarks

// BenchmarkFlatMapSingleRead benchmarks a single read operation
func BenchmarkFlatMapSingleRead(b *testing.B) {
	book := &books.Book{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := (i % largeTestSize) + 1
		globalFlatMap.Get([]int{key}, book)
	}
}

// BenchmarkFlatMapGetBatch benchmarks retrieving all items in a batch
func BenchmarkFlatMapGetBatch(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bookList, ok := globalFlatMap.GetBatch([]int{})
		if !ok {
			b.Fatal("Failed to get batch")
		}

		// Process all items in the batch
		count := bookList.ChildrenLength()
		tmpBook := &books.Book{}

		for j := range count {
			bookList.Children(tmpBook, j)
			_ = tmpBook.Id() // Basic access
		}
	}
}

// Single-Level Standard Map Benchmarks

// BenchmarkStdMapSingleRead benchmarks a single read operation from standard map
func BenchmarkStdMapSingleRead(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := (i % largeTestSize) + 1
		_ = globalStdMap[key]
	}
}

// BenchmarkStdMapFullIteration benchmarks iterating through the entire standard map
func BenchmarkStdMapFullIteration(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, book := range globalStdMap {
			_ = book.Id // Basic access
		}
	}
}

// Single-Level sync.Map Benchmarks

// BenchmarkSyncMapSingleRead benchmarks a single read operation
func BenchmarkSyncMapSingleRead(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := (i % largeTestSize) + 1
		globalSyncMap.Load(key)

	}
}

// BenchmarkSyncMapFullIteration benchmarks iterating through the entire sync.Map
func BenchmarkSyncMapFullIteration(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count := 0
		globalSyncMap.Range(func(key, value any) bool {
			book := value.(*books.BookT)
			_ = book.Id // Basic access
			count++
			if count >= largeTestSize { // Safety check to prevent infinite loops
				return false
			}
			return true
		})
	}
}

// Two-Level FlatMap Benchmarks

// BenchmarkTwoLevelFlatMapSingleRead benchmarks a single read from a two-level flatmap
func BenchmarkTwoLevelFlatMapSingleRead(b *testing.B) {
	book := &books.Book{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := (i % largeTestSize) + 1
		bucket := key % numBuckets

		globalTwoLevelFlatMap.Get([]int{bucket, key}, book)
	}
}

// BenchmarkTwoLevelFlatMapBucketRead benchmarks reading an entire bucket from a two-level flatmap
func BenchmarkTwoLevelFlatMapBucketRead(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bucket := i % numBuckets
		bookList, ok := globalTwoLevelFlatMap.GetBatch([]int{bucket})
		if !ok {
			continue // Skip if bucket not found
		}

		// Process all items in the bucket
		count := bookList.ChildrenLength()
		tmpBook := &books.Book{}

		for j := 0; j < count; j++ {
			bookList.Children(tmpBook, j)
			_ = tmpBook.Id()
		}
	}
}

// Two-Level Standard Map Benchmarks

// BenchmarkTwoLevelStdMapSingleRead benchmarks a single read from a two-level std map
func BenchmarkTwoLevelStdMapSingleRead(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := (i % largeTestSize) + 1
		bucket := key % numBuckets

		_ = globalTwoLevelStdMap[bucket][key]
	}
}

// BenchmarkTwoLevelStdMapBucketRead benchmarks reading an entire bucket from a two-level std map
func BenchmarkTwoLevelStdMapBucketRead(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bucket := i % numBuckets
		bucketMap, exists := globalTwoLevelStdMap[bucket]
		if !exists {
			continue
		}

		// Process all items in the bucket
		for _, book := range bucketMap {
			_ = book.Id
		}
	}
}

// Parallel Benchmarks

// BenchmarkTwoLevelFlatMapParallelBucketRead benchmarks parallel bucket reads
func BenchmarkTwoLevelFlatMapParallelBucketRead(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		localBucket := 0
		book := &books.Book{}

		for pb.Next() {
			// Each goroutine reads a different bucket in round-robin fashion
			localBucket = (localBucket + 1) % numBuckets

			bookList, ok := globalTwoLevelFlatMap.GetBatch([]int{localBucket})
			if !ok {
				continue
			}

			count := bookList.ChildrenLength()
			if count > 10 { // Only read up to 10 items to avoid long iterations
				count = 10
			}

			for j := 0; j < count; j++ {
				bookList.Children(book, j)
				_ = book.Id()
			}
		}
	})
}

// BenchmarkTwoLevelStdMapParallelBucketRead benchmarks parallel bucket reads on std map
func BenchmarkTwoLevelStdMapParallelBucketRead(b *testing.B) {

	b.RunParallel(func(pb *testing.PB) {
		localBucket := 0

		for pb.Next() {
			// Each goroutine reads a different bucket in round-robin fashion
			localBucket = (localBucket + 1) % numBuckets

			bucketMap, exists := globalTwoLevelStdMap[localBucket]
			if !exists {
				continue
			}

			// Copy a few keys to process outside the lock
			keys := make([]int, 0, 10)
			for k := range bucketMap {
				keys = append(keys, k)
				if len(keys) >= 10 { // Limit to 10 keys
					break
				}
			}

			// Process the copied keys
			for _, k := range keys {
				book := bucketMap[k]

				if book != nil {
					_ = book.Id
				}
			}
		}
	})
}

// BenchmarkSyncMapParallelSingleRead benchmarks parallel single-item reads from sync.Map
func BenchmarkSyncMapParallelSingleRead(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		localCounter := 0

		for pb.Next() {
			// Each goroutine reads different keys in round-robin fashion
			localCounter++
			key := (localCounter % largeTestSize) + 1

			val, _ := globalSyncMap.Load(key)
			if val != nil {
				book := val.(*books.BookT)
				_ = book.Id
			}
		}
	})
}

// Memory Profile Benchmark

// BenchmarkHeapProfile generates a heap profile for analyzing memory usage
func BenchmarkHeapProfile(b *testing.B) {
	if b.N > 1 {
		return // Only run once
	}

	// Create a temporary file for the profile
	f, err := os.Create("flatmap_heap.prof")
	if err != nil {
		b.Fatal("could not create memory profile:", err)
	}
	defer f.Close()

	// Reset memory state
	runtime.GC()

	// Generate some activity to profile
	book := &books.Book{}
	for i := range 1000 {
		key := (i % largeTestSize) + 1
		globalFlatMap.Get([]int{key}, book)
	}

	// Write the heap profile
	if err := pprof.WriteHeapProfile(f); err != nil {
		b.Fatal("could not write memory profile:", err)
	}

	b.Log("Heap profile written to flatmap_heap.prof")
}
