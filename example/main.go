package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/nidyaonur/flatmap/example/books"
	"github.com/nidyaonur/flatmap/pkg/flatmap"
)

var (
	testSize      = flag.Int("size", 1000000, "Number of items to test")
	runConstruct  = flag.Bool("construct", true, "Run construction benchmark")
	runRead       = flag.Bool("read", true, "Run read benchmark")
	runWrite      = flag.Bool("write", false, "Run write benchmark")
	numGoroutines = flag.Int("goroutines", 10, "Number of goroutines for concurrent tests")
	memProfile    = flag.String("memprofile", "", "Write memory profile to file")
	syncMapBench  = flag.Bool("syncmap", false, "Run sync.Map benchmark")
)

func main() {
	flag.Parse()

	fmt.Printf("Running with test size: %d\n", *testSize)

	if *syncMapBench {
		fmt.Println("Running sync.Map benchmark")
		start := time.Now()
		syncMap := constructSyncMap(*testSize)
		fmt.Printf("sync.Map construction time: %v\n", time.Since(start))

		if *runRead {
			start = time.Now()
			readSyncMap(syncMap, *testSize)
			fmt.Printf("sync.Map read time: %v\n", time.Since(start))
		}

		if *runWrite {
			start = time.Now()
			writeSyncMap(syncMap, *testSize)
			fmt.Printf("sync.Map write time: %v\n", time.Since(start))
		}
		fmt.Println("sync.Map benchmark completed")
	}

	if *runConstruct {
		start := time.Now()
		flatMap, err := constructFlatBookMap(*testSize)
		if err != nil {
			fmt.Println("Failed to construct flat map:", err)
			return
		}
		fmt.Printf("Construction time: %v\n", time.Since(start))

		if *runRead {
			start = time.Now()
			readFlatBookMap(flatMap, *testSize)
			fmt.Printf("Read time: %v\n", time.Since(start))
		}

		if *runWrite {
			start = time.Now()
			writeFlatBookMap(flatMap, *testSize)
			fmt.Printf("Write time: %v\n", time.Since(start))
		}
	}

	fmt.Println("Done")

	// Write memory profile if requested
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			fmt.Printf("Could not create memory profile: %v\n", err)
			return
		}
		defer f.Close()

		fmt.Println("Writing memory profile to", *memProfile)
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Printf("Could not write memory profile: %v\n", err)
		}
	}
}

func prepareDeltaItems(testSize int) []flatmap.DeltaItem[int] {
	deltaItems := make([]flatmap.DeltaItem[int], testSize)
	builder := flatbuffers.NewBuilder(1024)
	for i := 0; i < testSize; i++ {
		id := uint64(i + 1)
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
		deltaItems[i] = flatmap.DeltaItem[int]{
			Keys: []int{int(id)},
			Data: builder.FinishedBytes(),
		}
		builder.Reset() // Reset the builder for the next item
	}

	return deltaItems
}

func prepareSnapshot(testSize int) *flatmap.ShardSnapshot[int] {

	builder := flatbuffers.NewBuilder(1024 * testSize)
	keys := make([]int, testSize)
	childrenOffsets := make([]flatbuffers.UOffsetT, testSize)
	for i := 0; i < testSize; i++ {
		id := uint64(i + 1)

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
		childrenOffsets[i] = books.BookEnd(builder)
		keys[i] = int(id)
	}
	books.BookListStartChildrenVector(builder, testSize)
	for i := testSize - 1; i >= 0; i-- {
		builder.PrependUOffsetT(childrenOffsets[i])
	}
	childrenOffset := builder.EndVector(testSize)
	books.BookListStart(builder)
	books.BookListAddChildren(builder, childrenOffset)
	bookListOffset := books.BookListEnd(builder)
	builder.Finish(bookListOffset)

	return &flatmap.ShardSnapshot[int]{
		Keys:   keys,
		Path:   []int{},
		Buffer: builder.FinishedBytes(),
	}
}

func constructFlatBookMap(testSize int) (*flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList], error) {
	deltaItems := prepareDeltaItems(testSize)

	flatMap := flatmap.NewFlatNode(&flatmap.FlatConfig[int, *books.BookT, *books.Book, *books.BookList]{
		UpdateSeconds: 15,
		NewV: func() *books.Book {
			return &books.Book{}
		},
		NewVList: func() *books.BookList {
			return &books.BookList{}
		},
		GetKeysFromV: func(b *books.Book) []int {
			id := int(b.Id())
			return []int{id}
		},
	}, 0)
	flatMap.FeedDeltaBulk(deltaItems)
	deltaItems = nil
	time.Sleep(30 * time.Second) // Allow some time for the map to process the deltas
	return flatMap, nil
}
func constructFlatBookMapWithSnapshot(testSize int) (*flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList], error) {
	snapshot := prepareSnapshot(testSize)

	flatConf := &flatmap.FlatConfig[int, *books.BookT, *books.Book, *books.BookList]{
		UpdateSeconds: 5,
		NewV: func() *books.Book {
			return &books.Book{}
		},
		NewVList: func() *books.BookList {
			return &books.BookList{}
		},
		GetKeysFromV: func(b *books.Book) []int {
			id := int(b.Id())
			return []int{id}
		},
		SnapShotMode: flatmap.SnapshotModeConsumer,
	}

	flatMap := flatmap.NewFlatNode(flatConf, 0)
	flatMap.InitializeWithGroupedShardBuffers([]*flatmap.ShardSnapshot[int]{snapshot})
	time.Sleep(30 * time.Second) // Allow some time for the map to process the deltas
	return flatMap, nil
}

func readFlatBookMap(flatMap *flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList], testSize int) {
	wg := &sync.WaitGroup{}
	for i := 0; i < *numGoroutines; i++ {
		wg.Add(1)
		go func() {
			book := &books.Book{}
			for j := 1; j <= testSize; j++ {
				// check all fields
				ok := flatMap.Get([]int{j}, book)
				if !ok {
					fmt.Println("Failed to get book with key:", j)
				}
				id := book.Id()
				if id != uint64(j) {
					fmt.Println("Failed to get id:", j, id)
				}
				// title := book.Title()
				// if string(title) != fmt.Sprintf("Book Title %d", j) {
				// 	fmt.Println("Failed to get title:", j, string(title), fmt.Sprintf("Book Title %d", j))
				// }
				// pageCount := book.PageCount()
				// if pageCount != uint64(j) {
				// 	fmt.Println("Failed to get page count:", j, pageCount, uint64(j))
				// }
				// scalarListLen := book.ScalarListFieldLength()
				// if scalarListLen != 2 {
				// 	fmt.Println("Failed to get scalar list length:", j, scalarListLen)
				// }
				// for k := 0; k < scalarListLen; k++ {
				// 	scalar := book.ScalarListField(k)
				// 	if scalar != uint64(j+k) {
				// 		fmt.Println("Failed to get scalar list field:", j, scalar, uint64(j+k))
				// 	}
				// }
				// listFieldLen := book.ListFieldLength()
				// if listFieldLen != 2 {
				// 	fmt.Println("Failed to get list field length:", j, listFieldLen)
				// }
				// for k := 0; k < listFieldLen; k++ {
				// 	listField := book.ListField(k)
				// 	if string(listField) != fmt.Sprintf("Book SField %d", j+k) {
				// 		fmt.Println("Failed to get list field:",
				// 			j, string(listField), fmt.Sprintf("Book SField %d", j+k))
				// 	}
				// }
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func writeFlatBookMap(flatMap *flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList], testSize int) {
	wg := &sync.WaitGroup{}

	for i := 0; i < *numGoroutines; i++ {
		wg.Add(1)
		go func() {
			for j := 1; j <= testSize; j++ {
				builder := flatbuffers.NewBuilder(1024)
				title := builder.CreateString("Book Title")
				listField1 := builder.CreateString("Book SField")
				listField2 := builder.CreateString("Book SField")
				_ = books.BookStartListFieldVector(builder, 2)
				builder.PrependUOffsetT(listField2)
				builder.PrependUOffsetT(listField1)
				listFieldVector := builder.EndVector(2)
				_ = books.BookStartScalarListFieldVector(builder, 2)
				builder.PrependUint64(uint64(j + 1))
				builder.PrependUint64(uint64(j))
				scalarListFieldVector := builder.EndVector(2)
				books.BookStart(builder)
				books.BookAddId(builder, uint64(j))
				books.BookAddTitle(builder, title)
				books.BookAddPageCount(builder, uint64(j))
				books.BookAddScalarListField(builder, scalarListFieldVector)
				books.BookAddListField(builder, listFieldVector)
				book := books.BookEnd(builder)
				builder.Finish(book)
				deltaItem := flatmap.DeltaItem[int]{
					Keys: []int{j},
					Data: builder.FinishedBytes(),
				}
				err := flatMap.Set(deltaItem)
				if err != nil {
					fmt.Println("Failed to set book with key:", j)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func constructStandardMap(testSize int) map[int]*books.BookT {
	stdMap := make(map[int]*books.BookT, testSize)

	for i := 0; i < testSize; i++ {
		id := i + 1
		book := &books.BookT{
			Id:              uint64(id),
			Title:           "Book Title " + strconv.Itoa(id),
			PageCount:       uint64(id),
			ScalarListField: []uint64{uint64(id), uint64(id + 1)},
			ListField:       []string{"Book SField " + strconv.Itoa(id), "Book SField " + strconv.Itoa(id+1)},
		}
		stdMap[id] = book
	}

	return stdMap
}

func readStandardMap(stdMap map[int]*books.BookT, testSize int) {
	wg := &sync.WaitGroup{}
	var mapMux sync.RWMutex

	for i := 0; i < *numGoroutines; i++ {
		wg.Add(1)
		go func() {
			for j := 1; j <= testSize; j++ {
				mapMux.RLock()
				book := stdMap[j]
				mapMux.RUnlock()

				if book.Id != uint64(j) {
					fmt.Println("Failed to get id:", j, book.Id)
				}
				if book.Title != fmt.Sprintf("Book Title %d", j) {
					fmt.Println("Failed to get title:", j, book.Title)
				}
				if book.PageCount != uint64(j) {
					fmt.Println("Failed to get page count:", j, book.PageCount)
				}
				if len(book.ScalarListField) != 2 {
					fmt.Println("Failed to get scalar list length:", j)
				}
				for k := 0; k < len(book.ScalarListField); k++ {
					scalar := book.ScalarListField[k]
					if scalar != uint64(j+k) {
						fmt.Println("Failed to get scalar list field:", j, scalar)
					}
				}
				if len(book.ListField) != 2 {
					fmt.Println("Failed to get list field length:", j)
				}
				for k := 0; k < len(book.ListField); k++ {
					listField := book.ListField[k]
					if listField != fmt.Sprintf("Book SField %d", j+k) {
						fmt.Println("Failed to get list field:", j, listField)
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// Add this new function after the other read functions

func readFlatBookMapBatch(flatMap *flatmap.FlatNode[int, *books.BookT, *books.Book, *books.BookList], bucketCount int) {
	wg := &sync.WaitGroup{}
	for i := 0; i < *numGoroutines; i++ {
		wg.Add(1)
		go func(routineNum int) {
			book := &books.Book{}

			// Distribute buckets among goroutines
			bucketsPerRoutine := bucketCount / *numGoroutines
			startBucket := routineNum * bucketsPerRoutine
			endBucket := startBucket + bucketsPerRoutine

			for bucket := startBucket; bucket < endBucket; bucket++ {
				bookList, found := flatMap.GetBatch([]int{bucket})
				if !found {
					continue
				}

				count := bookList.ChildrenLength()
				for j := 0; j < count; j++ {
					bookList.Children(book, j)

					// Basic validation
					id := book.Id()
					title := book.Title()

					// Full validation if needed
					if id <= 0 {
						fmt.Printf("Invalid book ID: %d\n", id)
					}
					if len(title) == 0 {
						fmt.Println("Empty book title")
					}
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// Process all books in standard map for comparison
func readStandardMapFull(stdMap map[int]*books.BookT) {
	wg := &sync.WaitGroup{}
	var mapMux sync.RWMutex

	for i := 0; i < *numGoroutines; i++ {
		wg.Add(1)
		go func(routineNum int) {
			keysPerRoutine := len(stdMap) / *numGoroutines
			startKey := routineNum*keysPerRoutine + 1
			endKey := startKey + keysPerRoutine

			mapMux.RLock()
			for j := startKey; j <= endKey; j++ {
				if book, exists := stdMap[j]; exists {
					// Basic validation
					_ = book.Id
					_ = book.Title
				}
			}
			mapMux.RUnlock()

			wg.Done()
		}(i)
	}
	wg.Wait()
}

// Function to create a sync.Map populated with BookT objects
func constructSyncMap(testSize int) *sync.Map {
	var syncMap sync.Map

	for i := 0; i < testSize; i++ {
		id := i + 1
		book := &books.BookT{
			Id:              uint64(id),
			Title:           "Book Title",
			PageCount:       uint64(id),
			ScalarListField: []uint64{uint64(id), uint64(id + 1)},
			ListField:       []string{"Book SField", "Book SField"},
		}
		syncMap.Store(id, book)
	}

	return &syncMap
}

// Function to read from sync.Map concurrently
func readSyncMap(syncMap *sync.Map, testSize int) {
	wg := &sync.WaitGroup{}

	for i := 0; i < *numGoroutines; i++ {
		wg.Add(1)
		go func() {
			for j := 1; j <= testSize; j++ {
				book, ok := syncMap.Load(j)
				if !ok {
					fmt.Println("Failed to get book with key:", j)
					continue
				}

				bookT := book.(*books.BookT)
				if bookT.Id != uint64(j) {
					fmt.Println("Failed to get id:", j, bookT.Id)
				}
				// Minimal validation to match other benchmark functions
				// Commented out the full validation for consistency with other benchmarks
				// if bookT.Title != fmt.Sprintf("Book Title") {
				//   fmt.Println("Failed to get title:", j, bookT.Title)
				// }
				// if bookT.PageCount != uint64(j) {
				//   fmt.Println("Failed to get page count:", j, bookT.PageCount)
				// }
				// if len(bookT.ScalarListField) != 2 {
				//   fmt.Println("Failed to get scalar list length:", j)
				// }
				// for k := 0; k < len(bookT.ScalarListField); k++ {
				//   scalar := bookT.ScalarListField[k]
				//   if scalar != uint64(j+k) {
				//     fmt.Println("Failed to get scalar list field:", j, scalar)
				//   }
				// }
				// if len(bookT.ListField) != 2 {
				//   fmt.Println("Failed to get list field length:", j)
				// }
				// for k := 0; k < len(bookT.ListField); k++ {
				//   listField := bookT.ListField[k]
				//   if listField != "Book SField" {
				//     fmt.Println("Failed to get list field:", j, listField)
				//   }
				// }
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// Function to write to sync.Map concurrently
func writeSyncMap(syncMap *sync.Map, testSize int) {
	wg := &sync.WaitGroup{}

	for i := 0; i < *numGoroutines; i++ {
		wg.Add(1)
		go func() {
			for j := 1; j <= testSize; j++ {
				book := &books.BookT{
					Id:              uint64(j),
					Title:           "Book Title",
					PageCount:       uint64(j),
					ScalarListField: []uint64{uint64(j), uint64(j + 1)},
					ListField:       []string{"Book SField", "Book SField"},
				}
				syncMap.Store(j, book)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// Batch read operation for sync.Map
func readSyncMapBatch(syncMap *sync.Map, testSize int) {
	wg := &sync.WaitGroup{}
	bucketSize := 100 // Arbitrary bucket size for batch processing
	bucketCount := testSize/bucketSize + 1

	for i := 0; i < *numGoroutines; i++ {
		wg.Add(1)
		go func(routineNum int) {
			// Distribute buckets among goroutines
			bucketsPerRoutine := bucketCount / *numGoroutines
			startBucket := routineNum * bucketsPerRoutine
			endBucket := startBucket + bucketsPerRoutine

			for bucket := startBucket; bucket < endBucket; bucket++ {
				startKey := bucket*bucketSize + 1
				endKey := (bucket + 1) * bucketSize

				// Ensure we don't go beyond the testSize
				if endKey > testSize {
					endKey = testSize
				}

				for j := startKey; j <= endKey; j++ {
					bookVal, ok := syncMap.Load(j)
					if !ok {
						continue
					}

					book := bookVal.(*books.BookT)

					// Basic validation
					id := book.Id
					title := book.Title

					// Full validation if needed
					if id <= 0 {
						fmt.Printf("Invalid book ID: %d\n", id)
					}
					if len(title) == 0 {
						fmt.Println("Empty book title")
					}
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}
