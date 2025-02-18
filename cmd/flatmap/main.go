package main

import (
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/nidyaonur/flatmap/cmd/parser/books"
	"github.com/nidyaonur/flatmap/pkg/flatmap"
)

func main() {
	testSize := 1000000
	flatMap, err := constructFlatBookMap(testSize)
	if err != nil {
		fmt.Println("Failed to construct flat map:", err)
		return
	}
	// writeFlatBookMap(flatMap, testSize)
	readFlatBookMap(flatMap, testSize)

	fmt.Println("Done")

}

func constructFlatBookMap(testSize int) (*flatmap.FlatNode[int, *books.Book, *books.BookList], error) {
	// builder := flatbuffers.NewBuilder(1024)
	// strOffsets := make(map[string]flatbuffers.UOffsetT)
	// keys := make([][]int, testSize)
	deltaItems := make([]flatmap.DeltaItem[int], testSize)
	// vectorOffsets := make([]map[string]flatbuffers.UOffsetT, testSize)

	for i := 0; i < testSize; i++ {
		id := uint64(i + 1)
		builder := flatbuffers.NewBuilder(1024)
		title := builder.CreateString(fmt.Sprintf("Book Title %d", id))
		listField1 := builder.CreateString(fmt.Sprintf("Book SField %d", id))
		listField2 := builder.CreateString(fmt.Sprintf("Book SField %d", id+1))
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
		books.BookAddAdType(builder, books.AdType(1))
		book := books.BookEnd(builder)
		builder.Finish(book)
		deltaItems[i] = flatmap.DeltaItem[int]{
			Keys: []int{int(id % 100), int(id)},
			Data: builder.FinishedBytes(),
		}

	}
	timeStart := time.Now()

	flatConf := flatmap.FlatConfig[int, *books.Book, *books.BookList]{
		UpdateSeconds: 15,
		NewV: func() *books.Book {
			return &books.Book{}
		},
		NewVList: func() *books.BookList {
			return &books.BookList{}
		},
		GetKeysFromV: func(b *books.Book) []int {
			id := int(b.Id())
			return []int{id % 100, id}
		},
		EnumByteGetter: books.EnumByteGetter[*books.Book],
	}
	flatMap := flatmap.NewFlatNode(flatConf, books.Tables, 0)
	flatMap.FeedDeltaBulk(deltaItems)
	fmt.Println("Construc time:", time.Since(timeStart))
	return flatMap, nil
}

func readFlatBookMap(flatMap *flatmap.FlatNode[int, *books.Book, *books.BookList], testSize int) {
	// timeStart := time.Now()
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			book := &books.Book{}
			for j := 1; j <= testSize; j++ {
				// check all fields
				ok := flatMap.Get([]int{j % 100, j}, book)
				if !ok {
					fmt.Println("Failed to get book with key:", j)
				}
				id := book.Id()
				if id != uint64(j) {
					fmt.Println("Failed to get id:", j, id)
				}
				title := book.Title()
				if string(title) != fmt.Sprintf("Book Title %d", j) {
					fmt.Println("Failed to get title:", j, string(title), fmt.Sprintf("Book Title %d", j))
				}
				pageCount := book.PageCount()
				if pageCount != uint64(j) {
					fmt.Println("Failed to get page count:", j, pageCount, uint64(j))
				}
				scalarListLen := book.ScalarListFieldLength()
				if scalarListLen != 2 {
					fmt.Println("Failed to get scalar list length:", j, scalarListLen)
				}
				for k := 0; k < scalarListLen; k++ {
					scalar := book.ScalarListField(k)
					if scalar != uint64(j+k) {
						fmt.Println("Failed to get scalar list field:", j, scalar, uint64(j+k))
					}
				}
				listFieldLen := book.ListFieldLength()
				if listFieldLen != 2 {
					fmt.Println("Failed to get list field length:", j, listFieldLen)
				}
				for k := 0; k < listFieldLen; k++ {
					listField := book.ListField(k)
					if string(listField) != fmt.Sprintf("Book SField %d", j+k) {
						fmt.Println("Failed to get list field:",
							j, string(listField), fmt.Sprintf("Book SField %d", j+k))
					}
				}
				adType := book.AdType()
				if adType != books.AdType(1) {
					fmt.Println("Failed to get ad type:", j, adType)
				} else {
					fmt.Println("Ad type:", adType)
				}
			}
			wg.Done()
		}()
	}
	// fmt.Println("Read time:", time.Since(timeStart))
	wg.Wait()
}

func writeFlatBookMap(flatMap *flatmap.FlatNode[int, *books.Book, *books.BookList], testSize int) {
	// timeStart := time.Now()
	wg := &sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for j := 1; j <= testSize; j++ {
				builder := flatbuffers.NewBuilder(1024)
				title := builder.CreateString(fmt.Sprintf("Book Title %d", j))
				listField1 := builder.CreateString(fmt.Sprintf("Book SField %d", j))
				listField2 := builder.CreateString(fmt.Sprintf("Book SField %d", j+1))
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
					Keys: []int{j % 100, j},
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
	// fmt.Println("Write time:", time.Since(timeStart))
	wg.Wait()
}
