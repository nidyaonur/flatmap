package main

import (
	"fmt"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/nidyaonur/flatmap/cmd/parser/books"
	"github.com/nidyaonur/flatmap/pkg/flatmap"
)

func main() {
	testSize := 10000000
	flatMap, err := constructFlatBookMap(testSize)
	if err != nil {
		fmt.Println("Failed to construct flat map:", err)
		return
	}
	readFlatBookMap(flatMap, testSize)
}

func constructFlatBookMap(testSize int) (*flatmap.FlatNode[int, *books.Book, *books.BookList], error) {
	builder := flatbuffers.NewBuilder(1024)
	strOffsets := make(map[string]flatbuffers.UOffsetT)
	keys := make([][]int, testSize)
	vectorOffsets := make([]map[string]flatbuffers.UOffsetT, testSize)

	for i := 0; i < testSize; i++ {
		title := fmt.Sprintf("Book Title %d", i+1)
		strOffsets[title] = builder.CreateString(title)
		listField1 := fmt.Sprintf("Book SField %d", i+1) // id
		strOffsets[listField1] = builder.CreateString(listField1)
		listField2 := fmt.Sprintf("Book SField %d", i+2) // id + 1
		strOffsets[listField2] = builder.CreateString(listField2)
		vectorOffsets[i] = make(map[string]flatbuffers.UOffsetT)

	}
	for i := 0; i < testSize; i++ {
		scalarList := make([]uint64, 2)
		scalarList[0] = uint64(i + 1) // id
		scalarList[1] = uint64(i + 2) // id + 1
		vectorOffsets[i] = make(map[string]flatbuffers.UOffsetT)
		vectorOffsets[i]["ScalarListField"] = books.BookStartScalarListFieldVector(builder, 2)
		for j := 1; j >= 0; j-- {
			builder.PrependUint64(scalarList[j])
		}
		vectorOffsets[i]["ScalarListField"] = builder.EndVector(2)
		strList := make([]flatbuffers.UOffsetT, 2)
		strList[0] = strOffsets[fmt.Sprintf("Book SField %d", i+1)]
		strList[1] = strOffsets[fmt.Sprintf("Book SField %d", i+2)]
		vectorOffsets[i]["ListField"] = books.BookStartListFieldVector(builder, 2)
		for j := 1; j >= 0; j-- {
			builder.PrependUOffsetT(strList[j])
		}
		vectorOffsets[i]["ListField"] = builder.EndVector(2)
	}

	offsetList := make([]flatbuffers.UOffsetT, testSize)
	for i := 0; i < testSize; i++ {
		id := uint64(i + 1)
		books.BookStart(builder)
		books.BookAddId(builder, id)
		books.BookAddTitle(builder, strOffsets[fmt.Sprintf("Book Title %d", id)])
		books.BookAddPageCount(builder, id)
		books.BookAddScalarListField(builder, vectorOffsets[i]["ScalarListField"])
		books.BookAddListField(builder, vectorOffsets[i]["ListField"])
		book := books.BookEnd(builder)
		offsetList[i] = book
		keys[i] = []int{int(id % 100), int(id)}
	}
	books.BookListStartChildrenVector(builder, testSize)
	for i := testSize - 1; i >= 0; i-- {
		builder.PrependUOffsetT(offsetList[i])
	}
	booksVector := builder.EndVector(testSize)
	books.BookListStart(builder)
	books.BookListAddChildren(builder, booksVector)
	bookList := books.BookListEnd(builder)
	builder.Finish(bookList)

	flatConf := flatmap.FlatConfig[int, *books.Book, *books.BookList]{
		InitialBuffer: builder.FinishedBytes(),
		InitialKeys:   keys,
		UpdateSeconds: 15,
		Callbacks: flatmap.CallbackConfig[*books.Book, *books.BookList]{
			NewV: func() *books.Book {
				return &books.Book{}
			},
			NewVList: func() *books.BookList {
				return &books.BookList{}
			},
		},
	}
	flatMap := flatmap.NewFlatNode(flatConf, books.Tables)
	return flatMap, nil
}

func readFlatBookMap(flatMap *flatmap.FlatNode[int, *books.Book, *books.BookList], testSize int) {
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			book := &books.Book{}
			for j := 1; j < testSize; j++ {
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
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
