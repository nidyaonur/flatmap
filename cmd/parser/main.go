package main

import (
	"github.com/nidyaonur/flatmap/pkg/parser"
)

func main() {
	parser.Parse("./", "./books/my_generated_config.go", "books")

}
