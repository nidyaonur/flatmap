package main

import (
	"github.com/nidyaonur/flatmap/pkg/parser"
)

func main() {
	parser.Parse("../../../retail-backend/docs/fbs", "../../../retail-backend/fbs_models/my_generated_config.go", "fbs_models")
	// parser.Parse("./", "./books/my_generated_config.go", "books")

}
