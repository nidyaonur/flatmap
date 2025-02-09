package parser

import (
	"fmt"
	"os"
	"path/filepath"
)

func Parse(path, output, packageName string) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		fmt.Println("Failed to get file info:", err)
		return
	}

	var allTables []Table
	var enums map[string]bool = make(map[string]bool)
	if fileInfo.IsDir() {
		err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && filepath.Ext(filePath) == ".fbs" {
				tables, enumSet := processFile(filePath)
				allTables = append(allTables, tables...)
				for k, v := range enumSet {
					enums[k] = v
				}
			}
			return nil
		})
		if err != nil {
			fmt.Println("Failed to walk through directory:", err)
		}
	} else {
		if filepath.Ext(path) == ".fbs" {
			tables, enumSet := processFile(path)
			allTables = append(allTables, tables...)
			for k, v := range enumSet {
				enums[k] = v
			}
		}
	}

	goCode := GenerateGoFile(allTables, enums, packageName)
	os.WriteFile(output, []byte(goCode), 0644)
}

func processFile(path string) ([]Table, map[string]bool) {
	fbs, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Failed to read fbs file:", err)
		return nil, nil
	}
	exampleFBS := string(fbs)

	newParser := NewParser(exampleFBS)
	tables := newParser.Parse()
	if len(newParser.Errors) > 0 {
		fmt.Println("Errors during parse:")
		for _, e := range newParser.Errors {
			fmt.Println(" -", e)
		}
		return nil, nil
	}

	return tables, newParser.enumSet
}
