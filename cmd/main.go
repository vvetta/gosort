package main

import (
	"fmt"
	"os"

	"github.com/vvetta/gosort/internal/flagParser"
	"github.com/vvetta/gosort/internal/goSort"
)


func main() {
	// Flags: -k, -n, -r, -u

	flagP := flagParser.NewFlagParser(os.Args[1:])
	sortFlags, err := flagP.Parse()
	
	if err != nil {
		fmt.Println(err)
		return	
	}

	fmt.Println("flags: ", sortFlags)

	sorter := goSort.NewSorter(sortFlags)
	err = sorter.Sort()
	if err != nil {
		fmt.Println(err)
	}
}
