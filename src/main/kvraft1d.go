package main

import (
	"fmt"
	"os"

	"6.5840/kvraft1"
	"6.5840/tester1"
)

func main() {
	if err := tester.InitDaemon(os.Args[1:], kvraft.NewServer); err != nil {
		fmt.Printf("%v: InitDaemon err %v", os.Args[0], err)
		os.Exit(1)
	}
}
