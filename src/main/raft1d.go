package main

import (
	"fmt"
	"os"

	"6.5840/raft1"
	"6.5840/tester1"
)

func main() {
	if err := tester.InitDaemon(os.Args[1:], raft.NewRfsrv); err != nil {
		fmt.Printf("%v: InitDaemon err %v", os.Args[0], err)
		os.Exit(1)
	}
}
