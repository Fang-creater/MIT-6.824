package mr

import (
	//"log"
	"testing"
	//"time"
)

func TestWc(t *testing.T) {
	mkOut()
	t.Cleanup(func() {
		if !t.Failed() {
			cleanup()
		}
	})
	app := "../../mrapps/wc.so"
	files := findFilesPre("../main", "pg*txt", "..")
	mkCorrectOutput(files, app, "mr-wc-correct.txt")
	runMR(files, app, 3)
	mergeOutput("mr-wc-all.txt")
	runCmp(t, "mr-wc-all.txt", "mr-wc-correct.txt",
		"incorrect combined reduce output: mr-wc-all.txt vs mr-wc-correct.txt")
}

func TestIndexer(t *testing.T) {
	mkOut()
	t.Cleanup(func() {
		if !t.Failed() {
			cleanup()
		}
	})
	app := "../../mrapps/indexer.so"
	files := findFilesPre("../main", "pg*txt", "..")
	mkCorrectOutput(files, app, "mr-indexer-correct.txt")
	runMR(files, app, 2)
	mergeOutput("mr-indexer-all.txt")
	runCmp(t, "mr-indexer-all.txt", "mr-indexer-correct.txt",
		"incorrect combined reduce output: mr-indexer-all.txt vs mr-indexer-correct.txt")
}

// Test mappers run in parallel
func TestMapParallel(t *testing.T) {
	mkOut()
	t.Cleanup(func() {
		if !t.Failed() {
			cleanup()
		}
	})
	app := "../../mrapps/mtiming.so"
	files := findFilesPre("../main", "pg*txt", "..")
	runMR(files, app, 2)
	files = findFiles(tmp, "mr-out*")
	if n := countPattern(files, "times-"); n != 2 {
		t.Fatalf("saw %d worker(s) instead of 2", n)
	}
	if n := countPattern(files, "parallel.*"); n == 0 {
		t.Fatalf("map workers did not run in parallel")
	}
}

// Test reducers run in parallel
func TestReduceParallel(t *testing.T) {
	mkOut()
	t.Cleanup(func() {
		if !t.Failed() {
			cleanup()
		}
	})
	app := "../../mrapps/rtiming.so"
	files := findFilesPre("../main", "pg*txt", "..")
	runMR(files, app, 2)
	files = findFiles(tmp, "mr-out*")
	if n := countPattern(files, "[a-z] 2"); n < 2 {
		t.Fatalf("too few parallel reduces")
	}
}

// Test counts number of map jobs
func TestJobCount(t *testing.T) {
	mkOut()
	t.Cleanup(func() {
		if !t.Failed() {
			cleanup()
		}
	})
	app := "../../mrapps/jobcount.so"
	files := findFilesPre("../main", "pg*txt", "..")
	runMR(files, app, 4)
	files = findFiles(tmp, "mr-out*")
	if n := countPattern(files, "a 8"); n != 1 {
		t.Fatalf("map jobs ran incorrect number of times")
	}
}

// Test whether any worker or coordinator exits before the
// task has completed (i.e., all output files have been finalized)
func TestEarlyExit(t *testing.T) {
	const N = 4
	mkOut()
	t.Cleanup(func() {
		if !t.Failed() {
			cleanup()
		}
	})
	app := "../../mrapps/early_exit.so"
	files := findFilesPre("../main", "pg*txt", "..")
	c := make(chan int)
	c0 := make(chan bool)
	go func(c chan int) {
		for i := 0; i < N+1; i++ {
			// wait for any of the coord or workers to exit.
			<-c
			if i == 0 {
				// a process has exited. this means that the output should be finalized
				// otherwise, either a worker or the coordinator exited early
				mergeOutput("mr-wc-initial.txt")
			}
		}
		c0 <- true
	}(c)
	runMRchan(files, app, N, c, coordinatorSock())
	<-c0
	// all workers and coordinator exited
	files = findFiles(tmp, "mr-out*")
	mergeOutput("mr-wc-final.txt")
	runCmp(t, "mr-wc-initial.txt", "mr-wc-final.txt",
		"reduce output changed: mr-wc-initial.txt vs mr-wc-final.txt")
}

func TestCrashWorker(t *testing.T) {
	const N = 3
	mkOut()
	t.Cleanup(func() {
		if !t.Failed() {
			cleanup()
		}
	})
	files := findFilesPre("../main", "pg*txt", "..")
	mkCorrectOutput(files, "../../mrapps/nocrash.so", "mr-crash-correct.txt")
	app := "../../mrapps/crash.so"
	c := make(chan int)
	sock := coordinatorSock()
	go func(c chan int) {
		for true {
			w := <-c
			if w == N { // is coordinator done?
				return
			}
			startWorker(app, w, c, sock)
		}
	}(c)
	runMRchan(files, app, N, c, sock)
	files = findFiles(tmp, "mr-out*")
	mergeOutput("mr-crash-all.txt")
	runCmp(t, "mr-crash-all.txt", "mr-crash-correct.txt",
		"incorrect combined reduce output: mr-crash-all.txt vs mr-crash-correct.txt")
}
