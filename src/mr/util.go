package mr

import (
	crand "crypto/rand"
	"encoding/base64"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

var tmp string

func startWorker(app string, i int, c chan int, sock string) {
	worker := exec.Command("../../main/mrworker", append([]string{app}, sock)...)
	worker.Stderr = os.Stderr
	worker.Stdout = os.Stdout
	worker.Dir = tmp
	if err := worker.Start(); err != nil {
		log.Fatalf("mr failed %v", err)
	}
	go func(cmd *exec.Cmd, i int) {
		cmd.Wait()
		if c != nil {
			c <- i
		}
	}(worker, i)
}

// Run MapReduce: start a coordinator and several workers,
// and wait for the coordinator being done
func runMRchan(files []string, app string, n int, c chan int, sock string) {
	coord := exec.Command("../main/mrcoordinator", append([]string{sock}, files...)...)
	coord.Stderr = os.Stderr
	coord.Stdout = os.Stdout
	if err := coord.Start(); err != nil {
		log.Fatalf("mr failed %v", err)
	}

	// give the coordinator time to create the sockets.
	time.Sleep(1 * time.Second)

	for i := 0; i < n; i++ {
		startWorker(app, i, c, sock)
	}
	if err := coord.Wait(); err != nil {
		log.Fatalf("Wait %v", err)
	}
	if c != nil {
		c <- n
	}
	os.Remove(sock)
}

func runMR(files []string, app string, n int) {
	sock := coordinatorSock()
	runMRchan(files, app, n, nil, sock)
}

func RandString(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	const N = 20
	s := "/tmp/5840-mr-"
	s += RandString(20)
	return s
}

// Generate correct output for a test
func mkCorrectOutput(files []string, app, out string) {
	args := append([]string{app}, files...)
	cmd := exec.Command("../../main/mrsequential", args...)
	cmd.Dir = tmp
	if err := cmd.Run(); err != nil {
		log.Fatalf("mrsequential %v failed err %v", args, err)
	}
	outputFile, err := os.Create(filepath.Join(tmp, out))
	if err != nil {
		log.Fatalf("create %v failed err %v", out, err)
	}
	defer outputFile.Close()
	cmd = exec.Command("sort", filepath.Join(tmp, "mr-out-0"))
	cmd.Stdout = outputFile
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("sort failed err %v", err)
	}
	if err := os.Remove(filepath.Join(tmp, "mr-out-0")); err != nil {
		log.Fatalf("Remove failed err %v", err)
	}
}

func mergeOutput(out string) {
	files := findFiles(tmp, `mr-out-[0-9]`)
	if len(files) < 1 {
		log.Fatalf("reduce created no mr-out-X output files!")
	}
	outputFile, err := os.Create(filepath.Join(tmp, out))
	if err != nil {
		log.Fatalf("create %v failed err %v", out, err)
	}
	defer outputFile.Close()
	cmd := exec.Command("sort", files...)
	cmd.Stdout = outputFile
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("sort failed err %v", err)
	}
}

func findFiles(dir, s string) []string {
	cmd := exec.Command("find", dir, "-type", "f", "-name", s)
	output, err := cmd.Output()
	if err != nil {
		log.Fatalf("find failed err %v\n", err)
		return nil
	}
	s1 := strings.TrimSpace(string(output))
	if s1 == "" {
		return []string{}
	}
	files := strings.Split(s1, "\n")
	return files
}

func findFilesPre(dir, s, pre string) []string {
	files := findFiles(dir, s)
	for i, f := range files {
		files[i] = filepath.Join("..", f)
	}
	return files
}

func mkOut() {
	tmp = "mr-tmp-" + RandString(8)
	os.Mkdir(tmp, 0755)
}

func cleanup() {
	files := findFiles(tmp, "mr-*")
	for _, f := range files {
		os.Remove(f)
	}
	os.Remove(tmp)
}

func runCmp(t *testing.T, f1, f2, msg string) {
	cmd := exec.Command("cmp", f1, f2)
	cmd.Dir = tmp
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf(msg)
	}
}

func countPatternFile(f, p string) int {
	data, err := os.ReadFile(f)
	if err != nil {
		log.Fatalf("Open failed %s err %v", f, err)
	}
	re, err := regexp.Compile(p)
	if err != nil {
		log.Fatalf("Compile %v failed err %v", p, err)
	}
	m := re.FindAllString(string(data), -1)
	return len(m)
}

func countPattern(files []string, p string) int {
	n := 0
	for _, f := range files {
		n += countPatternFile(f, p)
	}
	return n
}
