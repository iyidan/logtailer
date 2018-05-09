package logtailer

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	TestDir = "./logtailer-test-dir"
)

func init() {
	Debug = true
}

func baseTestRule(t *testing.T, rule *WatchRule, beforeStart, afterStart, afterClosed func(), isWait bool) {
	defer func() {
		if err := os.RemoveAll(TestDir); err != nil {
			panic(err)
		}
	}()
	if err := os.Mkdir(TestDir, 0755); err != nil {
		t.Fatal(err)
	}

	beforeStart()

	if err := rule.Process(); err != nil {
		t.Fatal(err)
	}

	afterStart()

	if isWait {
		rule.Wait()
	} else {
		time.Sleep(time.Millisecond * 500)
		rule.Close()
	}

	afterClosed()
}

func commonTestRuleWait(t *testing.T, rule *WatchRule, beforeStart, afterStart, afterClosed func()) {
	baseTestRule(t, rule, beforeStart, afterStart, afterClosed, true)
}

func commonTestRule(t *testing.T, rule *WatchRule, beforeStart, afterStart, afterClosed func()) {
	baseTestRule(t, rule, beforeStart, afterStart, afterClosed, false)
}

func TestTailFileDefault(t *testing.T) {
	filename := filepath.Join(TestDir, "TestTailFileDefault.log")
	rule := &WatchRule{
		Path:  filename,
		Match: "test",
	}

	beforeStart := func() {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := file.WriteString("aaa\nbbb\ntest\naaa\nbbb\ntest\n\n\taaa test\n\n\t"); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
	}

	afterStart := func() {

	}

	afterClosed := func() {
		if rule.matchedIdx != 3 {
			t.Fatal()
		}
	}

	commonTestRule(t, rule, beforeStart, afterStart, afterClosed)
}

func TestTailFileWait(t *testing.T) {
	filename := filepath.Join(TestDir, "TestTailFileWait.log")
	rule := &WatchRule{
		Path:  filename,
		Match: "test",
	}

	beforeStart := func() {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := file.WriteString("aaa\nbbb\ntest\naaa\nbbb\ntest\n\n\taaa test\n\n\t"); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
	}

	afterStart := func() {

	}

	afterClosed := func() {
		if rule.matchedIdx != 3 {
			t.Fatal()
		}
	}

	commonTestRuleWait(t, rule, beforeStart, afterStart, afterClosed)
}

func TestTailFilePoll(t *testing.T) {
	filename := filepath.Join(TestDir, "TestTailFilePoll.log")
	rule := &WatchRule{
		Path:  filename,
		Match: "test",
		Poll:  true,
	}

	beforeStart := func() {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := file.WriteString("aaa\nbbb\ntest\naaa\nbbb\ntest\n\n\taaa test\n\n\t"); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
	}

	afterStart := func() {

	}

	afterClosed := func() {
		if rule.matchedIdx != 3 {
			t.Fatal()
		}
	}

	commonTestRule(t, rule, beforeStart, afterStart, afterClosed)
}

func TestTailFileSeekEnd(t *testing.T) {
	filename := filepath.Join(TestDir, "TestTailFileSeekEnd.log")
	rule := &WatchRule{
		Path:    filename,
		Match:   "test",
		SeekEnd: true,
		Follow:  true,
	}

	beforeStart := func() {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := file.WriteString("aaa\nbbb\ntest\naaa\nbbb\ntest\n\n\taaa test\n\n\t"); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
	}

	afterStart := func() {
		time.Sleep(time.Millisecond * 200)
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := file.WriteString("aaa\nbbb\ntest\naaa\nbbb\ntest\n\n\taaa test\n\n\t"); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
		time.Sleep(time.Millisecond * 200)
	}

	afterClosed := func() {
		if rule.matchedIdx != 3 {
			t.Fatal(rule.matchedIdx)
		}
	}

	commonTestRule(t, rule, beforeStart, afterStart, afterClosed)
}

func TestTailFileMultiWorker(t *testing.T) {
	filename := filepath.Join(TestDir, "TestTailFileMultiWorker.log")
	var matchedLines []string
	var matchedLinesLock sync.Mutex
	rule := &WatchRule{
		Path:             filename,
		Match:            "test",
		Follow:           true,
		HandlerWorkerNum: 20,
		Handler: func(rule *WatchRule, matchedIdx uint64, logTime time.Time, line string) {
			DefHandFunc(rule, matchedIdx, logTime, line)
			matchedLinesLock.Lock()
			matchedLines = append(matchedLines, line)
			matchedLinesLock.Unlock()
		},
	}

	beforeStart := func() {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := file.WriteString("test1\ntest2\ntest3\ntest4\ntest5\ntest6\n\n\ntest7\n\n\t"); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
	}

	afterStart := func() {
		time.Sleep(time.Millisecond * 200)
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := file.WriteString("test1\ntest2\ntest3\ntest4\ntest5\ntest6\n\n\ntest7\n\n\t"); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
		time.Sleep(time.Millisecond * 200)
	}

	afterClosed := func() {
		t.Logf("\n%s", strings.Join(matchedLines, "\n"))
		if len(matchedLines) != 14 {
			t.Fatal(len(matchedLines))
		}
	}

	commonTestRule(t, rule, beforeStart, afterStart, afterClosed)
}

func TestTailFileCustomLogTime(t *testing.T) {
	filename := filepath.Join(TestDir, "TestTailFile.log")
	var matchedLines []string
	var matchedLinesLock sync.Mutex
	rule := &WatchRule{
		Path:          filename,
		StartAtStr:    "1990-07-15 15:00:00",
		EndAtStr:      "1990-07-15 16:00:00",
		SeekEnd:       false,
		Match:         "test",
		WatchDirMs:    0,
		LogTimeLayout: DefLogTimeLayout,
		TimeLayout:    DefTimeLayout,
		TimeLocation:  DefTimeLocation,
		Follow:        true,
		Handler: func(rule *WatchRule, matchedIdx uint64, logTime time.Time, line string) {
			matchedLinesLock.Lock()
			matchedLines = append(matchedLines, line)
			matchedLinesLock.Unlock()
		},
	}

	beforeStart := func() {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}

		content := `
1990/07/15 14:00:00 test aaa
testbbb
1990/07/15 15:00:00 test ccc
1990/07/15 15:00:01 test ddd
1990/07/15xx15:00:01 test dddd
xxxxx

1990/07/15 15:30:00.1 test eee
1990/07/15 16:00:00.0 test fff
1990/07/15 16:00:01.123456 test fff

`
		if _, err := file.WriteString(content); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
	}

	afterStart := func() {
		// append new
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteString("\n1990/07/15 16:00:00.1 test ggg\n"); err != nil {
			t.Fatal(err)
		}
		file.Sync()
		file.Close()
	}

	afterClosed := func() {
		t.Logf("\n%s", strings.Join(matchedLines, "\n"))
		if len(matchedLines) != 5 {
			t.Fatal()
		}
	}

	commonTestRule(t, rule, beforeStart, afterStart, afterClosed)
}

func TestTailDir(t *testing.T) {
	var matchedLines []string
	var matchedLinesLock sync.Mutex
	rule := &WatchRule{
		Path:       TestDir,
		SeekEnd:    false,
		Match:      "test",
		WatchDirMs: 5,
		Follow:     true,
		Handler: func(rule *WatchRule, matchedIdx uint64, logTime time.Time, line string) {
			matchedLinesLock.Lock()
			matchedLines = append(matchedLines, line)
			matchedLinesLock.Unlock()
		},
	}

	filename1 := filepath.Join(TestDir, "dirfile1.log")
	filename2 := filepath.Join(TestDir, "dirfile2.log")

	beforeStart := func() {
		file, err := os.OpenFile(filename1, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}

		content := "AAAA\ntest\nbbbbtest\ncccc"
		if _, err := file.WriteString(content); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
	}

	afterStart := func() {
		// append new
		file, err := os.OpenFile(filename1, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteString("\naaaa test ggg\n"); err != nil {
			t.Fatal(err)
		}
		file.Sync()
		file.Close()
		time.Sleep(time.Millisecond * 200)

		// removed -> recreated
		os.Remove(filename1)
		time.Sleep(time.Millisecond * 10)
		file, err = os.OpenFile(filename1, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteString("\naaaa test ggg\n"); err != nil {
			t.Fatal(err)
		}
		file.Sync()
		file.Close()
		time.Sleep(time.Millisecond * 200)

		// removed
		os.Remove(filename1)
		time.Sleep(time.Millisecond * 10)

		// create new 2
		file, err = os.OpenFile(filename2, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}

		content := "BBBB\ntest\nbbbbtest\ncccc"
		if _, err := file.WriteString(content); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
		time.Sleep(time.Millisecond * 200)
	}

	afterClosed := func() {
		t.Logf("\n%s", strings.Join(matchedLines, "\n"))
		if len(matchedLines) != 6 {
			t.Fatal()
		}
	}

	commonTestRule(t, rule, beforeStart, afterStart, afterClosed)
}

func TestTailFileEmptyMatch(t *testing.T) {
	filename := filepath.Join(TestDir, "TestTailFileEmptyMatch.log")
	rule := &WatchRule{
		Path:  filename,
		Match: "",
	}

	beforeStart := func() {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := file.WriteString("aaa\nbbb\nccc\n\t\n\n"); err != nil {
			t.Fatal()
		}
		file.Sync()
		file.Close()
	}

	afterStart := func() {

	}

	afterClosed := func() {
		if rule.matchedIdx != 5 {
			t.Fatal()
		}
	}

	commonTestRule(t, rule, beforeStart, afterStart, afterClosed)
}
