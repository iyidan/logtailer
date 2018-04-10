package logtailer

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hpcloud/tail"
	"github.com/iyidan/grpool"
)

var (
	// DefTimeLayout for parse WatchRule.StartAt/EndAt
	DefTimeLayout = "2006-01-02 15:04:05"
	// DefLogTimeLayout for parse log time
	DefLogTimeLayout = "2006/01/02 15:04:05"
	// DefTimeLocation which location used for time parse
	DefTimeLocation = time.Local

	// Debug show debug message
	Debug bool
	// DefDebugLogger print debug message
	DefDebugLogger = log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	// DefHandFunc the default handler is be used when rule.handler empty
	DefHandFunc = func(rule *WatchRule, matchedIdx uint64, logTime time.Time, line string) {
		fmt.Printf("rule(%s) matched one: %d, %s, %s\n", rule.Path, matchedIdx, logTime.Format(rule.TimeLayout), line)
	}
)

func debugf(format string, a ...interface{}) {
	if Debug {
		DefDebugLogger.Printf(format, a...)
	}
}

func panicf(format string, a ...interface{}) {
	DefDebugLogger.Panicf(format, a...)
}

// Handler handle and process matched line
type Handler func(rule *WatchRule, matchedIdx uint64, logTime time.Time, line string)

// WatchRule log watch rule
type WatchRule struct {
	Path          string         `json:"path"` // can be dir/file path
	StartAtStr    string         `json:"start_at"`
	EndAtStr      string         `json:"end_at"`
	SeekEnd       bool           `json:"seek_end"` // is seek log file end when start process
	Match         string         `json:"match"`    // regexp
	WatchDirMs    int            `json:"watch_dir_ms"`
	LogTimeLayout string         `json:"log_time_layout"`
	TimeLayout    string         `json:"time_layout"`
	TimeLocation  *time.Location `json:"-"`
	Poll          bool           `json:"poll"`   // use poll instead inotify
	Follow        bool           `json:"follow"` // equal to tail -f

	Handler           Handler `json:"-"`
	HandlerWorkerNum  int     `json:"handler_worker_num"`
	HandlerWorkerPool *grpool.Pool

	startAt          time.Time
	endAt            time.Time
	watchDirDuration time.Duration
	matchedIdx       uint64
	closeCh          chan struct{}
	closedCh         chan struct{}
	closed           int32
}

func (rule *WatchRule) check() (err error) {
	rule.Path = strings.Trim(rule.Path, " \t\n ")
	rule.Match = strings.Trim(rule.Match, "\t\n ")
	if len(rule.Path) == 0 || len(rule.Match) == 0 {
		err = fmt.Errorf("rule.check: rule.path/match empty: %#v", rule)
		return
	}
	if rule.TimeLayout == "" {
		rule.TimeLayout = DefTimeLayout
	}
	if rule.LogTimeLayout == "" {
		rule.LogTimeLayout = DefLogTimeLayout
	}
	if rule.TimeLocation == nil {
		rule.TimeLocation = DefTimeLocation
	}
	if len(rule.StartAtStr) > 0 {
		rule.startAt, err = time.ParseInLocation(rule.TimeLayout, rule.StartAtStr, rule.TimeLocation)
		if err != nil {
			err = fmt.Errorf("rule.check: parse rule.startAt error: %#v", rule)
			return
		}
	}
	if len(rule.EndAtStr) > 0 {
		rule.endAt, err = time.ParseInLocation(rule.TimeLayout, rule.EndAtStr, rule.TimeLocation)
		if err != nil {
			err = fmt.Errorf("rule.check: parse rule.endAt error: %#v", rule)
			return
		}
	} else {
		rule.endAt = time.Now().Add(time.Hour * 24 * 365 * 100)
	}

	if rule.WatchDirMs <= 0 {
		rule.WatchDirMs = 5000
	}
	rule.watchDirDuration = time.Millisecond * time.Duration(rule.WatchDirMs)

	if rule.Handler == nil {
		rule.Handler = DefHandFunc
	}
	return
}

// Close will block until closed
func (rule *WatchRule) Close() {
	if !atomic.CompareAndSwapInt32(&rule.closed, 0, 1) {
		return
	}
	if rule.closeCh != nil {
		close(rule.closeCh)
	}

	<-rule.closedCh

	if rule.HandlerWorkerPool != nil {
		rule.HandlerWorkerPool.Release()
		rule.HandlerWorkerPool = nil
	}
}

// Wait wait for tailer end folloy=false
func (rule *WatchRule) Wait() {
	<-rule.closedCh
	rule.Close()
}

// Process start tail the rule.Path specific log file
func (rule *WatchRule) Process() (err error) {
	if err = rule.check(); err != nil {
		return
	}

	stats, err := os.Stat(rule.Path)
	if err != nil {
		err = fmt.Errorf("rule.Process: os.Stat error: %v", err)
		return
	}
	if !stats.IsDir() && !stats.Mode().IsRegular() {
		err = fmt.Errorf("rule.Process: rule.path is not a regular file or dir: %s", rule.Path)
		return
	}

	rule.closeCh = make(chan struct{}, 1)
	rule.closedCh = make(chan struct{}, 1)

	var (
		isDir         = stats.IsDir()
		fileChangedCh chan fileChangedInfo
		lines         <-chan string
	)

	if isDir {
		fileChangedCh = watchDirFileChanged(rule, rule.closeCh)
	} else {
		fileChangedCh = make(chan fileChangedInfo, 1)
		fileChangedCh <- fileChangedInfo{filename: rule.Path, IsNew: true}
	}

	// tail file
	lines = tailFileChan(rule, fileChangedCh, rule.closeCh)
	go func() {
		defer close(rule.closedCh)
		var (
			re  = regexp.MustCompile(rule.Match)
			err error
		)
		for line := range lines {
			if re.MatchString(line) {
				logTime := time.Time{}
				if len(rule.StartAtStr) > 0 || len(rule.EndAtStr) > 0 {
					if len(line) < len(rule.LogTimeLayout) {
						debugf("(%s) line too short and skiped: %s", rule.Path, line)
						continue
					}
					logTime, err = time.ParseInLocation(rule.LogTimeLayout, line[0:len(rule.LogTimeLayout)], rule.TimeLocation)
					if err != nil {
						debugf("(%s) parse logTime fail and skiped: %s, err: %v", rule.Path, line, err)
						continue
					}
					if logTime.Before(rule.startAt) || logTime.After(rule.endAt) {
						continue
					}
				}
				rule.handlerPool(rule.matchedIdx, logTime, line)
				rule.matchedIdx++
			}
		}
	}()

	return
}

func (rule *WatchRule) handlerPool(matchedIdx uint64, logTime time.Time, line string) {
	if rule.HandlerWorkerNum <= 0 {
		rule.Handler(rule, matchedIdx, logTime, line)
		return
	}
	if rule.HandlerWorkerPool == nil {
		rule.HandlerWorkerPool = grpool.NewPool(rule.HandlerWorkerNum, 0)
	}
	rule.HandlerWorkerPool.JobQueue <- func() {
		rule.Handler(rule, matchedIdx, logTime, line)
	}
}

type fileChangedInfo struct {
	filename  string
	IsRemoved bool
	IsNew     bool
}

func tailFileChan(rule *WatchRule, fileChangedCh <-chan fileChangedInfo, stopCh <-chan struct{}) <-chan string {
	lines := make(chan string)
	go func() {
		tailerStopChMap := make(map[string][2]chan struct{})
		cleanFunc := func() {
			for filename, oldStopCh := range tailerStopChMap {
				debugf("(%s) try stop tailer: %s", rule.Path, filename)
				close(oldStopCh[0])
				<-oldStopCh[1]
				debugf("(%s) tailer stoped: %s", rule.Path, filename)
			}
			debugf("(%s) tailFileChan stoped", rule.Path)
			close(lines)
		}
		for {
			select {
			case fChangeInfo, ok := <-fileChangedCh:
				if !ok {
					cleanFunc()
					return
				}
				filename := fChangeInfo.filename
				debugf("(%s) received new filename from file changed channel: %+v", rule.Path, fChangeInfo)
				if oldStopCh, ok := tailerStopChMap[filename]; ok {
					debugf("(%s) new filename tailer exist, try stop it: %s", rule.Path, filename)
					close(oldStopCh[0])
					<-oldStopCh[1]
					delete(tailerStopChMap, filename)
					debugf("(%s) new filename tailer exist, old tailer stoped: %s", rule.Path, filename)
				}
				if fChangeInfo.IsNew {
					tailerStopCh := make(chan struct{})
					tailerStopedCh := tailFile(rule, filename, tailerStopCh, lines)
					tailerStopChMap[filename] = [2]chan struct{}{tailerStopCh, tailerStopedCh}
				}
			case <-stopCh:
				cleanFunc()
				return
			case <-time.After(time.Second * 1):
				// close stoped
				for filename, oldStopCh := range tailerStopChMap {
					select {
					case <-oldStopCh[1]:
						close(oldStopCh[0])
						delete(tailerStopChMap, filename)
						debugf("(%s) tailer auto stoped: clean tailerStopChMap: %s", rule.Path, filename)
					default:
					}
				}
				if !rule.Follow && len(tailerStopChMap) == 0 {
					debugf("(%s) not follow and all tailer auto stoped, return", rule.Path)
					cleanFunc()
					return
				}
			}
		}
	}()
	return lines
}

func tailFile(rule *WatchRule, filename string, stopCh <-chan struct{}, lines chan<- string) chan struct{} {
	stopedCh := make(chan struct{})
	go func() {
		tailConf := tail.Config{MustExist: true, Follow: rule.Follow, Poll: rule.Poll, Logger: tail.DiscardingLogger}
		if rule.SeekEnd {
			tailConf.Location = &tail.SeekInfo{Offset: 0, Whence: os.SEEK_END}
		}
		if tailConf.Follow {
			tailConf.ReOpen = true
		}
		if Debug {
			tailConf.Logger = DefDebugLogger
		}
		tailer, err := tail.TailFile(filename, tailConf)
		if err != nil && !os.IsNotExist(err) {
			close(stopedCh)
			panicf("(%s) tail file err: %v file: %s", rule.Path, err, tailer.Filename)
		} else if os.IsNotExist(err) {
			close(stopedCh)
			debugf("(%s) tail file err: is not exists, file: %s", rule.Path, tailer.Filename)
			return
		}
		debugf("(%s) tailer created: %s", rule.Path, filename)
		for {
			select {
			case line, ok := <-tailer.Lines:
				if !ok {
					tailer.Stop()
					tailer.Cleanup()
					close(stopedCh)
					debugf("(%s) tailer for-loop tailer.Lines channel closed, exit: %s, %v, %v", rule.Path, filename, tailer.Err(), line)
					return
				}
				lines <- line.Text
			case <-stopCh:
				tailer.Stop()
				tailer.Cleanup()
				close(stopedCh)
				debugf("(%s) tailer for-loop stopCh closed, exit: %s", rule.Path, filename)
				return
			}
		}
	}()
	return stopedCh
}

func watchDirFileChanged(rule *WatchRule, stoped <-chan struct{}) (changedCh chan fileChangedInfo) {
	changedCh = make(chan fileChangedInfo, 10000)
	go func() {
		ticker := time.NewTicker(rule.watchDirDuration)
		lastStats := make(map[string]uint64) // filename->inode
		tickerFunc := func() {
			debugf("(%s) watchDirFileChanged: lastStats: %+v", rule.Path, lastStats)
			fdDir, err := os.Open(rule.Path)
			if err != nil {
				panicf("(%s) os.Open err: %v", rule.Path, err)
			}
			fileInfos, err := fdDir.Readdir(0)
			fdDir.Close()
			if err != nil {
				panicf("(%s) os.Readdir err: %v", rule.Path, err)
			}
			nowStats := make(map[string]uint64)
			for i := range fileInfos {
				filename := filepath.Join(rule.Path, fileInfos[i].Name())
				debugf("(%s) watchDirFileChanged: check file: %s", rule.Path, filename)
				if fileInfos[i].IsDir() {
					debugf("(%s) watchDirFileChanged: check file is dir: %s", rule.Path, filename)
					continue
				}
				if !fileInfos[i].Mode().IsRegular() {
					debugf("(%s) watchDirFileChanged: check file is not regular: %s", rule.Path, filename)
					continue
				}
				sysStat, ok := fileInfos[i].Sys().(*syscall.Stat_t)
				if !ok || sysStat == nil {
					debugf("(%s) watchDirFileChanged: check file get sysStat fail: %s", rule.Path, filename)
					continue
				}
				nowStats[filename] = sysStat.Ino
			}

			for filename, ino := range lastStats {
				if nowIno, ok := nowStats[filename]; !ok {
					// file removed
					changedCh <- fileChangedInfo{filename: filename, IsRemoved: true}
				} else if ino != nowIno {
					// file recreated
					changedCh <- fileChangedInfo{filename: filename, IsNew: true}
				}
			}
			for filename := range nowStats {
				if _, ok := lastStats[filename]; !ok {
					// file created
					changedCh <- fileChangedInfo{filename: filename, IsNew: true}
				}
			}
			lastStats = nowStats
		}

		tickerFunc()
		for {
			select {
			case <-ticker.C:
				tickerFunc()
			case <-stoped:
				ticker.Stop()
				close(changedCh)
				return
			}
		}
	}()
	return
}
