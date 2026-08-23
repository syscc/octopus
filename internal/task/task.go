package task

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/internal/utils/safe"
)

type taskGate struct {
	running atomic.Bool
}

type taskEntry struct {
	name        string
	interval    atomic.Int64
	fn          func()
	runOnStart  bool
	stopCh      chan struct{}
	wakeCh      chan struct{}
	doneCh      chan struct{}
	stopOnce    sync.Once
	doneOnce    sync.Once
	loopStarted atomic.Bool
	inactive    atomic.Bool
	gate        *taskGate
}

var (
	tasks         = make(map[string]*taskEntry)
	stoppingTasks = make(map[string]*taskEntry)
	taskGates     = make(map[string]*taskGate)
	tasksMu       sync.Mutex
	runnerStarted bool
)

func newTaskEntry(name string, interval time.Duration, runOnStart bool, fn func()) *taskEntry {
	entry := &taskEntry{
		name:       name,
		fn:         fn,
		runOnStart: runOnStart,
		stopCh:     make(chan struct{}),
		wakeCh:     make(chan struct{}, 1),
		doneCh:     make(chan struct{}),
		gate:       taskGates[name],
	}
	entry.interval.Store(int64(interval))
	return entry
}

func taskGateLocked(name string) *taskGate {
	gate := taskGates[name]
	if gate == nil {
		gate = &taskGate{}
		taskGates[name] = gate
	}
	return gate
}

// Register 注册一个定时任务
// runOnStart: 是否在启动时立即执行一次
func Register(name string, interval time.Duration, runOnStart bool, fn func()) {
	if interval <= 0 {
		log.Debugf("task %s not registered: interval is 0", name)
		return
	}

	for {
		tasksMu.Lock()
		if stopping := stoppingTasks[name]; stopping != nil {
			tasksMu.Unlock()
			<-stopping.doneCh
			continue
		}
		if _, exists := tasks[name]; exists {
			tasksMu.Unlock()
			log.Warnf("task %s already registered, skipping", name)
			return
		}

		taskGateLocked(name)
		entry := newTaskEntry(name, interval, runOnStart, fn)
		tasks[name] = entry
		if runnerStarted {
			startEntry(entry)
		}
		tasksMu.Unlock()
		log.Debugf("task %s registered with interval %v, runOnStart: %v", name, interval, runOnStart)
		return
	}
}

// Configure 创建或更新一个定时任务；interval 为 0 时停止并删除任务。
func Configure(name string, interval time.Duration, runOnStart bool, fn func()) {
	if interval <= 0 {
		Update(name, 0)
		return
	}

	for {
		tasksMu.Lock()
		if stopping := stoppingTasks[name]; stopping != nil {
			tasksMu.Unlock()
			<-stopping.doneCh
			continue
		}
		if entry, exists := tasks[name]; exists {
			entry.interval.Store(int64(interval))
			signalIntervalUpdate(entry)
			tasksMu.Unlock()
			log.Infof("task %s interval updated to %v", name, interval)
			return
		}

		taskGateLocked(name)
		entry := newTaskEntry(name, interval, runOnStart, fn)
		tasks[name] = entry
		if runnerStarted {
			startEntry(entry)
		}
		tasksMu.Unlock()
		log.Debugf("task %s configured with interval %v, runOnStart: %v", name, interval, runOnStart)
		return
	}
}

// Update 更新任务的执行间隔
// 当 interval 为 0 时，删除任务
func Update(name string, interval time.Duration) {
	tasksMu.Lock()
	entry, exists := tasks[name]
	if !exists {
		tasksMu.Unlock()
		log.Warnf("task %s not found", name)
		return
	}

	if interval <= 0 {
		delete(tasks, name)
		entry.inactive.Store(true)
		stoppingTasks[name] = entry
		entry.stopOnce.Do(func() { close(entry.stopCh) })
		if !entry.loopStarted.Load() {
			entry.doneOnce.Do(func() { close(entry.doneCh) })
		}
		tasksMu.Unlock()
		<-entry.doneCh
		tasksMu.Lock()
		if stoppingTasks[name] == entry {
			delete(stoppingTasks, name)
		}
		tasksMu.Unlock()
		log.Infof("task %s removed: interval is 0", name)
		return
	}

	entry.interval.Store(int64(interval))
	signalIntervalUpdate(entry)
	tasksMu.Unlock()
	log.Infof("task %s interval updated to %v", name, interval)
}

// startEntry starts at most one scheduler loop for entry. tasksMu must be held
// by callers that are adding an entry to the task table.
func startEntry(entry *taskEntry) {
	if entry == nil || !entry.loopStarted.CompareAndSwap(false, true) {
		return
	}
	safe.Go("task-loop:"+entry.name, func() {
		runTask(entry)
	})
}

func signalIntervalUpdate(entry *taskEntry) {
	select {
	case entry.wakeCh <- struct{}{}:
	default:
	}
}

// RUN 启动所有注册的任务
func RUN() {
	startRunner()

	// 阻塞主协程
	select {}
}

func startRunner() {
	tasksMu.Lock()
	if !runnerStarted {
		runnerStarted = true
		for _, entry := range tasks {
			startEntry(entry)
		}
	}
	tasksMu.Unlock()
}

func runTask(entry *taskEntry) {
	defer entry.doneOnce.Do(func() { close(entry.doneCh) })
	if entry.inactive.Load() {
		return
	}

	// 根据配置决定是否在启动时立即执行
	if entry.runOnStart {
		triggerTask(entry, "startup")
	}

	ticker := time.NewTicker(time.Duration(entry.interval.Load()))
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			triggerTask(entry, "ticker")
		case <-entry.wakeCh:
			ticker.Stop()
			ticker = time.NewTicker(time.Duration(entry.interval.Load()))
		case <-entry.stopCh:
			return
		}
	}
}

func triggerTask(entry *taskEntry, trigger string) {
	if entry == nil {
		return
	}

	// The table check and gate acquisition are one lifecycle decision: after
	// disable removes the entry, no new execution can be admitted.
	tasksMu.Lock()
	if entry.inactive.Load() || tasks[entry.name] != entry || entry.gate == nil || !entry.gate.running.CompareAndSwap(false, true) {
		tasksMu.Unlock()
		if entry.gate != nil && entry.gate.running.Load() {
			log.Warnf("task %s skipped: previous run still in progress (trigger=%s)", entry.name, trigger)
		}
		return
	}
	tasksMu.Unlock()

	safe.Go("task-exec:"+entry.name+":"+trigger, func() {
		defer entry.gate.running.Store(false)
		if entry.fn != nil {
			entry.fn()
		}
	})
}
