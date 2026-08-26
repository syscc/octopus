package task

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/internal/utils/safe"
)

// Schedule returns the next execution time after the supplied instant.
type Schedule interface {
	Next(time.Time) time.Time
}

type intervalSchedule struct {
	interval time.Duration
}

func (s intervalSchedule) Next(after time.Time) time.Time {
	return after.Add(s.interval)
}

func NewIntervalSchedule(interval time.Duration) Schedule {
	return intervalSchedule{interval: interval}
}

type taskGate struct {
	running atomic.Bool
}

type taskEntry struct {
	name        string
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

	scheduleMu sync.RWMutex
	schedule   Schedule
}

var (
	tasks         = make(map[string]*taskEntry)
	stoppingTasks = make(map[string]*taskEntry)
	taskGates     = make(map[string]*taskGate)
	tasksMu       sync.Mutex
	runnerStarted bool
)

func newTaskEntry(name string, schedule Schedule, runOnStart bool, fn func()) *taskEntry {
	return &taskEntry{
		name:       name,
		fn:         fn,
		runOnStart: runOnStart,
		stopCh:     make(chan struct{}),
		wakeCh:     make(chan struct{}, 1),
		doneCh:     make(chan struct{}),
		gate:       taskGates[name],
		schedule:   schedule,
	}
}

func taskGateLocked(name string) *taskGate {
	gate := taskGates[name]
	if gate == nil {
		gate = &taskGate{}
		taskGates[name] = gate
	}
	return gate
}

// Register registers a duration-based scheduled task.
// runOnStart controls whether the task executes once when the runner starts.
func Register(name string, interval time.Duration, runOnStart bool, fn func()) {
	if interval <= 0 {
		log.Debugf("task %s not registered: interval is 0", name)
		return
	}
	RegisterSchedule(name, NewIntervalSchedule(interval), runOnStart, fn)
}

func RegisterSchedule(name string, schedule Schedule, runOnStart bool, fn func()) {
	if schedule == nil {
		log.Debugf("task %s not registered: schedule is nil", name)
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
		entry := newTaskEntry(name, schedule, runOnStart, fn)
		tasks[name] = entry
		if runnerStarted {
			startEntry(entry)
		}
		tasksMu.Unlock()
		log.Debugf("task %s registered with runOnStart: %v", name, runOnStart)
		return
	}
}

// Configure creates or updates a duration-based scheduled task; interval 0
// stops and removes the task.
func Configure(name string, interval time.Duration, runOnStart bool, fn func()) {
	if interval <= 0 {
		Update(name, 0)
		return
	}
	ConfigureSchedule(name, NewIntervalSchedule(interval), runOnStart, fn)
}

// ConfigureSchedule creates or updates a task with an arbitrary schedule.
func ConfigureSchedule(name string, schedule Schedule, runOnStart bool, fn func()) {
	if schedule == nil {
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
			entry.scheduleMu.Lock()
			entry.schedule = schedule
			entry.scheduleMu.Unlock()
			if !entry.loopStarted.Load() {
				entry.runOnStart = runOnStart
			}
			signalScheduleUpdate(entry)
			tasksMu.Unlock()
			log.Infof("task %s schedule updated", name)
			return
		}

		taskGateLocked(name)
		entry := newTaskEntry(name, schedule, runOnStart, fn)
		tasks[name] = entry
		if runnerStarted {
			startEntry(entry)
		}
		tasksMu.Unlock()
		log.Debugf("task %s configured with runOnStart: %v", name, runOnStart)
		return
	}
}

// Update updates a duration-based task. When interval is 0, the task is
// stopped and removed.
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

	entry.scheduleMu.Lock()
	entry.schedule = NewIntervalSchedule(interval)
	entry.scheduleMu.Unlock()
	signalScheduleUpdate(entry)
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

func signalScheduleUpdate(entry *taskEntry) {
	select {
	case entry.wakeCh <- struct{}{}:
	default:
	}
}

// RUN starts all registered tasks and then keeps the runner alive.
func RUN() {
	startRunner()

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

func currentSchedule(entry *taskEntry) Schedule {
	entry.scheduleMu.RLock()
	defer entry.scheduleMu.RUnlock()
	return entry.schedule
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func runTask(entry *taskEntry) {
	defer entry.doneOnce.Do(func() { close(entry.doneCh) })
	if entry.inactive.Load() {
		return
	}

	if entry.runOnStart {
		triggerTask(entry, "startup")
	}

	for {
		schedule := currentSchedule(entry)
		if schedule == nil {
			return
		}
		next := schedule.Next(time.Now())
		if next.IsZero() {
			return
		}
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)

		select {
		case <-timer.C:
			triggerTask(entry, "schedule")
		case <-entry.wakeCh:
			stopTimer(timer)
		case <-entry.stopCh:
			stopTimer(timer)
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
