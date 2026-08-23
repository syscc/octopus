package task

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func resetTaskStateForTest(t *testing.T) {
	t.Helper()
	tasksMu.Lock()
	entries := make([]*taskEntry, 0, len(tasks))
	for _, entry := range tasks {
		entries = append(entries, entry)
	}
	tasksMu.Unlock()

	for _, entry := range entries {
		Update(entry.name, 0)
	}

	tasksMu.Lock()
	activeTasks := len(tasks)
	stopping := len(stoppingTasks)
	runnerStarted = false
	tasksMu.Unlock()
	if activeTasks != 0 {
		t.Fatalf("task state leaked active entries: %d", activeTasks)
	}
	if stopping != 0 {
		t.Fatalf("task state leaked stopping entries: %d", stopping)
	}
}

func waitForTaskSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for task execution")
	}
}

func waitForTaskDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for task callback to finish")
	}
}

type taskCallbackControl struct {
	fn       func()
	fired    <-chan struct{}
	finished <-chan struct{}
	release  func()
}

func newBlockingTaskCallback(calls *atomic.Int32) taskCallbackControl {
	fired := make(chan struct{})
	finished := make(chan struct{})
	releaseCh := make(chan struct{})
	var startOnce sync.Once
	var doneOnce sync.Once
	var releaseOnce sync.Once
	return taskCallbackControl{
		fn: func() {
			calls.Add(1)
			startOnce.Do(func() { close(fired) })
			<-releaseCh
			doneOnce.Do(func() { close(finished) })
		},
		fired:    fired,
		finished: finished,
		release:  func() { releaseOnce.Do(func() { close(releaseCh) }) },
	}
}

func assertNoStoppingTasks(t *testing.T) {
	t.Helper()
	tasksMu.Lock()
	stopping := len(stoppingTasks)
	tasksMu.Unlock()
	if stopping != 0 {
		t.Fatalf("expected no stopping tasks, got %d", stopping)
	}
}

func TestDynamicConfigureAndReenable(t *testing.T) {
	resetTaskStateForTest(t)
	defer resetTaskStateForTest(t)

	startRunner()
	var firstRuns atomic.Int32
	first := newBlockingTaskCallback(&firstRuns)
	defer first.release()
	Configure("dynamic-task", 5*time.Millisecond, false, first.fn)
	waitForTaskSignal(t, first.fired)
	beforeDisable := firstRuns.Load()

	Update("dynamic-task", 0)
	assertNoStoppingTasks(t)
	first.release()
	waitForTaskDone(t, first.finished)
	if got := firstRuns.Load(); got != beforeDisable {
		t.Fatalf("task callback changed after disable: before=%d after=%d", beforeDisable, got)
	}

	var secondRuns atomic.Int32
	second := newBlockingTaskCallback(&secondRuns)
	defer second.release()
	Configure("dynamic-task", 5*time.Millisecond, false, second.fn)
	waitForTaskSignal(t, second.fired)
	second.release()
	waitForTaskDone(t, second.finished)
	if secondRuns.Load() == 0 {
		t.Fatal("expected callback from re-enabled task")
	}

	tasksMu.Lock()
	entry := tasks["dynamic-task"]
	activeEntries := len(tasks)
	tasksMu.Unlock()
	if entry == nil || !entry.loopStarted.Load() || activeEntries != 1 {
		t.Fatalf("expected one active restarted entry, entry=%v active=%d", entry != nil, activeEntries)
	}

	Register("dynamic-task", 5*time.Millisecond, false, second.fn)
	tasksMu.Lock()
	activeEntries = len(tasks)
	tasksMu.Unlock()
	if activeEntries != 1 {
		t.Fatalf("duplicate Register created an entry: active=%d", activeEntries)
	}
}

func TestConfigureBeforeAndAfterRunnerStarts(t *testing.T) {
	resetTaskStateForTest(t)
	defer resetTaskStateForTest(t)

	var beforeRuns atomic.Int32
	before := newBlockingTaskCallback(&beforeRuns)
	defer before.release()
	Configure("configured-before-runner", 5*time.Millisecond, false, before.fn)

	tasksMu.Lock()
	beforeEntry := tasks["configured-before-runner"]
	tasksMu.Unlock()
	if beforeEntry == nil || beforeEntry.loopStarted.Load() {
		t.Fatalf("expected Configure before runner to defer loop start, entry=%+v", beforeEntry)
	}

	startRunner()
	waitForTaskSignal(t, before.fired)
	before.release()
	waitForTaskDone(t, before.finished)

	var afterRuns atomic.Int32
	after := newBlockingTaskCallback(&afterRuns)
	defer after.release()
	Configure("configured-after-runner", time.Hour, true, after.fn)
	waitForTaskSignal(t, after.fired)
	after.release()
	waitForTaskDone(t, after.finished)
	if afterRuns.Load() == 0 {
		t.Fatal("expected runOnStart callback after runner started")
	}
}

func TestConfigureZeroToPositiveAfterRunner(t *testing.T) {
	resetTaskStateForTest(t)
	defer resetTaskStateForTest(t)

	startRunner()
	Configure("zero-to-positive", 0, true, func() {})
	tasksMu.Lock()
	_, exists := tasks["zero-to-positive"]
	tasksMu.Unlock()
	if exists {
		t.Fatal("interval zero should not leave an active task")
	}

	var runs atomic.Int32
	callback := newBlockingTaskCallback(&runs)
	defer callback.release()
	Configure("zero-to-positive", 5*time.Millisecond, false, callback.fn)
	waitForTaskSignal(t, callback.fired)
	callback.release()
	waitForTaskDone(t, callback.finished)
	if runs.Load() == 0 {
		t.Fatal("expected zero-to-positive Configure to start callback")
	}
}

func updateMaximum(maximum *atomic.Int32, current int32) {
	for {
		old := maximum.Load()
		if current <= old || maximum.CompareAndSwap(old, current) {
			return
		}
	}
}

func TestTaskExecutionGateSpansDisableAndReenable(t *testing.T) {
	resetTaskStateForTest(t)
	defer resetTaskStateForTest(t)

	startRunner()
	var active atomic.Int32
	var maximum atomic.Int32
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	var releaseFirstOnce sync.Once
	var releaseSecondOnce sync.Once
	defer releaseFirstOnce.Do(func() { close(firstRelease) })
	defer releaseSecondOnce.Do(func() { close(secondRelease) })

	firstStarted := make(chan struct{})
	firstDone := make(chan struct{})
	var firstStartOnce sync.Once
	var firstDoneOnce sync.Once
	firstFn := func() {
		current := active.Add(1)
		updateMaximum(&maximum, current)
		firstStartOnce.Do(func() { firstStarted <- struct{}{} })
		<-firstRelease
		active.Add(-1)
		firstDoneOnce.Do(func() { close(firstDone) })
	}

	Register("gated-task", 5*time.Millisecond, false, firstFn)
	waitForTaskSignal(t, firstStarted)
	Update("gated-task", 0)
	assertNoStoppingTasks(t)

	secondStarted := make(chan struct{})
	secondDone := make(chan struct{})
	var secondStartOnce sync.Once
	var secondDoneOnce sync.Once
	secondFn := func() {
		current := active.Add(1)
		updateMaximum(&maximum, current)
		secondStartOnce.Do(func() { secondStarted <- struct{}{} })
		<-secondRelease
		active.Add(-1)
		secondDoneOnce.Do(func() { close(secondDone) })
	}
	Configure("gated-task", 5*time.Millisecond, false, secondFn)

	releaseFirstOnce.Do(func() { close(firstRelease) })
	waitForTaskDone(t, firstDone)
	waitForTaskSignal(t, secondStarted)
	releaseSecondOnce.Do(func() { close(secondRelease) })
	waitForTaskDone(t, secondDone)
	Update("gated-task", 0)
	assertNoStoppingTasks(t)

	if got := maximum.Load(); got != 1 {
		t.Fatalf("expected logical execution gate to keep max concurrency at 1, got %d", got)
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("expected all callbacks to finish, active=%d", got)
	}
}
