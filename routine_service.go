package concurrent

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	gorReady = iota
	gorRunning
	gorPendingShutdown
	gorShutdown
)

var ErrShutdownTwice = fmt.Errorf("shutdown a shutdown rs service")
var ErrTaskQueueFull = fmt.Errorf("task queue is full")
var ErrShutdown = fmt.Errorf("shutdown")

type SubmitTask func(r Routine) any

type RoutineService interface {
	Submit(SubmitTask) RoutineFuture
	MaxRoutines() int
	StandbyRoutines() int
	MaxTasks() int
	Tasks() int
	DoneTasks() uint64
	IsShutdown() bool
	ShutdownGracefully()
	ShutdownImmediately()
}

type RoutineFuture interface {
	Future
	RoutineId() int
}

type defaultRoutineFuture struct {
	Future
	r *routine
}

func (drf *defaultRoutineFuture) Await() Future {
	drf.Future.Await()
	return drf
}

func (drf *defaultRoutineFuture) AwaitTimeout(timeout time.Duration) Future {
	drf.Future.AwaitTimeout(timeout)
	return drf
}
func (drf *defaultRoutineFuture) AddListener(listener FutureListener) Future {
	drf.Future.AddListener(listener)
	return drf
}

func (drf *defaultRoutineFuture) RoutineId() int {
	if drf.r != nil {
		return drf.r.RoutineId()
	}

	return 0
}

type Routine interface {
	RoutineId() int
	Local() *sync.Map
}

type routine struct {
	rs        *routines
	shutdown  Future
	kv        sync.Map
	state     int32 // Use int32 for atomic operations
	rid       uint32
	doneCount uint64 // Already uint64 for atomic operations
	// Performance optimization fields
	ridCached  bool  // Whether goroutine ID is cached
	lastActive int64 // Last activity timestamp for optimization
}

// getGoRoutineId retrieves the current goroutine ID with caching optimization.
// This method parses the runtime stack trace to extract the goroutine ID.
// Note: This is for debugging purposes only and should not be relied upon for correctness.
func (r *routine) getGoRoutineId() int {
	// Return cached ID if available
	if r.ridCached {
		return int(atomic.LoadUint32(&r.rid))
	}

	var buf [64]byte
	n := runtime.Stack(buf[:], false)

	// Parse "goroutine 123 [running]:" format
	stack := string(buf[:n])
	if !strings.HasPrefix(stack, "goroutine ") {
		return 0 // Fallback for unexpected format
	}

	idStr := strings.Fields(strings.TrimPrefix(stack, "goroutine "))[0]
	if id, err := strconv.Atoi(idStr); err == nil {
		// Cache the ID for future use
		atomic.StoreUint32(&r.rid, uint32(id))
		r.ridCached = true
		return id
	}

	return 0 // Fallback if parsing fails
}

func (r *routine) run() {
	go func(r *routine) {
		// Initialize goroutine ID and optimization fields
		atomic.StoreUint32(&r.rid, uint32(r.getGoRoutineId()))
		atomic.StoreInt64(&r.lastActive, time.Now().UnixNano())

		// Use short, fixed timeout to ensure responsiveness to shutdown signals
		// This prevents deadlock during shutdown while maintaining reasonable performance
		const pollTimeout = time.Millisecond * 10 // Fixed 10ms timeout for responsiveness

		for !r.shutdown.IsDone() {
			if v := r.rs.tq.PopTimeout(pollTimeout); v == nil {
				// Double-check shutdown status after timeout to ensure responsiveness
				if r.shutdown.IsDone() {
					break
				}
				// Continue polling - no need for adaptive timeout since we use a fixed short timeout
			} else {
				// Process the task
				atomic.StoreInt32(&r.state, gorRunning)
				atomic.StoreInt64(&r.lastActive, time.Now().UnixNano())

				v.(*routineTask).t(r)
				atomic.AddUint64(&r.doneCount, 1)
				atomic.StoreInt32(&r.state, gorReady)

				// Check shutdown status after each task to ensure quick response
				if r.shutdown.IsDone() {
					break
				}
			}
		}

		atomic.StoreInt32(&r.state, gorShutdown)
	}(r)
}

func (r *routine) RoutineId() int {
	return int(r.rid)
}

func (r *routine) Local() *sync.Map {
	return &r.kv
}

type routineTask struct {
	f RoutineFuture
	t func(r *routine)
}

type routines struct {
	blocking       bool
	state          int32
	maxRoutines    int
	maxQueuedTasks int
	tq             BlockingQueue
	routineList    []*routine
	// Performance optimization fields
	cachedStandby     int32  // Cached standby routine count
	cachedDone        uint64 // Cached done task count
	lastStatsTime     int64  // Last statistics update time for standby routines
	lastDoneStatsTime int64  // Last statistics update time for done tasks
	statsInterval     int64  // Statistics cache interval (nanoseconds)
}

func (rs *routines) MaxRoutines() int {
	return rs.maxRoutines
}

func (rs *routines) StandbyRoutines() int {
	// Always recalculate for accuracy
	v := 0
	for _, r := range rs.routineList {
		if atomic.LoadInt32(&r.state) == gorReady {
			v++
		}
	}

	// Update cache for other methods that might use it
	atomic.StoreInt32(&rs.cachedStandby, int32(v))
	atomic.StoreInt64(&rs.lastStatsTime, time.Now().UnixNano())

	return v
}

func (rs *routines) MaxTasks() int {
	return rs.maxQueuedTasks
}

func (rs *routines) Tasks() int {
	return rs.tq.Len()
}

func (rs *routines) DoneTasks() uint64 {
	// Use cached value if recent enough (within 50ms)
	now := time.Now().UnixNano()
	if now-atomic.LoadInt64(&rs.lastDoneStatsTime) < 50*1000*1000 { // 50ms in nanoseconds
		return atomic.LoadUint64(&rs.cachedDone)
	}

	// Recalculate and cache
	v := uint64(0)
	for _, r := range rs.routineList {
		v += atomic.LoadUint64(&r.doneCount)
	}

	// Update cache with separate timestamp
	atomic.StoreUint64(&rs.cachedDone, v)
	atomic.StoreInt64(&rs.lastDoneStatsTime, now)

	return v
}

func (rs *routines) buildTask(submit SubmitTask) (RoutineFuture, func(r *routine)) {
	sf := submit
	rf := &defaultRoutineFuture{Future: NewFuture()}
	task := func(r *routine) {
		rf := rf
		rf.r = r
		defer func() {
			if e := recover(); e != nil {
				if cast, ok := e.(error); ok {
					rf.Completable().Fail(cast)
				} else {
					rf.Completable().Fail(fmt.Errorf("%v", e))
				}
			}
		}()

		if sf != nil {
			rf.Completable().Complete(sf(r))
		}
	}

	return rf, task
}

func (rs *routines) pushTask(rf RoutineFuture, task func(r *routine)) *routineTask {
	rt := &routineTask{
		f: rf,
		t: task,
	}

	// Fast path for unlimited queue
	if rs.maxQueuedTasks < 0 {
		rs.tq.PushTimeout(rt, time.Microsecond)
		return rt
	}

	// Check capacity with optimized calculation
	queueLen := rs.tq.Len()
	standbyRoutines := int(atomic.LoadInt32(&rs.cachedStandby)) // Use cached value for better performance

	if queueLen-standbyRoutines > rs.maxQueuedTasks {
		if !rs.blocking {
			// Non-blocking: fail immediately
			rf.Completable().Fail(ErrTaskQueueFull)
			return rt
		} else {
			// Blocking: wait with adaptive backoff
			backoff := time.Microsecond
			maxBackoff := time.Millisecond

			for queueLen-rs.StandbyRoutines() > rs.maxQueuedTasks {
				if atomic.LoadInt32(&rs.state) != gorRunning {
					rf.Completable().Fail(ErrShutdown)
					return rt
				}

				time.Sleep(backoff)
				if backoff < maxBackoff {
					backoff *= 2
				}
				queueLen = rs.tq.Len()
			}
		}
	}

	// Push task with appropriate timeout
	if !rs.blocking {
		if rs.tq.PushTimeout(rt, time.Microsecond) {
			return rt
		}
		rf.Completable().Fail(ErrTaskQueueFull)
		return rt
	} else {
		// Blocking mode: retry until success or shutdown
		for !rs.tq.PushTimeout(rt, time.Microsecond) {
			if atomic.LoadInt32(&rs.state) != gorRunning {
				rf.Completable().Fail(ErrShutdown)
				break
			}
			runtime.Gosched()
		}
		return rt
	}
}

func (rs *routines) Submit(f SubmitTask) RoutineFuture {
	if atomic.LoadInt32(&rs.state) != gorRunning {
		return &defaultRoutineFuture{Future: NewFailedFuture(ErrShutdown)}
	}

	return rs.pushTask(rs.buildTask(f)).f
}

func (rs *routines) IsShutdown() bool {
	return atomic.LoadInt32(&rs.state) == gorShutdown
}

// ShutdownGracefully initiates a graceful shutdown of the routine service.
// It stops accepting new tasks, waits for all queued tasks to complete,
// then shuts down all worker routines. This method blocks until shutdown is complete.
func (rs *routines) ShutdownGracefully() {
	if !atomic.CompareAndSwapInt32(&rs.state, gorRunning, gorPendingShutdown) {
		panic(ErrShutdownTwice)
	}

	for rs.tq.Len() > 0 {
		runtime.Gosched()
	}

	for _, r := range rs.routineList {
		r.shutdown.Completable().Complete(nil)
	}

	for _, r := range rs.routineList {
		for atomic.LoadInt32(&r.state) != gorShutdown {
			runtime.Gosched()
		}
	}

	atomic.StoreInt32(&rs.state, gorShutdown)
}

// ShutdownImmediately initiates an immediate shutdown of the routine service.
// It stops accepting new tasks, drops all queued tasks without processing them,
// and shuts down all worker routines immediately. This method returns quickly.
func (rs *routines) ShutdownImmediately() {
	if !atomic.CompareAndSwapInt32(&rs.state, gorRunning, gorPendingShutdown) {
		panic(ErrShutdownTwice)
	}

	for _, r := range rs.routineList {
		r.shutdown.Completable().Complete(nil)
	}

	atomic.StoreInt32(&rs.state, gorShutdown)
}

func newRoutinePool(nRoutines int, maxQueuedTasks int, blocking bool) RoutineService {
	r := &routines{
		blocking:       blocking,
		tq:             NewBlockingQueue(maxQueuedTasks),
		state:          gorRunning,
		maxRoutines:    nRoutines,
		maxQueuedTasks: maxQueuedTasks,
		// Initialize optimization fields
		cachedStandby:     int32(nRoutines), // Initially all routines are standby
		cachedDone:        0,
		lastStatsTime:     time.Now().UnixNano(),
		lastDoneStatsTime: 0,                // Initialize as 0 to force initial calculation
		statsInterval:     10 * 1000 * 1000, // 10ms in nanoseconds
	}

	// Pre-allocate routine list for better performance
	r.routineList = make([]*routine, 0, nRoutines)

	for i := 0; i < nRoutines; i++ {
		rw := &routine{
			rs:        r,
			shutdown:  NewFuture(),
			ridCached: false,
		}

		r.routineList = append(r.routineList, rw)
		rw.run()
	}

	return r
}

// NewRoutinePool creates a fixed-size routine pool with a limited task queue.
// When the task queue is full, Submit operations will return a failed Future immediately.
//
// Parameters:
//   - nRoutines: Number of worker routines in the pool (must be > 0)
//   - maxQueuedTasks: Maximum number of queued tasks (-1 for unlimited capacity)
//
// This is ideal for scenarios where you want fast failure when the system is overloaded.
func NewRoutinePool(nRoutines int, maxQueuedTasks int) RoutineService {
	return newRoutinePool(nRoutines, maxQueuedTasks, false)
}

// NewBlockingRoutinePool creates a fixed-size routine pool with a limited task queue.
// When the task queue is full, Submit operations will block until space becomes available.
//
// Parameters:
//   - nRoutines: Number of worker routines in the pool (must be > 0)
//   - maxQueuedTasks: Maximum number of queued tasks (-1 for unlimited capacity)
//
// This is ideal for scenarios where you want to apply backpressure when overloaded.
func NewBlockingRoutinePool(nRoutines int, maxQueuedTasks int) RoutineService {
	return newRoutinePool(nRoutines, maxQueuedTasks, true)
}
