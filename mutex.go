package concurrent

import (
	"runtime"
	"sync/atomic"
	"time"
)

const (
	mutexUnlocked = 0
	mutexLocked   = 1
)

type TryLocker interface {
	TryLock() bool
	TryLockTimeout(timeout time.Duration) bool
}

type Mutex struct {
	state uint32  // Single atomic state: 0=unlocked, 1=locked
}

// op provides lightweight atomic locking for other components (Future, Queue)
type op struct {
	state uint32  // Single atomic state: 0=unlocked, 1=locked
}

// acquire implements highly optimized adaptive locking for op struct
// Uses CPU-efficient spinning strategy to minimize power consumption
func (o *op) acquire() {
	// Fast path: try to acquire immediately
	if atomic.CompareAndSwapUint32(&o.state, 0, 1) {
		return
	}
	
	// Optimized adaptive spinning with CPU efficiency focus
	for i := 0; ; i++ {
		if i < 3 {
			// Phase 1: Brief CPU spinning - reduced iterations to save CPU
			for j := 0; j < 6; j++ { // Reduced from 30 to 6 for lower CPU usage
				if atomic.CompareAndSwapUint32(&o.state, 0, 1) {
					return
				}
				runtime.Gosched() // Yield earlier in tight loop
			}
		} else if i < 12 {
			// Phase 2: Extended yield phase for better CPU efficiency
			runtime.Gosched()
			if atomic.CompareAndSwapUint32(&o.state, 0, 1) {
				return
			}
		} else {
			// Phase 3: Ultra-minimal sleep to reduce CPU idle consumption
			time.Sleep(time.Nanosecond * 25) // Reduced from 100ns to 25ns
			if atomic.CompareAndSwapUint32(&o.state, 0, 1) {
				return
			}
			i = 0 // Reset to maintain phase cycling
		}
	}
}

// release releases the lock held by op struct
func (o *op) release() {
	atomic.StoreUint32(&o.state, 0)
}

// TryLock attempts to acquire the lock without blocking.
// Returns true if the lock is successfully acquired, false if already held.
func (m *Mutex) TryLock() bool {
	return atomic.CompareAndSwapUint32(&m.state, mutexUnlocked, mutexLocked)
}

// Lock acquires the mutex lock, blocking until available.
// Uses CPU-efficient adaptive spinning optimized for low power consumption.
func (m *Mutex) Lock() {
	// Fast path: try to acquire immediately
	if atomic.CompareAndSwapUint32(&m.state, mutexUnlocked, mutexLocked) {
		return
	}
	
	// Highly optimized slow path: CPU efficiency focused
	for i := 0; ; i++ {
		if i < 4 {
			// Phase 1: Brief spinning with early yielding for CPU efficiency
			for j := 0; j < 4; j++ { // Reduced from 10 to 4 iterations
				if atomic.CompareAndSwapUint32(&m.state, mutexUnlocked, mutexLocked) {
					return
				}
				runtime.Gosched() // Yield in tight loop for better scheduling
			}
		} else if i < 20 {
			// Phase 2: Extended cooperative yielding - optimized range
			runtime.Gosched()
			if atomic.CompareAndSwapUint32(&m.state, mutexUnlocked, mutexLocked) {
				return
			}
		} else {
			// Phase 3: Minimal sleep with progressive backoff
			sleepDuration := time.Nanosecond * time.Duration(20+(i%10)) // Progressive 20-30ns
			time.Sleep(sleepDuration)
			if atomic.CompareAndSwapUint32(&m.state, mutexUnlocked, mutexLocked) {
				return
			}
			i = 0 // Reset to maintain phase cycling
		}
	}
}

// Unlock releases the mutex lock.
func (m *Mutex) Unlock() {
	if !atomic.CompareAndSwapUint32(&m.state, mutexLocked, mutexUnlocked) {
		panic("concurrent: unlock of unlocked mutex")
	}
}

// TryLockTimeout attempts to acquire the lock within the specified timeout duration.
// Returns true if the lock is successfully acquired, false if the timeout expires.
// Optimized for minimal CPU overhead during timeout operations.
func (m *Mutex) TryLockTimeout(timeout time.Duration) bool {
	// Fast path: try to acquire immediately
	if atomic.CompareAndSwapUint32(&m.state, mutexUnlocked, mutexLocked) {
		return true
	}
	
	// Use more efficient timeout checking
	deadline := time.Now().Add(timeout)
	checkInterval := timeout / 100 // Dynamic check interval based on timeout
	if checkInterval < time.Microsecond {
		checkInterval = time.Microsecond
	} else if checkInterval > time.Millisecond {
		checkInterval = time.Millisecond
	}
	
	for i := 0; ; i++ {
		// Efficient timeout check with reduced frequency
		if i%10 == 0 && time.Now().After(deadline) {
			return false
		}
		
		if i < 4 {
			// Phase 1: Minimal spinning for efficiency
			for j := 0; j < 3; j++ { // Reduced from 20 to 3
				if atomic.CompareAndSwapUint32(&m.state, mutexUnlocked, mutexLocked) {
					return true
				}
				runtime.Gosched()
			}
		} else if i < 16 {
			// Phase 2: Cooperative yielding
			runtime.Gosched()
			if atomic.CompareAndSwapUint32(&m.state, mutexUnlocked, mutexLocked) {
				return true
			}
		} else {
			// Phase 3: Adaptive sleep based on timeout duration
			sleepTime := checkInterval / 20
			if sleepTime < time.Nanosecond*10 {
				sleepTime = time.Nanosecond * 10
			}
			time.Sleep(sleepTime)
			if atomic.CompareAndSwapUint32(&m.state, mutexUnlocked, mutexLocked) {
				return true
			}
			i = 0
		}
	}
}

// IsLocked reports whether the mutex is currently locked.
// Note that the status may change immediately after this call returns.
func (m *Mutex) IsLocked() bool {
	return atomic.LoadUint32(&m.state) == mutexLocked
}

// Unlocked returns a channel that will be closed when the mutex is unlocked.
// This optimized version uses efficient polling to minimize memory allocation.
func (m *Mutex) Unlocked() chan struct{} {
	// If already unlocked, return a closed channel immediately
	if atomic.LoadUint32(&m.state) == mutexUnlocked {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	
	// For locked mutex, use a lightweight approach
	ch := make(chan struct{})
	
	// Use a single goroutine with minimal overhead
	go func() {
		defer close(ch)
		
		// Simple polling with fixed small interval to reduce CPU and memory overhead
		for atomic.LoadUint32(&m.state) == mutexLocked {
			time.Sleep(time.Microsecond * 10) // Fixed small interval
			runtime.Gosched() // Yield to scheduler
		}
	}()
	
	return ch
}
