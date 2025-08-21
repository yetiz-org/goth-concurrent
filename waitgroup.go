package concurrent

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type WaitGroup struct {
	once       sync.Once
	rl         sync.Locker
	s          *sync.Cond
	delta      int64
	// Performance optimization fields
	waiters    int32  // Number of goroutines waiting
	lastUpdate int64  // Timestamp of last delta change for optimization
	spinLimit  int32  // Adaptive spinning limit
}

func (w *WaitGroup) lazyInit() {
	w.once.Do(func() {
		l := sync.RWMutex{}
		w.rl = l.RLocker()
		w.s = sync.NewCond(&l)
		// Initialize optimization fields
		atomic.StoreInt32(&w.spinLimit, 100) // Default adaptive spin limit
	})
}

// Add adds delta to the WaitGroup counter.
// If the counter becomes zero, all goroutines blocked on Wait are released.
// If the counter would go negative, the operation is ignored to prevent race conditions.
func (w *WaitGroup) Add(delta int) {
	w.lazyInit()
	
	// Fast path for simple increments without waiters
	if delta > 0 && atomic.LoadInt32(&w.waiters) == 0 {
		atomic.AddInt64(&w.delta, int64(delta))
		atomic.StoreInt64(&w.lastUpdate, time.Now().UnixNano())
		return
	}
	
	// Slow path: need to check for negative values and potentially notify waiters
	w.s.L.Lock()
	defer w.s.L.Unlock()
	
	// Check if the operation would result in a negative counter
	currentValue := atomic.LoadInt64(&w.delta)
	if currentValue+int64(delta) < 0 {
		return // Silently ignore operations that would make counter negative
	}

	newValue := atomic.AddInt64(&w.delta, int64(delta))
	atomic.StoreInt64(&w.lastUpdate, time.Now().UnixNano())
	
	if newValue == 0 {
		// Wake up all waiters
		w.s.Broadcast()
	}
}

// Done decrements the WaitGroup counter by one.
// This method should be called when a goroutine completes its work.
// It's equivalent to calling Add(-1).
func (w *WaitGroup) Done() {
	w.Add(-1)
}

// Wait blocks until the WaitGroup counter is zero.
// It should be called by the main goroutine that is waiting for other goroutines to complete.
// This method will block until all goroutines have called Done().
func (w *WaitGroup) Wait() {
	w.lazyInit()
	
	// Fast path: check if already zero without lock
	if atomic.LoadInt64(&w.delta) == 0 {
		return
	}
	
	// Adaptive spinning before blocking
	spinLimit := atomic.LoadInt32(&w.spinLimit)
	for i := int32(0); i < spinLimit; i++ {
		if atomic.LoadInt64(&w.delta) == 0 {
			// Adjust spin limit based on success
			if i < spinLimit/2 {
				atomic.CompareAndSwapInt32(&w.spinLimit, spinLimit, spinLimit+10)
			}
			return
		}
		runtime.Gosched()
	}
	
	// Slow path: acquire lock and wait
	atomic.AddInt32(&w.waiters, 1)
	defer atomic.AddInt32(&w.waiters, -1)
	
	w.s.L.Lock()
	for atomic.LoadInt64(&w.delta) != 0 {
		w.s.Wait()
	}
	w.s.L.Unlock()
	
	// Adjust spin limit based on blocking
	if spinLimit > 10 {
		atomic.CompareAndSwapInt32(&w.spinLimit, spinLimit, spinLimit-5)
	}
}

// Remain returns the current number of goroutines that haven't called Done yet.
// This method provides a way to check how many operations are still pending.
// The value may change immediately after this method returns.
func (w *WaitGroup) Remain() int {
	w.lazyInit()
	// Optimized: no lock needed for atomic read
	return int(atomic.LoadInt64(&w.delta))
}

// Reset resets the WaitGroup counter to zero.
// This method should only be called when no other goroutines are calling Add, Done, or Wait.
// Using Reset concurrently with other methods may cause undefined behavior.
func (w *WaitGroup) Reset() {
	w.lazyInit()
	w.s.L.Lock()
	defer w.s.L.Unlock()
	
	// Atomically reset the counter to zero
	oldValue := atomic.SwapInt64(&w.delta, 0)
	atomic.StoreInt64(&w.lastUpdate, time.Now().UnixNano())
	
	// Reset optimization fields
	atomic.StoreInt32(&w.spinLimit, 100)
	
	// If there were waiters, wake them up
	if oldValue > 0 {
		w.s.Broadcast()
	}
}
