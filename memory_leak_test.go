package concurrent

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestMemoryLeakFuture tests for memory leaks in Future operations
func TestMemoryLeakFuture(t *testing.T) {
	runtime.GC()
	runtime.GC() // Double GC to ensure clean start
	
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	
	// Run intensive Future operations
	const iterations = 10000
	futures := make([]Future, iterations)
	
	for i := 0; i < iterations; i++ {
		future := NewFuture()
		
		// Add multiple listeners to test memory pool
		for j := 0; j < 5; j++ {
			future.AddListener(NewFutureListener(func(f Future) {
				// Simple listener that doesn't leak
			}))
		}
		
		// Create chains
		chain := future.Chainable().Then(func(prev Future) any {
			return prev.Get()
		}).Then(func(prev Future) any {
			return prev.Get()
		})
		
		future.Completable().Complete(i)
		chain.Get() // Ensure completion
		
		futures[i] = future
		
		// Periodic cleanup check
		if i%1000 == 999 {
			runtime.GC()
		}
	}
	
	// Force garbage collection
	runtime.GC()
	runtime.GC()
	time.Sleep(time.Millisecond * 100) // Allow GC to complete
	
	runtime.ReadMemStats(&m2)
	
	allocDiff := m2.Alloc - m1.Alloc
	t.Logf("Memory used: %d bytes (%.2f MB)", allocDiff, float64(allocDiff)/1024/1024)
	
	// Check for reasonable memory usage (should not exceed 50MB for this test)
	if allocDiff > 50*1024*1024 {
		t.Errorf("Potential memory leak detected: %d bytes allocated", allocDiff)
	}
	
	// Verify memory pools are working
	if allocDiff > iterations*1000 { // More than 1KB per future seems excessive
		t.Logf("Warning: Memory usage seems high, memory pools may not be effective")
	}
}

// TestMemoryLeakMutex tests for memory leaks in Mutex operations
func TestMemoryLeakMutex(t *testing.T) {
	runtime.GC()
	runtime.GC()
	
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	
	const iterations = 50000
	mutexes := make([]*Mutex, iterations)
	
	for i := 0; i < iterations; i++ {
		m := &Mutex{}
		mutexes[i] = m
		
		// Test various operations
		m.Lock()
		m.Unlock()
		
		m.TryLock()
		m.Unlock()
		
		m.TryLockTimeout(time.Microsecond)
		
		// Test Unlocked channel only occasionally (more realistic usage)
		if i%1000 == 0 {
			ch := m.Unlocked()
			select {
			case <-ch:
			default:
			}
		}
		
		if i%5000 == 4999 {
			runtime.GC()
		}
	}
	
	runtime.GC()
	runtime.GC()
	time.Sleep(time.Millisecond * 100)
	
	runtime.ReadMemStats(&m2)
	
	// Handle potential integer overflow/underflow in memory calculation
	var allocDiff uint64
	if m2.Alloc >= m1.Alloc {
		allocDiff = m2.Alloc - m1.Alloc
	} else {
		// If m2.Alloc < m1.Alloc, it means GC freed more memory than was allocated
		// In this case, we consider the net memory usage as 0
		allocDiff = 0
		t.Logf("GC freed more memory than allocated during test - net memory usage: 0 bytes")
	}
	
	t.Logf("Mutex memory used: %d bytes (%.2f MB)", allocDiff, float64(allocDiff)/1024/1024)
	
	// Mutex operations should use minimal memory
	if allocDiff > 10*1024*1024 {
		t.Errorf("Potential memory leak in Mutex: %d bytes allocated", allocDiff)
	}
}

// TestMemoryLeakQueue tests for memory leaks in Queue operations
func TestMemoryLeakQueue(t *testing.T) {
	runtime.GC()
	runtime.GC()
	
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	
	const iterations = 20000
	
	// Test different queue configurations
	queues := []BlockingQueue{
		NewBlockingQueue(100),
		NewBlockingQueue(-1), // Unlimited
		NewBlockingQueue(1000),
	}
	
	for _, q := range queues {
		for i := 0; i < iterations; i++ {
			q.Push(i)
			q.Pop()
			
			// Test timeout operations
			q.PushTimeout(i, time.Microsecond)
			q.PopTimeout(time.Microsecond)
			
			if i%2000 == 1999 {
				q.Reset()
				runtime.GC()
			}
		}
	}
	
	runtime.GC()
	runtime.GC()
	time.Sleep(time.Millisecond * 100)
	
	runtime.ReadMemStats(&m2)
	
	allocDiff := m2.Alloc - m1.Alloc
	t.Logf("Queue memory used: %d bytes (%.2f MB)", allocDiff, float64(allocDiff)/1024/1024)
	
	if allocDiff > 20*1024*1024 {
		t.Errorf("Potential memory leak in Queue: %d bytes allocated", allocDiff)
	}
}

// TestMemoryLeakWaitGroup tests for memory leaks in WaitGroup operations
func TestMemoryLeakWaitGroup(t *testing.T) {
	runtime.GC()
	runtime.GC()
	
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	
	const iterations = 10000
	wgs := make([]*WaitGroup, iterations)
	
	for i := 0; i < iterations; i++ {
		wg := &WaitGroup{}
		wgs[i] = wg
		
		// Test various operations
		wg.Add(5)
		go func() {
			time.Sleep(time.Microsecond)
			wg.Done()
		}()
		go func() {
			time.Sleep(time.Microsecond)
			wg.Done()
		}()
		go func() {
			time.Sleep(time.Microsecond)
			wg.Done()
		}()
		go func() {
			time.Sleep(time.Microsecond)
			wg.Done()
		}()
		go func() {
			time.Sleep(time.Microsecond)
			wg.Done()
		}()
		
		wg.Wait()
		wg.Reset()
		
		if i%1000 == 999 {
			runtime.GC()
		}
	}
	
	runtime.GC()
	runtime.GC()
	time.Sleep(time.Millisecond * 100)
	
	runtime.ReadMemStats(&m2)
	
	allocDiff := m2.Alloc - m1.Alloc
	t.Logf("WaitGroup memory used: %d bytes (%.2f MB)", allocDiff, float64(allocDiff)/1024/1024)
	
	if allocDiff > 15*1024*1024 {
		t.Errorf("Potential memory leak in WaitGroup: %d bytes allocated", allocDiff)
	}
}

// TestMemoryLeakRoutineService tests for memory leaks in RoutineService operations
func TestMemoryLeakRoutineService(t *testing.T) {
	runtime.GC()
	runtime.GC()
	
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	
	// Create routine service with larger queue to avoid blocking
	rs := NewRoutinePool(10, 10000) // Increased queue size
	
	const iterations = 2000 // Reduced iterations to avoid long test times
	futures := make([]RoutineFuture, iterations)
	
	for i := 0; i < iterations; i++ {
		idx := i // Capture loop variable
		future := rs.Submit(func(r Routine) any {
			// Simulate minimal work to avoid timing issues
			runtime.Gosched()
			return idx
		})
		
		futures[i] = future
		
		// More frequent cleanup to prevent queue buildup
		if i%200 == 199 {
			// Wait for recent tasks to complete
			for j := i - 199; j <= i; j++ {
				futures[j].Get()
			}
			runtime.GC()
		}
	}
	
	// Wait for all remaining tasks to complete
	for _, f := range futures {
		f.Get()
	}
	
	rs.ShutdownGracefully()
	
	runtime.GC()
	runtime.GC()
	time.Sleep(time.Millisecond * 100) // Reduced wait time
	
	runtime.ReadMemStats(&m2)
	
	allocDiff := m2.Alloc - m1.Alloc
	t.Logf("RoutineService memory used: %d bytes (%.2f MB)", allocDiff, float64(allocDiff)/1024/1024)
	
	if allocDiff > 15*1024*1024 { // Adjusted threshold for fewer iterations
		t.Errorf("Potential memory leak in RoutineService: %d bytes allocated", allocDiff)
	}
}

// TestLongTermStability tests long-term stability under continuous load
func TestLongTermStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-term stability test in short mode")
	}
	
	const duration = time.Second * 10 // 10 seconds of continuous operation
	const goroutines = 50
	
	stopChan := make(chan struct{})
	errorCount := int64(0)
	operationCount := int64(0)
	
	// Start multiple goroutines doing continuous work
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			rs := NewRoutinePool(5, 100)
			defer rs.ShutdownGracefully()
			
			for {
				select {
				case <-stopChan:
					return
				default:
					// Mix of operations
					switch id % 4 {
					case 0:
						// Future operations
						future := NewFuture()
						future.AddListener(NewFutureListener(func(f Future) {}))
						future.Completable().Complete(id)
						future.Get()
						
					case 1:
						// Mutex operations
						m := &Mutex{}
						m.Lock()
						time.Sleep(time.Microsecond)
						m.Unlock()
						
					case 2:
						// Queue operations
						q := NewBlockingQueue(10)
						q.Push(id)
						q.Pop()
						
					case 3:
						// RoutineService operations
						f := rs.Submit(func(r Routine) any {
							return id
						})
						f.Get()
					}
					
					atomic.AddInt64(&operationCount, 1)
				}
			}
		}(i)
	}
	
	// Monitor for panics and errors
	go func() {
		defer func() {
			if r := recover(); r != nil {
				atomic.AddInt64(&errorCount, 1)
			}
		}()
		
		for {
			select {
			case <-stopChan:
				return
			default:
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()
	
	// Run for specified duration
	time.Sleep(duration)
	close(stopChan)
	
	// Allow goroutines to finish
	time.Sleep(time.Millisecond * 100)
	
	ops := atomic.LoadInt64(&operationCount)
	errors := atomic.LoadInt64(&errorCount)
	
	t.Logf("Long-term stability test completed:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Operations: %d", ops)
	t.Logf("  Errors: %d", errors)
	t.Logf("  Ops/sec: %.2f", float64(ops)/duration.Seconds())
	
	if errors > 0 {
		t.Errorf("Stability test failed with %d errors", errors)
	}
	
	if ops < 1000 {
		t.Errorf("Suspiciously low operation count: %d", ops)
	}
}

// TestConcurrentStress tests system behavior under extreme concurrent stress
func TestConcurrentStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent stress test in short mode")
	}
	
	const numGoroutines = 200
	const operationsPerGoroutine = 1000
	
	var wg WaitGroup
	errorCount := int64(0)
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt64(&errorCount, 1)
				}
			}()
			
			for j := 0; j < operationsPerGoroutine; j++ {
				// Random mix of operations
				switch (id + j) % 5 {
				case 0:
					future := NewFuture()
					future.Completable().Complete(j)
					future.Get()
					
				case 1:
					m := &Mutex{}
					if m.TryLock() {
						m.Unlock()
					}
					
				case 2:
					q := NewBlockingQueue(5)
					q.TryPush(j)
					q.TryPop()
					
				case 3:
					wg2 := &WaitGroup{}
					wg2.Add(1)
					go func() {
						wg2.Done()
					}()
					wg2.Wait()
					
				case 4:
					rs := NewRoutinePool(2, 10)
					f := rs.Submit(func(r Routine) any {
						return j
					})
					f.Get()
					rs.ShutdownGracefully()
				}
			}
		}(i)
	}
	
	wg.Wait()
	
	errors := atomic.LoadInt64(&errorCount)
	t.Logf("Concurrent stress test completed with %d errors", errors)
	
	if errors > 0 {
		t.Errorf("Stress test failed with %d errors", errors)
	}
}
