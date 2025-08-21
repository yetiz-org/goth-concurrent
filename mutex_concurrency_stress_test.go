package concurrent

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestMutex_HighContentionCorrectness tests mutex correctness under extreme contention
func TestMutex_HighContentionCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high contention test in short mode")
	}

	numThreads := runtime.NumCPU() * 4 // High contention scenario
	iterations := 1000
	sharedCounter := int64(0)
	
	var mutex Mutex
	var wg sync.WaitGroup
	
	// Test function that increments shared counter under mutex protection
	testFunc := func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			mutex.Lock()
			// Critical section - increment shared counter
			temp := atomic.LoadInt64(&sharedCounter)
			runtime.Gosched() // Force potential race condition
			atomic.StoreInt64(&sharedCounter, temp+1)
			mutex.Unlock()
		}
	}
	
	// Launch concurrent goroutines
	wg.Add(numThreads)
	start := time.Now()
	
	for i := 0; i < numThreads; i++ {
		go testFunc()
	}
	
	wg.Wait()
	duration := time.Since(start)
	
	expectedValue := int64(numThreads * iterations)
	actualValue := atomic.LoadInt64(&sharedCounter)
	
	t.Logf("High contention test completed in %v", duration)
	t.Logf("Threads: %d, Iterations per thread: %d", numThreads, iterations)
	t.Logf("Expected: %d, Actual: %d", expectedValue, actualValue)
	
	assert.Equal(t, expectedValue, actualValue, "Shared counter should equal expected value under high contention")
}

// TestMutex_TryLockContentionBehavior tests TryLock behavior under contention
func TestMutex_TryLockContentionBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TryLock contention test in short mode")
	}

	numThreads := runtime.NumCPU() * 2
	testDuration := time.Second * 2
	
	var mutex Mutex
	var wg sync.WaitGroup
	
	successCount := int64(0)
	failureCount := int64(0)
	
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()
	
	// Test function for TryLock contention
	testFunc := func() {
		defer wg.Done()
		
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if mutex.TryLock() {
					atomic.AddInt64(&successCount, 1)
					// Hold lock for a short time to create contention
					time.Sleep(time.Microsecond * 10)
					mutex.Unlock()
				} else {
					atomic.AddInt64(&failureCount, 1)
				}
				runtime.Gosched()
			}
		}
	}
	
	// Launch concurrent goroutines
	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go testFunc()
	}
	
	wg.Wait()
	
	totalAttempts := atomic.LoadInt64(&successCount) + atomic.LoadInt64(&failureCount)
	successRate := float64(atomic.LoadInt64(&successCount)) / float64(totalAttempts) * 100
	
	t.Logf("TryLock contention test results:")
	t.Logf("Threads: %d, Duration: %v", numThreads, testDuration)
	t.Logf("Total attempts: %d", totalAttempts)
	t.Logf("Successes: %d, Failures: %d", atomic.LoadInt64(&successCount), atomic.LoadInt64(&failureCount))
	t.Logf("Success rate: %.2f%%", successRate)
	
	// Verify that both successes and failures occurred (indicating contention)
	assert.Greater(t, atomic.LoadInt64(&successCount), int64(0), "Should have some successful TryLock calls")
	assert.Greater(t, atomic.LoadInt64(&failureCount), int64(0), "Should have some failed TryLock calls due to contention")
	assert.Greater(t, totalAttempts, int64(1000), "Should have significant number of attempts")
}

// TestMutex_TryLockTimeoutAccuracy tests TryLockTimeout accuracy under contention
func TestMutex_TryLockTimeoutAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TryLockTimeout accuracy test in short mode")
	}

	var mutex Mutex
	timeout := time.Millisecond * 50
	
	// Lock the mutex in main thread
	mutex.Lock()
	defer mutex.Unlock()
	
	numThreads := runtime.NumCPU()
	var wg sync.WaitGroup
	timeoutResults := make([]time.Duration, numThreads)
	
	// Test function for timeout accuracy
	testFunc := func(index int) {
		defer wg.Done()
		
		start := time.Now()
		acquired := mutex.TryLockTimeout(timeout)
		elapsed := time.Since(start)
		
		timeoutResults[index] = elapsed
		
		// Should not acquire lock since it's held by main thread
		assert.False(t, acquired, "Should not acquire locked mutex within timeout")
	}
	
	// Launch concurrent timeout attempts
	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go testFunc(i)
	}
	
	wg.Wait()
	
	// Analyze timeout accuracy
	tolerancePercent := 20.0 // 20% tolerance for timing variations
	tolerance := time.Duration(float64(timeout) * tolerancePercent / 100)
	
	for i, elapsed := range timeoutResults {
		t.Logf("Thread %d timeout: %v (target: %v)", i, elapsed, timeout)
		
		// Verify timeout is within reasonable bounds
		assert.True(t, elapsed >= timeout-tolerance && elapsed <= timeout+time.Millisecond*20,
			"Timeout should be reasonably accurate (elapsed: %v, target: %v)", elapsed, timeout)
	}
}

// TestMutex_UnlockedChannelBehavior tests Unlocked() channel behavior under contention
func TestMutex_UnlockedChannelBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Unlocked channel test in short mode")
	}

	var mutex Mutex
	numWaiters := runtime.NumCPU() * 2
	
	// Lock the mutex initially
	mutex.Lock()
	
	var wg sync.WaitGroup
	notifications := int64(0)
	
	// Create multiple goroutines waiting for unlock
	testFunc := func() {
		defer wg.Done()
		
		unlockedCh := mutex.Unlocked()
		
		select {
		case <-unlockedCh:
			atomic.AddInt64(&notifications, 1)
		case <-time.After(time.Second * 5):
			t.Error("Timeout waiting for unlock notification")
		}
	}
	
	// Launch waiters
	wg.Add(numWaiters)
	for i := 0; i < numWaiters; i++ {
		go testFunc()
	}
	
	// Wait a bit to ensure all waiters are blocked
	time.Sleep(time.Millisecond * 100)
	
	// Unlock the mutex
	unlockTime := time.Now()
	mutex.Unlock()
	
	// Wait for all notifications
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// Success
	case <-time.After(time.Second * 10):
		t.Fatal("Timeout waiting for all waiters to be notified")
	}
	
	elapsedSinceUnlock := time.Since(unlockTime)
	
	t.Logf("All %d waiters notified in %v after unlock", numWaiters, elapsedSinceUnlock)
	
	// Verify all waiters were notified
	assert.Equal(t, int64(numWaiters), atomic.LoadInt64(&notifications), 
		"All waiters should be notified when mutex is unlocked")
	
	// Verify reasonable notification timing
	assert.Less(t, elapsedSinceUnlock, time.Second, 
		"All notifications should complete quickly after unlock")
}

// TestMutex_MixedOperationsStress tests mixed lock operations under stress
func TestMutex_MixedOperationsStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping mixed operations stress test in short mode")
	}

	testDuration := time.Second * 3
	numWorkers := runtime.NumCPU()
	
	var mutex Mutex
	var wg sync.WaitGroup
	
	// Counters for different operations
	lockCount := int64(0)
	tryLockSuccessCount := int64(0)
	tryLockFailCount := int64(0)
	timeoutSuccessCount := int64(0)
	timeoutFailCount := int64(0)
	
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()
	
	// Worker function with mixed operations
	worker := func(workerID int) {
		defer wg.Done()
		
		for {
			select {
			case <-ctx.Done():
				return
			default:
				operation := workerID % 4
				
				switch operation {
				case 0: // Regular Lock/Unlock
					mutex.Lock()
					atomic.AddInt64(&lockCount, 1)
					time.Sleep(time.Microsecond * 5) // Brief critical section
					mutex.Unlock()
					
				case 1: // TryLock
					if mutex.TryLock() {
						atomic.AddInt64(&tryLockSuccessCount, 1)
						time.Sleep(time.Microsecond * 3)
						mutex.Unlock()
					} else {
						atomic.AddInt64(&tryLockFailCount, 1)
					}
					
				case 2: // TryLockTimeout short
					if mutex.TryLockTimeout(time.Microsecond * 10) {
						atomic.AddInt64(&timeoutSuccessCount, 1)
						time.Sleep(time.Microsecond * 2)
						mutex.Unlock()
					} else {
						atomic.AddInt64(&timeoutFailCount, 1)
					}
					
				case 3: // Test Unlocked channel (non-blocking)
					unlockedCh := mutex.Unlocked()
					select {
					case <-unlockedCh:
						// Mutex was already unlocked
					default:
						// Mutex is currently locked
					}
				}
				
				runtime.Gosched()
			}
		}
	}
	
	// Launch workers
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker(i)
	}
	
	wg.Wait()
	
	// Report stress test results
	totalOps := atomic.LoadInt64(&lockCount) + 
		atomic.LoadInt64(&tryLockSuccessCount) + atomic.LoadInt64(&tryLockFailCount) +
		atomic.LoadInt64(&timeoutSuccessCount) + atomic.LoadInt64(&timeoutFailCount)
	
	t.Logf("Mixed operations stress test results (duration: %v):", testDuration)
	t.Logf("Workers: %d, Total operations: %d", numWorkers, totalOps)
	t.Logf("Lock operations: %d", atomic.LoadInt64(&lockCount))
	t.Logf("TryLock success: %d, fail: %d", 
		atomic.LoadInt64(&tryLockSuccessCount), atomic.LoadInt64(&tryLockFailCount))
	t.Logf("TryLockTimeout success: %d, fail: %d", 
		atomic.LoadInt64(&timeoutSuccessCount), atomic.LoadInt64(&timeoutFailCount))
	
	// Verify significant activity occurred
	assert.Greater(t, totalOps, int64(1000), "Should have significant operation count")
	assert.Greater(t, atomic.LoadInt64(&lockCount), int64(10), "Should have regular lock operations")
	
	// Verify mutex is in consistent state (unlocked) at the end
	assert.False(t, mutex.IsLocked(), "Mutex should be unlocked after stress test")
}

// TestMutex_PanicSafety tests mutex behavior during panics
func TestMutex_PanicSafety(t *testing.T) {
	var mutex Mutex
	
	// Test that panic in critical section doesn't leave mutex in bad state
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Expected panic
			}
		}()
		
		mutex.Lock()
		panic("simulated panic in critical section")
	}()
	
	// Mutex should still be locked after panic
	assert.True(t, mutex.IsLocked(), "Mutex should remain locked after panic")
	
	// Should be able to unlock manually
	mutex.Unlock()
	assert.False(t, mutex.IsLocked(), "Mutex should be unlocked after manual unlock")
	
	// Should be able to lock again normally
	assert.True(t, mutex.TryLock(), "Should be able to lock again after panic recovery")
	mutex.Unlock()
}
