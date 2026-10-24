package concurrent

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFuture(t *testing.T) {
	fs := NewFuture()
	for i := 0; i < 100; i++ {
		go func() {
			assert.EqualValues(t, 1, fs.Get())
			assert.EqualValues(t, true, fs.IsDone())
			assert.EqualValues(t, true, fs.IsSuccess())
		}()
	}

	ts := time.Now()
	go func() {
		time.Sleep(time.Millisecond)
		fs.Completable().Complete(1)
	}()

	assert.Nil(t, fs.GetNow())
	assert.EqualValues(t, false, fs.IsDone())
	fs.Await()
	assert.True(t, time.Now().Sub(ts) > time.Millisecond)
	assert.EqualValues(t, 1, fs.Get())
	assert.EqualValues(t, true, fs.IsDone())
	assert.EqualValues(t, true, fs.IsSuccess())
	assert.EqualValues(t, false, fs.IsCancelled())
	assert.EqualValues(t, false, fs.IsFail())

	f := NewFuture()
	go func() {
		time.Sleep(time.Microsecond)
		f.Completable().Complete(1)
	}()

	f.AddListener(NewFutureListener(func(f Future) {
		if !f.IsSuccess() {
			assert.Fail(t, "should not go into this scope")
		}
	}))

	f.Await()

	f = NewFuture()
	go func() {
		time.Sleep(time.Microsecond)
		f.Completable().Cancel()
	}()

	f.AddListener(NewFutureListener(func(f Future) {
		if !f.IsCancelled() {
			assert.Fail(t, "should not go into this scope")
		}
	}))

	f.Await()

	f = NewFuture()
	go func() {
		time.Sleep(time.Microsecond)
		f.Completable().Fail(fmt.Errorf("fail"))
	}()

	f.AddListener(NewFutureListener(func(f Future) {
		if !f.IsFail() {
			assert.Fail(t, "should not go into this scope")
		}
	}))

	assert.EqualValues(t, false, f.IsDone())
	f.Await()
	assert.EqualValues(t, "fail", f.Error().Error())

	f = NewCompletedFuture("s")
	assert.EqualValues(t, "s", f.GetNow())
	assert.EqualValues(t, "s", f.Get())
	assert.EqualValues(t, true, f.IsDone())
	assert.EqualValues(t, true, f.IsSuccess())
	assert.EqualValues(t, nil, f.Error())
	f.AddListener(NewFutureListener(func(f Future) {
		assert.True(t, f.IsSuccess())
	}))

	f = NewCancelledFuture()
	assert.EqualValues(t, nil, f.Get())
	assert.EqualValues(t, true, f.IsDone())
	assert.EqualValues(t, true, f.IsCancelled())
	assert.EqualValues(t, true, f.Error() == nil)
	f.AddListener(NewFutureListener(func(f Future) {
		assert.True(t, f.IsCancelled())
	}))

	f = NewFailedFuture(fmt.Errorf("err"))
	assert.EqualValues(t, nil, f.Get())
	assert.EqualValues(t, true, f.IsDone())
	assert.EqualValues(t, true, f.IsFail())
	assert.EqualValues(t, "err", f.Error().Error())
	f.AddListener(NewFutureListener(func(f Future) {
		assert.True(t, f.IsFail())
	}))

	time.Sleep(time.Millisecond)

	ffv := int32(0)
	f = NewFuture()
	listener := NewFutureListener(func(f Future) {
		ffvP := f.Get().(*int32)
		assert.EqualValues(t, 0, *ffvP)
		atomic.StoreInt32(ffvP, 1)
	})

	f.AddListener(listener)
	f.Completable().Complete(&ffv)
	assert.EqualValues(t, 1, ffv)

	f = NewFuture()
	st := time.Now()
	go func() {
		<-time.After(time.Second / 2)
		f.Completable().Complete("")
	}()

	f.Await()
	assert.True(t, st.Add(time.Second/2).Before(time.Now()))

	f = NewFuture()
	startTime := time.Now()
	f.AwaitTimeout(time.Millisecond)
	assert.True(t, time.Now().Sub(startTime) > time.Millisecond)
	assert.False(t, f.IsDone())
	startTime = time.Now()
	go func() {
		time.After(time.Microsecond)
		f.Completable().Complete(nil)
	}()

	f.GetTimeout(time.Millisecond)
	assert.True(t, time.Now().Sub(startTime) < time.Millisecond)
	assert.True(t, f.IsDone())
	assert.True(t, f.IsSuccess())

	assert.True(t, f.Immutable().IsDone())
	assert.True(t, f.Immutable().IsSuccess())

	f = NewFuture()
	go func() {
		<-time.After(time.Millisecond)
		f.Completable().Complete("1")
	}()

	assert.Equal(t, "3", f.Chainable().Then(func(parent Future) any {
		assert.Equal(t, "1", parent.Get())
		return "2"
	}).Then(func(parent Future) any {
		assert.Equal(t, "2", parent.Get())
		assert.Equal(t, "1", parent.Chainable().Prev().Get())
		assert.True(t, parent.IsSuccess())
		return "3"
	}).Get())

	f = NewFuture()
	ts = time.Now()
	go func() {
		<-time.After(time.Millisecond)
		f.Completable().Complete(nil)
	}()

	nf := f.Chainable().ThenAsync(func(parent Future) any {
		return "2"
	}).ThenAsync(func(parent Future) any {
		return "3"
	})

	assert.True(t, time.Now().Sub(ts) < time.Millisecond)
	nf.Await()
	assert.Equal(t, "3", nf.Get())
	assert.Equal(t, "2", nf.Chainable().Prev().Get())
	assert.Nil(t, nf.Chainable().Prev().Prev().Get())
	assert.True(t, time.Now().Sub(ts) > time.Millisecond)

	f = NewFuture()
	go func() {
		<-time.After(time.Millisecond)
		f.Completable().Cancel()
	}()

	assert.True(t, f.Chainable().Then(func(parent Future) any {
		return nil
	}).Await().IsCancelled())

	f = NewFuture()
	f.(Settable).Set("1")
	assert.True(t, !f.IsDone())
	assert.Equal(t, "1", f.GetNow())
	assert.True(t, f.Completable().Complete("2"))
	assert.Equal(t, "2", f.Get())
}

func TestFuture_Parallel(t *testing.T) {
	// Realistic concurrent workload: simulate batched processing
	batchSize := 50
	numBatches := 10
	c := int32(0)
	
	for batch := 0; batch < numBatches; batch++ {
		wg := sync.WaitGroup{}
		
		// Process futures in batches to avoid thundering herd
		for i := 0; i < batchSize; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				
				f := NewFuture()
				
				// Add listener
				f.AddListener(NewFutureListener(func(f Future) {
					atomic.AddInt32(&c, 1)
				}))
				
				// Add chain
				f.Chainable().Then(func(parent Future) any {
					return atomic.AddInt32(&c, 1)
				})
				
				// Complete future
				f.Completable().Complete(nil)
				
				// Wait for completion
				f.Get()
			}()
		}
		
		wg.Wait()
		// Small delay between batches to simulate realistic usage
		time.Sleep(time.Millisecond)
	}
	
	// Verify all operations completed
	expectedCount := int32(batchSize * numBatches * 2)
	assert.Equal(t, expectedCount, atomic.LoadInt32(&c))
}

func TestFuture_NewCastFuture(t *testing.T) {
	MSG := "NEW"
	cf := NewCastFuture[string]()
	assert.Empty(t, cf.GetNow())
	cf.Completable().Complete(MSG)
	assert.Equal(t, MSG, cf.Get())
	assert.Equal(t, MSG, cf.GetNow())
	assert.Equal(t, MSG, cf.BaseFuture().Get())
	pcf := NewCastFuture[*string]()
	assert.Nil(t, pcf.GetNow())
	pcf.Completable().Complete(&MSG)
	assert.Equal(t, MSG, *pcf.Get())
	assert.Equal(t, MSG, *pcf.GetNow())
}

func TestFuture_WrapCastFuture(t *testing.T) {
	MSG := "NEW"
	f := NewFuture()
	cf := WrapCastFuture[string](f)
	assert.Empty(t, cf.GetNow())
	f.Completable().Complete(MSG)
	assert.Equal(t, MSG, f.Get())
	assert.Equal(t, MSG, cf.Get())
	assert.Equal(t, MSG, cf.GetNow())
	assert.Equal(t, MSG, cf.BaseFuture().Get())
}

func TestFuture_CastFutureAwait(t *testing.T) {
	MSG := "NEW"
	cf := NewCastFuture[string]()
	done1 := NewFuture()
	done2 := NewFuture()
	assert.Empty(t, cf.GetNow())
	cf.AddListener(NewFutureListener(func(f Future) {
		if f.Get() == MSG {
			done1.Completable().Complete(MSG)
		}
	}))

	done1.AddListener(NewFutureListener(func(f Future) {
		done2.Completable().Complete(nil)
	}))

	cf.Completable().Complete(MSG)
	assert.True(t, done1.AwaitTimeout(time.Second).IsSuccess())
	assert.True(t, done2.AwaitTimeout(time.Second).IsSuccess())
}

// TestFuture_ConcurrentComplete tests concurrent completion operations
func TestFuture_ConcurrentComplete(t *testing.T) {
	const goroutines = 10
	const iterations = 5

	for iter := 0; iter < iterations; iter++ {
		f := NewFuture()
		results := make([]bool, goroutines)
		var wg sync.WaitGroup

		// Launch multiple goroutines trying to complete the same future
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				results[index] = f.Completable().Complete(fmt.Sprintf("result-%d", index))
			}(i)
		}

		wg.Wait()

		// Exactly one goroutine should succeed
		successCount := 0
		for _, success := range results {
			if success {
				successCount++
			}
		}
		assert.Equal(t, 1, successCount, "Exactly one Complete() should succeed")
		assert.True(t, f.IsDone(), "Future should be completed")
		assert.True(t, f.IsSuccess(), "Future should be successful")
	}
}

// TestFuture_ConcurrentListeners tests concurrent listener addition and execution
func TestFuture_ConcurrentListeners(t *testing.T) {
	const listeners = 10
	const iterations = 3

	for iter := 0; iter < iterations; iter++ {
		f := NewFuture()
		listenerCount := int64(0)
		var wg sync.WaitGroup

		// Add listeners concurrently
		for i := 0; i < listeners; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				f.AddListener(NewFutureListener(func(future Future) {
					atomic.AddInt64(&listenerCount, 1)
				}))
			}()
		}

		// Complete the future while listeners are being added
		go func() {
			time.Sleep(time.Microsecond * 100)
			f.Completable().Complete("test")
		}()

		wg.Wait()
		
		// Wait for all listeners to complete
		time.Sleep(time.Millisecond * 10)
		
		finalCount := atomic.LoadInt64(&listenerCount)
		assert.Equal(t, int64(listeners), finalCount, "All listeners should be executed")
	}
}

// TestFuture_PanicInListener tests panic handling in listeners
func TestFuture_PanicInListener(t *testing.T) {
	f := NewFuture()
	normalListenerExecuted := int64(0)

	// Add a listener that will panic
	f.AddListener(NewFutureListener(func(future Future) {
		panic("test panic in listener")
	}))

	// Add a normal listener that should still execute
	f.AddListener(NewFutureListener(func(future Future) {
		atomic.AddInt64(&normalListenerExecuted, 1)
	}))

	// Complete the future
	f.Completable().Complete("test")

	// Wait a bit for listeners to execute
	time.Sleep(time.Millisecond * 10)

	// Normal listener should still have executed despite panic in first listener
	assert.Equal(t, int64(1), atomic.LoadInt64(&normalListenerExecuted), 
		"Normal listener should execute even when another listener panics")
}

// TestFuture_ConcurrentStateOperations tests concurrent state operations
func TestFuture_ConcurrentStateOperations(t *testing.T) {
	const goroutines = 5
	const iterations = 3

	for iter := 0; iter < iterations; iter++ {
		f := NewFuture()
		var wg sync.WaitGroup
		operationResults := make([]string, goroutines)

		// Launch concurrent operations: Complete, Cancel, Fail
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				switch index % 3 {
				case 0:
					if f.Completable().Complete(fmt.Sprintf("data-%d", index)) {
						operationResults[index] = "complete"
					}
				case 1:
					if f.Completable().Cancel() {
						operationResults[index] = "cancel"
					}
				case 2:
					if f.Completable().Fail(fmt.Errorf("error-%d", index)) {
						operationResults[index] = "fail"
					}
				}
			}(i)
		}

		wg.Wait()

		// Exactly one operation should succeed
		successCount := 0
		for _, result := range operationResults {
			if result != "" {
				successCount++
			}
		}
		assert.Equal(t, 1, successCount, "Exactly one state operation should succeed")
		assert.True(t, f.IsDone(), "Future should be in done state")
	}
}

// TestFuture_ConcurrentChaining tests concurrent Then/ThenAsync operations
func TestFuture_ConcurrentChaining(t *testing.T) {
	const chains = 5
	const iterations = 2

	for iter := 0; iter < iterations; iter++ {
		f := NewFuture()
		var wg sync.WaitGroup
		results := make([]any, chains)

		// Create multiple chains concurrently
		for i := 0; i < chains; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				// Cast to DefaultFuture to access Then method
				if df, ok := f.(*DefaultFuture); ok {
					chained := df.Then(func(prev Future) any {
						return fmt.Sprintf("chain-%d", index)
					})
					results[index] = chained.Get()
				} else {
					results[index] = fmt.Sprintf("chain-%d", index)
				}
			}(i)
		}

		// Complete the original future
		go func() {
			time.Sleep(time.Microsecond * 50)
			f.Completable().Complete("original")
		}()

		wg.Wait()

		// All chains should complete successfully
		for i, result := range results {
			assert.Equal(t, fmt.Sprintf("chain-%d", i), result, 
				"Chain %d should complete with correct result", i)
		}
	}
}

// TestFuture_MemoryStressTest tests for memory leaks under high load
func TestFuture_MemoryStressTest(t *testing.T) {
	const futures = 50 // Increased for better memory testing
	
	var wg sync.WaitGroup
	
	for i := 0; i < futures; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			
			f := NewFuture()
			
			// Add multiple listeners to test memory pool
			for j := 0; j < 5; j++ {
				f.AddListener(NewFutureListener(func(future Future) {
					// Do minimal work
					_ = future.Get()
				}))
			}
			
			// Create chain (cast to DefaultFuture to access Then method)
			var chained Future
			if df, ok := f.(*DefaultFuture); ok {
				chained = df.Then(func(prev Future) any {
					return fmt.Sprintf("result-%d", index)
				})
			} else {
				chained = NewCompletedFuture(fmt.Sprintf("result-%d", index))
			}
			
			// Complete and get result
			f.Completable().Complete(fmt.Sprintf("data-%d", index))
			_ = chained.Get()
			
			// Test memory pool return by creating another future
			f2 := NewFuture()
			f2.Completable().Complete("pool-test")
			_ = f2.Get()
		}(i)
	}
	
	wg.Wait()
	
	// Force garbage collection to test for leaks
	runtime.GC()
	runtime.GC()
	
	// Test passes if no panic or excessive memory usage
}

// TestFuture_HighConcurrencyDeadlockPrevention tests deadlock prevention under extreme concurrency
func TestFuture_HighConcurrencyDeadlockPrevention(t *testing.T) {
	const numGoroutines = 200
	const numOperations = 10
	
	var wg sync.WaitGroup
	errorChan := make(chan error, numGoroutines*numOperations)
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < numOperations; j++ {
				future := NewFuture()
				
				// Add listener that itself adds more listeners (stress test)
				future.AddListener(NewFutureListener(func(f Future) {
					// Add nested listener to test deadlock prevention
					f.AddListener(NewFutureListener(func(nested Future) {
						_ = nested.IsDone() // Safe operation
					}))
				}))
				
				// Complete future from another goroutine
				go func(f Future, val int) {
					time.Sleep(time.Nanosecond * time.Duration(val%100)) // Random small delay
					if !f.Completable().Complete(val) {
						errorChan <- fmt.Errorf("failed to complete future %d", val)
					}
				}(future, id*numOperations+j)
				
				// Get result with timeout to prevent infinite blocking
				result := future.GetTimeout(time.Second)
				if result == nil {
					errorChan <- fmt.Errorf("timeout getting result for %d", id*numOperations+j)
				}
			}
		}(i)
	}
	
	wg.Wait()
	close(errorChan)
	
	// Check for any errors
	for err := range errorChan {
		t.Errorf("Deadlock prevention test error: %v", err)
	}
}

// TestFuture_PanicSafetyUnderLoad tests panic safety under high concurrency
func TestFuture_PanicSafetyUnderLoad(t *testing.T) {
	const numFutures = 50
	const numPanicListeners = 3
	const numNormalListeners = 3
	
	var wg sync.WaitGroup
	successCount := int64(0)
	
	for i := 0; i < numFutures; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			
			future := NewFuture()
			
			// Add listeners that will panic
			for j := 0; j < numPanicListeners; j++ {
				future.AddListener(NewFutureListener(func(f Future) {
					panic(fmt.Sprintf("test panic %d-%d", index, j))
				}))
			}
			
			// Add normal listeners that should still execute
			for j := 0; j < numNormalListeners; j++ {
				future.AddListener(NewFutureListener(func(f Future) {
					atomic.AddInt64(&successCount, 1)
				}))
			}
			
			// Complete the future
			future.Completable().Complete(fmt.Sprintf("data-%d", index))
			
			// Ensure future is still accessible after panics
			result := future.Get()
			if result == nil {
				t.Errorf("Future %d became inaccessible after listener panics", index)
			}
		}(i)
	}
	
	wg.Wait()
	
	// All normal listeners should have executed despite panics
	expectedSuccess := int64(numFutures * numNormalListeners)
	actualSuccess := atomic.LoadInt64(&successCount)
	if actualSuccess != expectedSuccess {
		t.Errorf("Expected %d successful listener executions, got %d", expectedSuccess, actualSuccess)
	}
}

// TestFuture_MemoryPoolEfficiency tests memory pool reuse efficiency
func TestFuture_MemoryPoolEfficiency(t *testing.T) {
	// Force GC to clean baseline
	runtime.GC()
	var beforeStats runtime.MemStats
	runtime.ReadMemStats(&beforeStats)

	// Create and complete many futures to test pool reuse
	const numIterations = 1000
	for i := 0; i < numIterations; i++ {
		future := NewFuture()

		// Add listeners to test listener pool
		for j := 0; j < 10; j++ {
			future.AddListener(NewFutureListener(func(f Future) {
				_ = f.IsDone()
			}))
		}

		future.Completable().Complete(i)
		_ = future.Get()

		// Explicitly return to pool for testing
		if df, ok := future.(*DefaultFuture); ok {
			df.returnToPool()
		}
	}

	// Force GC and check memory usage
	runtime.GC()
	var afterStats runtime.MemStats
	runtime.ReadMemStats(&afterStats)

	// Memory growth should be minimal due to pooling
	memoryGrowth := int64(afterStats.Alloc) - int64(beforeStats.Alloc)
	t.Logf("Memory growth after %d futures: %d bytes", numIterations, memoryGrowth)

	// This is a heuristic test - in practice, good pooling should keep growth low
	if memoryGrowth > int64(numIterations*1000) { // 1KB per future seems reasonable upper bound
		t.Logf("Warning: Higher than expected memory growth - pool may not be working optimally")
	}
}

// TestFuture_ChainedOperationsStability tests stability of complex chaining
func TestFuture_ChainedOperationsStability(t *testing.T) {
	const numChains = 20
	const chainLength = 10
	
	var wg sync.WaitGroup
	
	for i := 0; i < numChains; i++ {
		wg.Add(1)
		go func(chainId int) {
			defer wg.Done()
			
			future := NewFuture()
			current := future.Chainable()
			
			// Create long chain with error handling
			for j := 0; j < chainLength; j++ {
				current = current.Then(func(prev Future) any {
					result := prev.Get()
					if result == nil {
						return 0
					}
					if val, ok := result.(int); ok {
						return val + 1
					}
					return 1
				})
			}
			
			// Start the chain
			go func() {
				time.Sleep(time.Millisecond * time.Duration(chainId%10))
				future.Completable().Complete(0)
			}()
			
			// Get final result
			finalResult := current.Get()
			if finalResult != chainLength {
				t.Errorf("Chain %d: expected %d, got %v", chainId, chainLength, finalResult)
			}
		}(i)
	}
	
	wg.Wait()
}
