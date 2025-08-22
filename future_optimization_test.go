package concurrent

import (
	"sync"
	"testing"
	"time"
)

// TestFutureOptimizationVerification tests all Future optimizations without triggering RoutineService
func TestFutureOptimizationVerification(t *testing.T) {
	t.Run("ListenerPreallocation", testListenerPreallocation)
	t.Run("CompletionOptimization", testCompletionOptimization)
	t.Run("AtomicOptimization", testAtomicOptimization)
	t.Run("ThenChainOptimization", testThenChainOptimization)
	t.Run("ConcurrentSafety", testConcurrentSafety)
}

// testListenerPreallocation verifies listener pre-allocation strategy optimization
func testListenerPreallocation(t *testing.T) {
	future := NewFuture()
	
	// Add multiple listeners to trigger capacity growth
	for i := 0; i < 20; i++ {
		future.AddListener(&optimizationTestListener{id: i})
	}
	
	// Verify future completes successfully with all listeners
	go func() {
		time.Sleep(10 * time.Millisecond)
		future.Completable().Complete("test_result")
	}()
	
	result := future.Get()
	if result != "test_result" {
		t.Errorf("Expected 'test_result', got %v", result)
	}
}

// testCompletionOptimization verifies Complete() method lock optimization
func testCompletionOptimization(t *testing.T) {
	const numFutures = 100
	futures := make([]Future, numFutures)
	
	// Create multiple futures
	for i := 0; i < numFutures; i++ {
		futures[i] = NewFuture()
	}
	
	// Complete all futures concurrently
	var wg sync.WaitGroup
	for i := 0; i < numFutures; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			success := futures[idx].Completable().Complete(idx)
			if !success {
				t.Errorf("Future %d completion failed", idx)
			}
		}(i)
	}
	
	wg.Wait()
	
	// Verify all futures completed correctly
	for i := 0; i < numFutures; i++ {
		if !futures[i].IsDone() {
			t.Errorf("Future %d not completed", i)
		}
		result := futures[i].Get()
		if result != i {
			t.Errorf("Future %d: expected %d, got %v", i, i, result)
		}
	}
}

// testAtomicOptimization verifies atomic operation optimization with double-check locking
func testAtomicOptimization(t *testing.T) {
	future := NewFuture()
	const numListeners = 50
	
	// Complete future immediately
	future.Completable().Complete("fast_result")
	
	// Add listeners to completed future (should execute immediately via fast path)
	var results []string
	var mu sync.Mutex
	
	var wg sync.WaitGroup
	for i := 0; i < numListeners; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			future.AddListener(&callbackListener{
				callback: func(f Future) {
					result := f.Get()
					mu.Lock()
					results = append(results, result.(string))
					mu.Unlock()
				},
			})
		}(i)
	}
	
	wg.Wait()
	
	if len(results) != numListeners {
		t.Errorf("Expected %d results, got %d", numListeners, len(results))
	}
	
	for i, result := range results {
		if result != "fast_result" {
			t.Errorf("Listener %d: expected 'fast_result', got %s", i, result)
		}
	}
}

// testThenChainOptimization verifies Then() chain operation optimization
func testThenChainOptimization(t *testing.T) {
	future := NewFuture()
	
	// Create a chain of Then operations
	chain1 := future.Chainable().Then(func(prev Future) any {
		result := prev.Get()
		return result.(int) * 2
	})
	
	chain2 := chain1.Chainable().Then(func(prev Future) any {
		result := prev.Get()
		return result.(int) + 10
	})
	
	chain3 := chain2.Chainable().Then(func(prev Future) any {
		result := prev.Get()
		return result.(int) * 3
	})
	
	// Complete original future
	go func() {
		time.Sleep(5 * time.Millisecond)
		future.Completable().Complete(5)
	}()
	
	// Verify chain completes correctly: (5 * 2 + 10) * 3 = 60
	result := chain3.Get()
	if result != 60 {
		t.Errorf("Expected 60, got %v", result)
	}
}

// testConcurrentSafety verifies all optimizations maintain concurrent safety
func testConcurrentSafety(t *testing.T) {
	const numGoroutines = 100
	const numOperations = 10
	
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numOperations)
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < numOperations; j++ {
				future := NewFuture()
				expectedValue := id*numOperations + j
				
				// Add listener concurrently
				go func(val int) {
					future.AddListener(&optimizationTestListener{id: val})
				}(expectedValue)
				
				// Complete future concurrently
				go func(val int) {
					time.Sleep(time.Millisecond)
					success := future.Completable().Complete(val)
					if !success {
						errors <- &testError{msg: "completion failed"}
					}
				}(expectedValue)
				
				// Get result
				result := future.Get()
				if result != expectedValue {
					errors <- &testError{msg: "wrong result"}
				}
			}
		}(i)
	}
	
	wg.Wait()
	close(errors)
	
	// Check for any errors
	for err := range errors {
		t.Errorf("Concurrent safety test error: %v", err)
	}
}

// Helper types for testing

type optimizationTestListener struct {
	id int
}

func (l *optimizationTestListener) OperationCompleted(f Future) {
	// Simple listener implementation for testing
}

type callbackListener struct {
	callback func(Future)
}

func (l *callbackListener) OperationCompleted(f Future) {
	if l.callback != nil {
		l.callback(f)
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
