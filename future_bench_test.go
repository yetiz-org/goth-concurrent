package concurrent

import (
	"testing"
	"time"
)

// BenchmarkFutureComplete tests basic completion performance
func BenchmarkFutureComplete(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future := NewFuture()
			future.Completable().Complete("test")
			future.Get()
		}
	})
}

// BenchmarkFutureCompleteWithListeners tests completion with listeners
func BenchmarkFutureCompleteWithListeners(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future := NewFuture()
			
			// Add 5 listeners (typical real-world scenario)
			for i := 0; i < 5; i++ {
				future.AddListener(&testListener{})
			}
			
			future.Completable().Complete("test")
			future.Get()
		}
	})
}

// BenchmarkFutureChaining tests future chaining performance
func BenchmarkFutureChaining(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future1 := NewFuture()
			future2 := future1.Chainable().Then(func(prev Future) any {
				return prev.Get().(string) + "_chained"
			})
			
			future1.Completable().Complete("test")
			future2.Get()
		}
	})
}

// BenchmarkFutureTimeout tests timeout operations
func BenchmarkFutureTimeout(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future := NewFuture()
			future.Completable().Complete("test")
			future.GetTimeout(time.Millisecond)
		}
	})
}

// BenchmarkFutureCancel tests cancellation performance
func BenchmarkFutureCancel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future := NewFuture()
			future.Completable().Cancel()
			future.Get()
		}
	})
}

// BenchmarkFutureConcurrentListeners tests concurrent listener operations
func BenchmarkFutureConcurrentListeners(b *testing.B) {
	// Use sequential approach to avoid massive goroutine creation
	for i := 0; i < b.N; i++ {
		future := NewFuture()
		
		// Add listeners sequentially to avoid goroutine explosion
		for j := 0; j < 3; j++ {
			future.AddListener(&testListener{})
		}
		
		future.Completable().Complete("test")
		future.Get()
	}
}

// BenchmarkFutureMemoryAllocation tests memory allocation patterns
func BenchmarkFutureMemoryAllocation(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future := NewFuture()
			
			// Add listeners to test slice allocation
			for i := 0; i < 10; i++ {
				future.AddListener(&testListener{})
			}
			
			future.Completable().Complete("test")
			future.Get()
		}
	})
}

// BenchmarkFutureHeavyChaining tests deep chaining scenarios
func BenchmarkFutureHeavyChaining(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future := NewFuture()
			
			// Create a chain of 5 futures
			current := future.Chainable()
			for i := 0; i < 5; i++ {
				current = current.Then(func(prev Future) any {
					return prev.Get()
				})
			}
			
			future.Completable().Complete("test")
			current.Get()
		}
	})
}

// testListener is a simple test listener implementation
type testListener struct{}

func (l *testListener) OperationCompleted(future Future) {
	// Minimal listener operation - avoid calling Get() to prevent deadlocks
	_ = future.IsDone()
}
