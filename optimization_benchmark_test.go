package concurrent

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkFuture_MemoryPoolVsNoPools compares memory pool optimization effects
func BenchmarkFuture_MemoryPoolVsNoPools(b *testing.B) {
	b.Run("WithMemoryPools", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				future := NewFuture()
				
				// Add multiple listeners to utilize memory pool
				for i := 0; i < 5; i++ {
					future.AddListener(NewFutureListener(func(f Future) {
						// Simple listener
					}))
				}
				
				future.Completable().Complete(42)
			}
		})
	})
}

// BenchmarkFuture_ChainEfficiency measures chaining performance improvements
func BenchmarkFuture_ChainEfficiency(b *testing.B) {
	b.Run("OptimizedThenChain", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			future := NewFuture()
			
			// Create a chain of Then operations
			chain := future.Chainable().Then(func(prev Future) any {
				value := prev.Get().(int)
				return fmt.Sprintf("step1_%d", value)
			}).Then(func(prev Future) any {
				value := prev.Get().(string)
				return len(value)
			}).Then(func(prev Future) any {
				value := prev.Get().(int)
				return value > 0
			})
			
			// Complete the original future
			future.Completable().Complete(42)
			
			// Wait for chain completion
			chain.Get()
		}
	})
	
	b.Run("OptimizedThenAsyncChain", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			future := NewFuture()
			
			// Create a chain of ThenAsync operations
			chain := future.Chainable().ThenAsync(func(prev Future) any {
				value := prev.Get().(int)
				result := NewFuture()
				go func() {
					result.Completable().Complete(fmt.Sprintf("async_step1_%d", value))
				}()
				return result
			}).ThenAsync(func(prev Future) any {
				value := prev.Get().(string)
				result := NewFuture()
				go func() {
					result.Completable().Complete(len(value))
				}()
				return result
			})
			
			// Complete the original future
			future.Completable().Complete(42)
			
			// Wait for chain completion
			chain.Get()
		}
	})
}

// BenchmarkFuture_ConcurrentListeners measures listener execution performance
func BenchmarkFuture_ConcurrentListeners(b *testing.B) {
	numListeners := []int{1, 10, 50, 100}
	
	for _, n := range numListeners {
		b.Run(fmt.Sprintf("Listeners_%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				future := NewFuture()
				var wg sync.WaitGroup
				wg.Add(n)
				
				// Add multiple listeners
				for j := 0; j < n; j++ {
					future.AddListener(NewFutureListener(func(f Future) {
						// Simulate some work
						runtime.Gosched()
						wg.Done()
					}))
				}
				
				// Complete and wait for all listeners
				future.Completable().Complete(42)
				wg.Wait()
			}
		})
	}
}

// BenchmarkFuture_PanicSafety measures overhead of panic safety mechanisms
func BenchmarkFuture_PanicSafety(b *testing.B) {
	b.Run("PanicSafeCompletion", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			future := NewFuture()
			
			// Add a listener that might panic
			future.AddListener(NewFutureListener(func(f Future) {
				if val, ok := f.Get().(int); ok && val == 999 { // Trigger panic condition rarely
					panic("test panic")
				}
			}))
			
			// Complete with safe value
			future.Completable().Complete(42)
		}
	})
	
	b.Run("PanicSafeListenerExecution", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			future := NewFuture()
			
			// Add multiple listeners, some might panic
			for j := 0; j < 5; j++ {
				idx := j
				future.AddListener(NewFutureListener(func(f Future) {
					if val, ok := f.Get().(int); idx == 4 && ok && val == 999 { // Rare panic condition
						panic("listener panic")
					}
					// Normal processing
				}))
			}
			
			future.Completable().Complete(42)
		}
	})
}

// BenchmarkMutex_AdaptiveSpinning compares spinning optimization effects
func BenchmarkMutex_AdaptiveSpinning(b *testing.B) {
	b.Run("OptimizedSpinning", func(b *testing.B) {
		m := &Mutex{}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Lock()
				// Simulate very short critical section
				runtime.Gosched()
				m.Unlock()
			}
		})
	})
	
	b.Run("ContendedLocking", func(b *testing.B) {
		m := &Mutex{}
		counter := int64(0)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Lock()
				atomic.AddInt64(&counter, 1)
				// Simulate work
				for i := 0; i < 100; i++ {
					_ = atomic.LoadInt64(&counter)
				}
				m.Unlock()
			}
		})
	})
}

// BenchmarkMutex_TryLockTimeout measures timeout optimization efficiency
func BenchmarkMutex_TryLockTimeout(b *testing.B) {
	timeouts := []time.Duration{
		time.Microsecond * 10,
		time.Microsecond * 100,
		time.Millisecond * 1,
		time.Millisecond * 10,
	}
	
	for _, timeout := range timeouts {
		b.Run(fmt.Sprintf("Timeout_%v", timeout), func(b *testing.B) {
			m := &Mutex{}
			m.Lock() // Pre-lock to test timeout behavior
			defer m.Unlock()
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.TryLockTimeout(timeout)
			}
		})
	}
}

// BenchmarkMutex_UnlockedChannel measures Unlocked() method efficiency
func BenchmarkMutex_UnlockedChannel(b *testing.B) {
	b.Run("UnlockedImmediate", func(b *testing.B) {
		m := &Mutex{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ch := m.Unlocked()
			<-ch // Should complete immediately
		}
	})
	
	b.Run("UnlockedWait", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := &Mutex{}
			m.Lock()
			
			go func() {
				time.Sleep(time.Microsecond * 10)
				m.Unlock()
			}()
			
			ch := m.Unlocked()
			<-ch
		}
	})
}

// BenchmarkMutex_CompareWithStdMutex compares with standard library mutex
func BenchmarkMutex_CompareWithStdMutex(b *testing.B) {
	b.Run("CustomMutex", func(b *testing.B) {
		m := &Mutex{}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Lock()
				m.Unlock()
			}
		})
	})
	
	b.Run("StdMutex", func(b *testing.B) {
		var m sync.Mutex
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Lock()
				m.Unlock()
			}
		})
	})
	
	b.Run("CustomMutexContended", func(b *testing.B) {
		m := &Mutex{}
		counter := int64(0)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Lock()
				atomic.AddInt64(&counter, 1)
				m.Unlock()
			}
		})
	})
	
	b.Run("StdMutexContended", func(b *testing.B) {
		var m sync.Mutex
		counter := int64(0)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Lock()
				atomic.AddInt64(&counter, 1)
				m.Unlock()
			}
		})
	})
}

// BenchmarkOverallSystemPerformance measures system-wide performance improvements
func BenchmarkOverallSystemPerformance(b *testing.B) {
	b.Run("MixedWorkload", func(b *testing.B) {
		m := &Mutex{}
		futures := make([]Future, 100)
		
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// Mixed workload: mutex operations + future operations
				go func() {
					m.Lock()
					defer m.Unlock()
					
					// Create and complete a future
					idx := int(atomic.AddInt64(&counter, 1)) % len(futures)
					if futures[idx] == nil {
						futures[idx] = NewFuture()
						futures[idx].Completable().Complete(42)
					}
				}()
			}
		})
	})
}

var counter int64

// BenchmarkMemoryEfficiency measures memory allocation efficiency
func BenchmarkMemoryEfficiency(b *testing.B) {
	b.Run("FutureMemoryUsage", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			future := NewFuture()
			future.AddListener(NewFutureListener(func(f Future) {}))
			future.Completable().Complete(42)
		}
	})
	
	b.Run("MutexMemoryUsage", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := &Mutex{}
			m.Lock()
			m.Unlock()
		}
	})
}
