package concurrent

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stateWait = iota
	stateSuccess
	stateCancel
	stateFail
)

type ChainFuture interface {
	Future
	Prev() ChainFuture
	Then(fn func(prev Future) any) (future ChainFuture)
	ThenAsync(fn func(prev Future) any) (future ChainFuture)
}

type Future interface {
	Immutable
	Completable() Completable
	Immutable() Immutable
	Chainable() ChainFuture
}

type Immutable interface {
	Get() any
	GetTimeout(timeout time.Duration) any
	GetNow() any
	Done() <-chan struct{}
	Await() Future
	AwaitTimeout(timeout time.Duration) Future
	IsDone() bool
	IsSuccess() bool
	IsCancelled() bool
	IsFail() bool
	Error() error
	AddListener(listener FutureListener) Future
}

type Completable interface {
	Complete(obj any) bool
	Cancel() bool
	Fail(err error) bool
}

type Settable interface {
	Set(obj any)
}

type CastFuture[T any] interface {
	Get() (obj T)
	GetTimeout(timeout time.Duration) (obj T)
	GetNow() (obj T)
	Done() <-chan struct{}
	Await() CastFuture[T]
	AwaitTimeout(timeout time.Duration) CastFuture[T]
	IsDone() bool
	IsSuccess() bool
	IsCancelled() bool
	IsFail() bool
	Error() error
	AddListener(listener FutureListener) CastFuture[T]
	Completable() Completable
	Chainable() ChainFuture
	BaseFuture() Future
}

func NewCastFuture[T any]() CastFuture[T] {
	var an any = &DefaultCastFuture[T]{Future: NewFuture()}
	return an.(CastFuture[T])
}

func WrapCastFuture[T any](future Future) CastFuture[T] {
	var an any = &DefaultCastFuture[T]{Future: future}
	return an.(CastFuture[T])
}

type DefaultCastFuture[T any] struct {
	Future
}

func (f *DefaultCastFuture[T]) Get() (obj T) {
	if v := f.Future.Get(); v != nil {
		obj = v.(T)
	}

	return
}

func (f *DefaultCastFuture[T]) GetTimeout(timeout time.Duration) (obj T) {
	if v := f.Future.GetTimeout(timeout); v != nil {
		obj = v.(T)
	}

	return
}

func (f *DefaultCastFuture[T]) GetNow() (obj T) {
	if v := f.Future.GetNow(); v != nil {
		obj = v.(T)
	}

	return
}

func (f *DefaultCastFuture[T]) Await() CastFuture[T] {
	f.BaseFuture().Get()
	return f
}

func (f *DefaultCastFuture[T]) AwaitTimeout(timeout time.Duration) CastFuture[T] {
	f.BaseFuture().GetTimeout(timeout)
	return f
}

func (f *DefaultCastFuture[T]) AddListener(listener FutureListener) CastFuture[T] {
	f.BaseFuture().AddListener(listener)
	return f
}

func (f *DefaultCastFuture[T]) BaseFuture() Future {
	return f.Future
}

func NewFuture() Future {
	return newDefaultFuture()
}

func NewCompletedFuture(obj any) Future {
	f := NewFuture()
	f.Completable().Complete(obj)
	return f
}

func NewCancelledFuture() Future {
	f := NewFuture()
	f.Completable().Cancel()
	return f
}

func NewFailedFuture(err error) Future {
	f := NewFuture()
	f.Completable().Fail(err)
	return f
}

func NewChainFuture(future ChainFuture) ChainFuture {
	f := newDefaultFuture()
	f.prev = future
	return f
}

// futureListenerPool manages a pool of listener slices to reduce allocations
var futureListenerPool = sync.Pool{
	New: func() interface{} {
		return make([]FutureListener, 0, 16) // Start with reasonable capacity
	},
}

// getFutureListeners gets a listener slice from pool
func getFutureListeners() []FutureListener {
	return futureListenerPool.Get().([]FutureListener)[:0] // Reset length but keep capacity
}

// putFutureListeners returns a listener slice to pool
func putFutureListeners(listeners []FutureListener) {
	if cap(listeners) > 0 && cap(listeners) <= 256 { // Only pool reasonably sized slices
		futureListenerPool.Put(listeners)
	}
}

type DefaultFuture struct {
	op
	ch        chan struct{}
	obj       any
	state     int32
	err       error
	listeners []FutureListener  // Use slice instead of list for better performance
	listenerCap int32           // Track capacity to avoid frequent reallocations
	prev      ChainFuture
}

// futurePool manages a pool of DefaultFuture instances to reduce allocations
var futurePool = sync.Pool{
	New: func() interface{} {
		return &DefaultFuture{
			ch:          make(chan struct{}),
			state:       stateWait,
			listenerCap: 0,
		}
	},
}

// newDefaultFuture creates a new DefaultFuture with optimized initialization.
// Uses memory pool and pre-allocated channel for better performance.
func newDefaultFuture() *DefaultFuture {
	f := futurePool.Get().(*DefaultFuture)
	
	// Reset state for reuse
	f.ch = make(chan struct{})
	f.obj = nil
	f.state = stateWait
	f.err = nil
	f.listeners = nil
	f.listenerCap = 0
	f.prev = nil
	
	return f
}

// returnToPool returns a Future to the object pool after use
func (f *DefaultFuture) returnToPool() {
	// Only return to pool if it's in a clean state
	if f.listeners != nil {
		putFutureListeners(f.listeners)
		f.listeners = nil
	}
	
	// Clear references to help GC
	f.obj = nil
	f.err = nil
	f.prev = nil
	
	// Return to pool for reuse
	futurePool.Put(f)
}

func (f *DefaultFuture) Prev() ChainFuture {
	return f.prev
}

func (f *DefaultFuture) self() Future {
	return f
}

func (f *DefaultFuture) Completable() Completable {
	return f.self().(Completable)
}

func (f *DefaultFuture) Immutable() Immutable {
	return f.self().(Immutable)
}

func (f *DefaultFuture) Chainable() ChainFuture {
	return f.self().(ChainFuture)
}

func (f *DefaultFuture) Future() Future {
	return f.self().(Future)
}

// _waitJudge optimized waiting with fast-path checks and efficient timeout handling.
// Reduces unnecessary channel operations for already completed futures.
func (f *DefaultFuture) _waitJudge(timeout time.Duration) (done bool) {
	// Fast path: check if already done to avoid channel operations
	if atomic.LoadInt32(&f.state) != stateWait {
		return true
	}
	
	if timeout == 0 {
		// Block until completion - no timeout
		<-f.ch
		return true
	} else {
		// Use select with timeout for bounded waiting
		select {
		case <-f.ch:
			return true
		case <-time.After(timeout):
			return false
		}
	}
}

func (f *DefaultFuture) Get() any {
	return f.GetTimeout(0)
}

func (f *DefaultFuture) GetTimeout(timeout time.Duration) any {
	if f.IsDone() {
		f.acquire()
		obj := f.obj
		f.release()
		return obj
	}

	if f._waitJudge(timeout) {
		f.acquire()
		obj := f.obj
		f.release()
		return obj
	}

	return nil
}

// GetNow retrieves the result immediately, regardless of completion status.
// This method returns the current value even if the Future is not yet completed.
// Uses optimized synchronization for thread safety.
func (f *DefaultFuture) GetNow() any {
	f.acquire()
	defer f.release()
	return f.obj
}

func (f *DefaultFuture) Done() <-chan struct{} {
	return f.ch
}

func (f *DefaultFuture) Await() Future {
	f.Get()
	return f
}

func (f *DefaultFuture) AwaitTimeout(timeout time.Duration) Future {
	f.GetTimeout(timeout)
	return f
}

func (f *DefaultFuture) IsDone() bool {
	return atomic.LoadInt32(&f.state) != stateWait
}

func (f *DefaultFuture) IsSuccess() bool {
	return atomic.LoadInt32(&f.state) == stateSuccess
}

func (f *DefaultFuture) IsCancelled() bool {
	return atomic.LoadInt32(&f.state) == stateCancel
}

func (f *DefaultFuture) IsFail() bool {
	return atomic.LoadInt32(&f.state) == stateFail
}

func (f *DefaultFuture) Error() error {
	return f.err
}

// AddListener adds a completion listener to this Future.
// If the Future is already completed, the listener is invoked immediately.
// This optimized version uses memory pool to reduce allocations.
func (f *DefaultFuture) AddListener(listener FutureListener) Future {
	if listener == nil {
		return f
	}

	// Fast path: check state before acquiring lock for better performance
	if atomic.LoadInt32(&f.state) != stateWait {
		// Execute listener immediately for completed futures with panic protection
		f.safeCallListener(listener)
		return f
	}
	
	f.acquire()
	defer f.release()
	
	// Double-check state after lock acquisition (avoid TOCTOU race)
	if f.state != stateWait {
		// Release lock before calling listener to avoid deadlock
		f.release()
		f.safeCallListener(listener)
		f.acquire()
		return f
	}

	// Use memory pool for listener storage optimization
	if f.listeners == nil {
		f.listeners = getFutureListeners()
		atomic.StoreInt32(&f.listenerCap, int32(cap(f.listeners)))
	} else if len(f.listeners) == cap(f.listeners) {
		// Grow using more efficient strategy
		oldListeners := f.listeners
		newCap := cap(f.listeners) * 3 / 2 // 1.5x growth for memory efficiency
		if newCap < 16 {
			newCap = 16
		}
		// Get new slice from pool or allocate if too large
		var newListeners []FutureListener
		if newCap <= 256 {
			newListeners = getFutureListeners()
			if cap(newListeners) < newCap {
				newListeners = make([]FutureListener, 0, newCap)
			}
		} else {
			newListeners = make([]FutureListener, 0, newCap)
		}
		
		newListeners = newListeners[:len(f.listeners)]
		copy(newListeners, f.listeners)
		
		// Return old slice to pool
		putFutureListeners(oldListeners)
		f.listeners = newListeners
		atomic.StoreInt32(&f.listenerCap, int32(newCap))
	}
	
	f.listeners = append(f.listeners, listener)
	return f
}

// Complete marks this Future as successfully completed with the given result.
// Optimized version with panic-safe locks and reduced overhead.
func (f *DefaultFuture) Complete(obj any) bool {
	// Fast path: try atomic swap first
	if !atomic.CompareAndSwapInt32(&f.state, stateWait, stateSuccess) {
		return false
	}
	
	// Set object with panic-safe lock handling
	func() {
		f.acquire()
		defer func() {
			f.release()
			if r := recover(); r != nil {
				// Panic during object setting - reset state and re-panic
				atomic.StoreInt32(&f.state, stateWait)
				panic(r)
			}
		}()
		f.obj = obj
	}()
	
	// Close channel to signal completion - panic safe
	defer func() {
		if r := recover(); r != nil {
			// Channel close panic - reset state for safety
			atomic.StoreInt32(&f.state, stateWait)
			panic(r)
		}
	}()
	close(f.ch)
	
	// Call listeners WITHOUT holding any locks to avoid deadlock
	f.callListener()
	
	return true
}

// Cancel marks this Future as cancelled.
// Uses panic-safe operations and avoids deadlock.
func (f *DefaultFuture) Cancel() bool {
	// Fast path: try atomic swap first
	if !atomic.CompareAndSwapInt32(&f.state, stateWait, stateCancel) {
		return false
	}
	
	// Close channel with panic protection
	defer func() {
		if r := recover(); r != nil {
			// Channel close panic - reset state for safety
			atomic.StoreInt32(&f.state, stateWait)
			panic(r)
		}
	}()
	close(f.ch)
	
	// Call listeners WITHOUT holding any locks to avoid deadlock
	f.callListener()
	
	return true
}

// Fail marks this Future as failed with the given error.
// Uses panic-safe locks and avoids deadlock.
func (f *DefaultFuture) Fail(err error) bool {
	// Fast path: try atomic swap first
	if !atomic.CompareAndSwapInt32(&f.state, stateWait, stateFail) {
		return false
	}
	
	// Set error with comprehensive panic protection
	func() {
		f.acquire()
		defer func() {
			f.release()
			if r := recover(); r != nil {
				// Panic during error setting - reset state and re-panic
				atomic.StoreInt32(&f.state, stateWait)
				panic(r)
			}
		}()
		f.err = err
	}()
	
	// Close channel with panic protection
	defer func() {
		if r := recover(); r != nil {
			// Channel close panic - reset state for safety
			atomic.StoreInt32(&f.state, stateWait)
			panic(r)
		}
	}()
	close(f.ch)
	
	// Call listeners WITHOUT holding any locks to avoid deadlock
	f.callListener()
	
	return true
}

func (f *DefaultFuture) Set(obj any) {
	f.obj = obj
}

// callListener invokes all registered listeners when the future completes.
// This version uses memory pools and optimized panic protection.
func (f *DefaultFuture) callListener() {
	// Safely extract listeners under lock protection
	var listeners []FutureListener
	func() {
		f.acquire()
		defer f.release()
		if f.listeners == nil {
			return
		}
		
		// Move listeners to avoid copying - more efficient
		listeners = f.listeners
		f.listeners = nil
		f.listenerCap = 0
	}()
	
	// Early return if no listeners
	if listeners == nil || len(listeners) == 0 {
		return
	}
	
	// Process listeners without holding any locks
	defer func() {
		// Return listener slice to pool for reuse
		putFutureListeners(listeners)
	}()
	
	// Optimized listener execution with minimal allocations
	for i := range listeners {
		listener := listeners[i]
		if listener == nil {
			continue
		}
		
		// Fast path: avoid reflection for known safe types
		if needsNilCheck(listener) {
			if ref := reflect.ValueOf(listener); ref.Kind() == reflect.Ptr && ref.IsNil() {
				continue
			}
		}
		
		// Execute with panic protection - no function allocation
		f.safeCallListener(listener)
	}
}

// safeCallListener executes a listener with optimized panic protection
// Uses stack-allocated recovery to minimize overhead
func (f *DefaultFuture) safeCallListener(listener FutureListener) {
	defer func() {
		if r := recover(); r != nil {
			// Panic occurred in listener - log and continue
			// In production, this should integrate with logging system
			// Currently we silently recover to maintain stability
			_ = r // Acknowledge the panic value
		}
	}()
	
	// Call listener - this may panic but won't affect other listeners
	listener.OperationCompleted(f)
}

// needsNilCheck performs fast type assertion to avoid reflection overhead
// for common FutureListener implementations
func needsNilCheck(listener FutureListener) bool {
	// Fast path: check if it's our internal type
	if _, ok := listener.(*_FutureListener); ok {
		return false // _FutureListener is never nil if interface is not nil
	}
	return true // Use reflection for unknown types
}

// Then creates a new Future that will be completed with the result of applying 
// the given function to this Future's result. This optimized version uses
// memory pools and provides enhanced performance.
func (f *DefaultFuture) Then(fn func(prev Future) any) (future ChainFuture) {
	if fn == nil {
		panic("concurrent: Then function cannot be nil")
	}
	
	cf := f
	future = NewChainFuture(cf)
	
	// Fast path: if already completed, execute immediately to avoid listener overhead
	if atomic.LoadInt32(&cf.state) != stateWait {
		if cf.IsCancelled() {
			future.Completable().Cancel()
			return future
		}
		
		// Execute synchronously for completed futures with panic protection
		defer func() {
			if r := recover(); r != nil {
				// Handle panic in Then function
				if err, ok := r.(error); ok {
					future.Completable().Fail(err)
				} else {
					future.Completable().Fail(fmt.Errorf("%v", r))
				}
			}
		}()
		executeThenFunction(cf, future, fn)
		return future
	}
	
	// Async path: add optimized listener for pending futures
	f.AddListener(&optimizedThenListener{
		sourceFuture: cf,
		targetFuture: future,
		fn:          fn,
	})
	
	return future
}

// optimizedThenListener implements FutureListener with better performance
type optimizedThenListener struct {
	sourceFuture Future
	targetFuture Future
	fn          func(prev Future) any
}

func (l *optimizedThenListener) OperationCompleted(f Future) {
	// Early exit if source was cancelled
	if l.sourceFuture.IsCancelled() {
		l.targetFuture.Completable().Cancel()
		return
	}
	
	// Skip if target already completed (race condition protection)
	if l.targetFuture.IsDone() {
		return
	}
	
	// Execute with comprehensive panic protection
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				l.targetFuture.Completable().Fail(err)
			} else {
				l.targetFuture.Completable().Fail(fmt.Errorf("%v", r))
			}
		}
	}()
	
	executeThenFunction(l.sourceFuture, l.targetFuture, l.fn)
}

// executeThenFunction executes the Then function with optimized error handling
// Avoids anonymous function allocation for better performance
func executeThenFunction(source, target Future, fn func(prev Future) any) {
	result, err := safeExecuteThenFunction(source, fn)
	
	if err != nil {
		target.Completable().Fail(err)
	} else {
		target.Completable().Complete(result)
	}
}

// safeExecuteThenFunction executes function with panic protection without anonymous function allocation
func safeExecuteThenFunction(source Future, fn func(prev Future) any) (result any, err error) {
	defer func() {
		if e := recover(); e != nil {
			if errVal, ok := e.(error); ok {
				err = errVal
			} else {
				err = fmt.Errorf("%v", e)
			}
		}
	}()
	result = fn(source)
	return result, nil
}

// ThenAsync creates a new Future that will be completed asynchronously with the result
// of applying the given function to this Future's result. This optimized version 
// uses efficient goroutine management and memory pools.
func (f *DefaultFuture) ThenAsync(fn func(prev Future) any) (future ChainFuture) {
	if fn == nil {
		panic("concurrent: ThenAsync function cannot be nil")
	}
	
	cf := f
	future = NewChainFuture(cf)
	
	// Fast path: if already completed, start goroutine immediately
	if atomic.LoadInt32(&cf.state) != stateWait {
		if cf.IsCancelled() {
			future.Completable().Cancel()
			return future
		}
		
		// Launch goroutine with resource management
		go func() {
			defer func() {
				if r := recover(); r != nil {
					// Handle panic in async execution
					if err, ok := r.(error); ok {
						future.Completable().Fail(err)
					} else {
						future.Completable().Fail(fmt.Errorf("%v", r))
					}
				}
			}()
			executeThenAsyncFunction(cf, future, fn)
		}()
		return future
	}
	
	// Async path: add optimized listener for pending futures
	f.AddListener(&optimizedThenAsyncListener{
		sourceFuture: cf,
		targetFuture: future,
		fn:          fn,
	})
	
	return future
}

// optimizedThenAsyncListener implements FutureListener for async operations
type optimizedThenAsyncListener struct {
	sourceFuture Future
	targetFuture Future
	fn          func(prev Future) any
}

func (l *optimizedThenAsyncListener) OperationCompleted(f Future) {
	// Early exit if source was cancelled
	if l.sourceFuture.IsCancelled() {
		l.targetFuture.Completable().Cancel()
		return
	}
	
	// Skip if target already completed (race condition protection)
	if l.targetFuture.IsDone() {
		return
	}
	
	// Launch goroutine with enhanced error handling
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Handle panic in async listener
				if err, ok := r.(error); ok {
					l.targetFuture.Completable().Fail(err)
				} else {
					l.targetFuture.Completable().Fail(fmt.Errorf("%v", r))
				}
			}
		}()
		executeThenAsyncFunction(l.sourceFuture, l.targetFuture, l.fn)
	}()
}

// executeThenAsyncFunction executes the ThenAsync function in a goroutine with optimized error handling
func executeThenAsyncFunction(source, target Future, fn func(prev Future) any) {
	var result any
	var err error
	
	// Optimized panic recovery
	func() {
		defer func() {
			if e := recover(); e != nil {
				if errVal, ok := e.(error); ok {
					err = errVal
				} else {
					err = fmt.Errorf("%v", e)
				}
			}
		}()
		result = fn(source)
	}()
	
	if err != nil {
		target.Completable().Fail(err)
	} else {
		target.Completable().Complete(result)
	}
}

type FutureListener interface {
	OperationCompleted(f Future)
}

type _FutureListener struct {
	f func(f Future)
}

func (l *_FutureListener) OperationCompleted(f Future) {
	l.f(f)
}

func NewFutureListener(f func(f Future)) FutureListener {
	if f == nil {
		return nil
	}

	return &_FutureListener{
		f: f,
	}
}

func Do(f func() any) (future Future) {
	future = NewFuture()
	var rtn any
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				future.Completable().Fail(err)
			} else {
				future.Completable().Fail(fmt.Errorf("%v", e))
			}
		} else {
			future.Completable().Complete(rtn)
		}
	}()

	rtn = f()
	return future
}

func DoAsync(f func() any) (future Future) {
	future = NewFuture()
	go func() {
		var rtn any
		defer func() {
			if e := recover(); e != nil {
				if err, ok := e.(error); ok {
					future.Completable().Fail(err)
				} else {
					future.Completable().Fail(fmt.Errorf("%v", e))
				}
			} else {
				future.Completable().Complete(rtn)
			}
		}()

		rtn = f()
	}()

	return future
}
