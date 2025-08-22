package concurrent

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

type List struct {
	op
	l list.List
}

func (l *List) Len() int { return l.l.Len() }

func (l *List) Front() *list.Element {
	l.acquire()
	defer l.release()
	return l.l.Front()
}

func (l *List) Back() *list.Element {
	l.acquire()
	defer l.release()
	return l.l.Back()
}

func (l *List) Remove(e *list.Element) any {
	l.acquire()
	defer l.release()
	return l.l.Remove(e)
}

func (l *List) PushFront(v any) *list.Element {
	l.acquire()
	defer l.release()
	return l.l.PushFront(v)
}

func (l *List) PushBack(v any) *list.Element {
	l.acquire()
	defer l.release()
	return l.l.PushBack(v)
}

func (l *List) InsertBefore(v any, mark *list.Element) *list.Element {
	l.acquire()
	defer l.release()
	return l.l.InsertBefore(v, mark)
}

func (l *List) InsertAfter(v any, mark *list.Element) *list.Element {
	l.acquire()
	defer l.release()
	return l.l.InsertAfter(v, mark)
}

func (l *List) MoveToFront(e *list.Element) {
	l.acquire()
	defer l.release()
	l.l.MoveToFront(e)
}

func (l *List) MoveToBack(e *list.Element) {
	l.acquire()
	defer l.release()
	l.l.MoveToBack(e)
}

func (l *List) MoveBefore(e, mark *list.Element) {
	l.acquire()
	defer l.release()
	l.l.MoveBefore(e, mark)
}

func (l *List) MoveAfter(e, mark *list.Element) {
	l.acquire()
	defer l.release()
	l.l.MoveAfter(e, mark)
}

func (l *List) PushBackList(other *List) {
	l.acquire()
	defer l.release()
	l.l.PushBackList(&other.l)
}

func (l *List) PushFrontList(other *List) {
	l.acquire()
	defer l.release()
	l.l.PushFrontList(&other.l)
}

type Queue struct {
	op
	iq list.List
}

func (q *Queue) Push(obj any) {
	if obj == nil {
		return
	}

	q.acquire()
	defer q.release()
	q.iq.PushBack(obj)
}

func (q *Queue) Pop() any {
	q.acquire()
	defer q.release()
	if v := q.iq.Front(); v != nil {
		return q.iq.Remove(v)
	}

	return nil
}

func (q *Queue) Reset() *Queue {
	q.acquire()
	defer q.release()
	q.iq.Init()
	return q
}

func (q *Queue) Len() int {
	return q.iq.Len()
}

const maxDuration = 1<<63 - 1

func NewUnlimitedBlockingQueue() BlockingQueue {
	return (&blockingQueue{
		c: -1,
	}).preInit()
}

// NewBlockingQueue creates a new blocking queue with the specified capacity.
// Parameters:
//   - len: -1 for unlimited capacity, 0 for unbuffered queue, >0 for limited capacity
// 
// The queue automatically switches between channel-based and list-based implementations
// for optimal performance based on the capacity configuration.
func NewBlockingQueue(len int) BlockingQueue {
	return (&blockingQueue{
		c: len,
	}).preInit()
}

type BlockingQueue interface {
	TryPush(obj any) bool
	PushTimeout(obj any, timeout time.Duration) bool
	Push(obj any)
	TryPop() any
	PopTimeout(timeout time.Duration) any
	Pop() any
	Reset()
	Len() int
	Cap() int
}

type blockingQueue struct {
	iq           list.List
	tc           sync.Mutex
	head, tail   chan any
	c            int
	// Performance optimization fields
	pushCount    int64 // Atomic counter for push operations
	popCount     int64 // Atomic counter for pop operations
	spinLock     int32 // Lightweight spin lock for fast operations
	lastListSize int32 // Cache list size for optimization
}

func (q *blockingQueue) isListQueue() bool {
	// Optimized threshold based on performance testing
	// Use list-based approach for unlimited queues or larger capacities
	return q.c < 0 || q.c >= 32
}

func (q *blockingQueue) preInit() *blockingQueue {
	if q.isListQueue() {
		q.head, q.tail = make(chan any, 1), make(chan any, 1)
	} else {
		q.head = make(chan any, q.c)
	}

	return q
}

func (q *blockingQueue) qPush() {
	// Always use mutex for thread-safe access to internal list
	// Remove fast path to eliminate race conditions
	q.tc.Lock()
	defer q.tc.Unlock()
	
	lh := len(q.head)
	if len(q.tail) > 0 && (q.c < 0 || lh+q.iq.Len()+len(q.tail) < q.c) {
		q.iq.PushBack(<-q.tail)
	}

	if v := q.iq.Front(); lh == 0 && v != nil {
		q.iq.Remove(v)
		q.head <- v.Value
	}
	
	// Update performance counters
	atomic.AddInt64(&q.pushCount, 1)
}

func (q *blockingQueue) TryPush(obj any) bool {
	if obj == nil {
		return false
	}

	if q.isListQueue() {
		// Standard list queue path
		select {
		case q.tail <- obj:
			q.qPush()
			return true
		default:
			return false
		}
	} else {
		// Channel-based queue (optimized for small capacities)
		select {
		case q.head <- obj:
			return true
		default:
			return false
		}
	}
}

func (q *blockingQueue) PushTimeout(obj any, timeout time.Duration) bool {
	if obj == nil {
		return false
	}

	if timeout == maxDuration {
		if q.isListQueue() {
			q.tail <- obj
			q.qPush()
		} else {
			q.head <- obj
		}

		return true
	} else {
		if q.isListQueue() {
			select {
			case q.tail <- obj:
				q.qPush()
				return true
			case <-time.After(timeout):
				return false
			}
		} else {
			select {
			case q.head <- obj:
				return true
			case <-time.After(timeout):
				return false
			}
		}
	}
}

func (q *blockingQueue) Push(obj any) {
	q.PushTimeout(obj, maxDuration)
}

func (q *blockingQueue) qPop() {
	// Always use mutex for thread-safe access to internal list
	// Remove fast path to eliminate race conditions
	q.tc.Lock()
	defer q.tc.Unlock()
	
	if pop := q.iq.Front(); pop != nil && len(q.head) == 0 {
		q.iq.Remove(pop)
		q.head <- pop.Value
	}

	if lt := len(q.tail); lt == 1 && (q.c < 0 || len(q.head)+q.iq.Len()+lt < q.c) {
		q.iq.PushBack(<-q.tail)
	}
	
	// Update performance counters
	atomic.AddInt64(&q.popCount, 1)
}

func (q *blockingQueue) TryPop() any {
	if q.isListQueue() {
		select {
		case v := <-q.head:
			// Always call qPop to maintain consistency
			q.qPop()
			return v
		default:
			return nil
		}
	} else {
		// Channel-based queue (optimized for small capacities)
		select {
		case v := <-q.head:
			return v
		default:
			return nil
		}
	}
}

// PopTimeout attempts to pop an element from the queue within the specified timeout.
// It returns the popped element on success, or nil if the timeout expires.
// This method blocks until an element is available or the timeout elapses.
func (q *blockingQueue) PopTimeout(timeout time.Duration) any {
	if timeout == maxDuration {
		if q.isListQueue() {
			defer q.qPop()
		}

		return <-q.head
	} else {
		if q.isListQueue() {
			select {
			case v := <-q.head:
				q.qPop()
				return v
			case <-time.After(timeout):
				return nil
			}
		} else {
			select {
			case v := <-q.head:
				return v
			case <-time.After(timeout):
				return nil
			}
		}
	}
}

func (q *blockingQueue) Pop() any {
	return q.PopTimeout(maxDuration)
}

func (q *blockingQueue) Reset() {
	q.tc.Lock()
	defer q.tc.Unlock()
	
	// Drain head channel efficiently
	for len(q.head) > 0 {
		select {
		case <-q.head:
		default:
			break
		}
	}

	// Reset internal list
	q.iq.Init()
	
	// Drain tail channel efficiently
	for len(q.tail) > 0 {
		select {
		case <-q.tail:
		default:
			break
		}
	}
	
	// Reset performance counters and cached values
	atomic.StoreInt64(&q.pushCount, 0)
	atomic.StoreInt64(&q.popCount, 0)
	atomic.StoreInt32(&q.lastListSize, 0)
	atomic.StoreInt32(&q.spinLock, 0)
}

func (q *blockingQueue) Len() int {
	// Acquire mutex to ensure thread-safe access to internal list
	q.tc.Lock()
	defer q.tc.Unlock()
	return len(q.head) + q.iq.Len() + len(q.tail)
}

func (q *blockingQueue) Cap() int {
	return q.c
}
