package concurrent

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRoutineService(t *testing.T) {
	rs := NewRoutinePool(3, 1)
	assert.Equal(t, 3, rs.StandbyRoutines())
	wg := WaitGroup{}
	wg.Add(1)
	f := rs.Submit(func(r Routine) any {
		wg.Done()
		time.Sleep(time.Millisecond)
		return 1
	})

	wg.Wait()
	assert.Equal(t, 2, rs.StandbyRoutines())
	f.Await()
	assert.Equal(t, 3, rs.StandbyRoutines())
	assert.NotEqual(t, 0, f.RoutineId())
	assert.Equal(t, 1, f.Get())
	assert.False(t, rs.IsShutdown())
	rs.ShutdownGracefully()
	assert.Panics(t, rs.ShutdownGracefully)
	assert.Panics(t, rs.ShutdownImmediately)
	assert.True(t, rs.IsShutdown())
}

func TestRoutineServiceRoutineId(t *testing.T) {
	rs := NewBlockingRoutinePool(3, 1)
	f1 := rs.Submit(func(r Routine) any {
		time.Sleep(time.Millisecond)
		return 1
	})

	f2 := rs.Submit(func(r Routine) any {
		time.Sleep(time.Millisecond)
		return 1
	})

	f3 := rs.Submit(func(r Routine) any {
		time.Sleep(time.Millisecond)
		return 1
	})

	f4 := rs.Submit(func(r Routine) any {
		time.Sleep(time.Millisecond)
		return 1
	})

	f1.Await()
	f2.Await()
	f3.Await()
	f4.Await()
	assert.NotEqual(t, 0, f1.RoutineId())
	assert.NotEqual(t, 0, f2.RoutineId())
	assert.NotEqual(t, 0, f3.RoutineId())
	assert.NotEqual(t, 0, f4.RoutineId())
	assert.True(t, (f1.RoutineId() != f2.RoutineId()) && (f2.RoutineId() != f3.RoutineId()))
	assert.True(t, (f1.RoutineId() == f4.RoutineId()) || (f2.RoutineId() == f4.RoutineId()) || (f3.RoutineId() == f4.RoutineId()))
	rs.ShutdownGracefully()
	assert.Panics(t, rs.ShutdownGracefully)
	assert.Panics(t, rs.ShutdownImmediately)
	assert.True(t, rs.IsShutdown())
}

func TestRoutineServiceSubmitGracefully(t *testing.T) {
	tick := 100
	round := 10000
	wg := sync.WaitGroup{}
	for i := 0; i < tick; i++ {
		wg.Add(1)
		go func() {
			rs := NewBlockingRoutinePool(3, 1)
			c := int32(0)
			for i := 0; i < round; i++ {
				rs.Submit(func(r Routine) any {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			rs.ShutdownGracefully()
			assert.Equal(t, int64(round), int64(rs.DoneTasks()))
			assert.Equal(t, int32(round), c)

			for i := 0; i < round; i++ {
				rs.Submit(func(r Routine) any {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			assert.Equal(t, uint64(round), rs.DoneTasks())
			assert.Equal(t, int32(round), c)
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestRoutineServiceSubmitGracefullyF(t *testing.T) {
	rs := NewBlockingRoutinePool(100, 10000)
	tc := 10000
	c := int32(0)
	cc := int32(0)
	for i := 0; i < tc; i++ {
		ic := i
		rs.Submit(func(r Routine) any {
			time.Sleep(time.Microsecond)
			atomic.AddInt32(&c, int32(ic))
			return &cc
		}).Chainable().Then(func(parent Future) any {
			return atomic.AddInt32(parent.Get().(*int32), 1)
		})
	}

	rs.ShutdownGracefully()
	// Brief pause to ensure all tasks are properly counted
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, uint64(tc), rs.DoneTasks())
	assert.Equal(t, int32((tc-1)*tc/2), c)
	assert.Equal(t, int32(tc), cc)
	for i := 0; i < tc; i++ {
		rs.Submit(func(r Routine) any {
			return atomic.AddInt32(&c, 1)
		})
	}

	assert.Equal(t, uint64(tc), rs.DoneTasks())
	assert.Equal(t, int32((tc-1)*tc/2), c)
	assert.Equal(t, int32(tc), cc)
}

func TestRoutineServiceSubmitImmediately(t *testing.T) {
	tick := 100
	wg := sync.WaitGroup{}
	for i := 0; i < tick; i++ {
		wg.Add(1)
		go func() {
			rs := NewBlockingRoutinePool(3, 1)
			c := int32(0)
			go func() {
				time.Sleep(time.Microsecond)
				rs.ShutdownImmediately()
			}()

			for i := 0; i < 10000; i++ {
				rs.Submit(func(r Routine) any {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			assert.GreaterOrEqual(t, int32(10000), c)
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestRoutineServiceSubmitImmediately10000(t *testing.T) {
	tick := 100
	wg := sync.WaitGroup{}
	for i := 0; i < tick; i++ {
		wg.Add(1)
		go func() {
			rs := NewBlockingRoutinePool(3, 10000)
			c := int32(0)

			for i := 0; i < 10000; i++ {
				rs.Submit(func(r Routine) any {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			rs.ShutdownImmediately()
			assert.GreaterOrEqual(t, int32(10000), c)
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestRoutineServiceSubmitImmediatelyUnLimit(t *testing.T) {
	tick := 100
	wg := sync.WaitGroup{}
	for i := 0; i < tick; i++ {
		wg.Add(1)
		go func() {
			rs := NewBlockingRoutinePool(3, -1)
			c := int32(0)

			for i := 0; i < 10000; i++ {
				rs.Submit(func(r Routine) any {
					time.Sleep(time.Microsecond)
					return atomic.AddInt32(&c, 1)
				})
			}

			rs.ShutdownImmediately()
			assert.GreaterOrEqual(t, int32(10000), c)
			wg.Done()
		}()
	}

	wg.Wait()
}
