package syncx_test

import (
	"testing"

	"github.com/powerman/check"

	"github.com/powerman/syncx"
)

func TestNewTryWaitGroup_nil_panics(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	t.Panic(func() { syncx.NewTryWaitGroup(nil) })
}

func TestTryWaitGroup_TryGo_nil_panics(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	wg := syncx.NewWaitGroup()
	t.Panic(func() { wg.TryGo(nil) })
}

func TestWaitGroup_Add_panics_after_wait(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	wg := syncx.NewWaitGroup()
	wg.Wait()
	t.Panic(func() { wg.Add(1) })
}

func TestWaitGroup_Add_works_before_wait(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	wg := syncx.NewWaitGroup()
	started := make(chan struct{})
	finished := make(chan struct{})

	t.True(wg.TryGo(func() {
		started <- struct{}{}
		<-finished
	}))
	<-started
	t.True(wg.TryGo(func() {
		close(finished)
	}))
	wg.Wait()
}

func TestWaitGroup_TryGo_before_wait(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	wg := syncx.NewWaitGroup()
	ran := make(chan struct{})

	t.True(wg.TryGo(func() { close(ran) }))
	wg.Wait()
	<-ran
}

func TestWaitGroup_TryGo_after_wait(tt *testing.T) {
	tt.Parallel()
	t := check.Must(tt)

	wg := syncx.NewWaitGroup()
	wg.Wait()
	t.False(wg.TryGo(func() {}))
}

func TestWaitGroup_Add_negative_after_stop(tt *testing.T) {
	tt.Parallel()

	wg := syncx.NewWaitGroup()
	wg.Add(1)
	wg.Stop()
	wg.Add(-1)
	wg.Wait()
}
