// Package syncx provides generic helpers extending the standard sync package.
//
// # WaitGroup
//
// [WaitGroup] is a drop-in replacement for [sync.WaitGroup]
// that adds protection against the [sync.WaitGroup.Add]/[sync.WaitGroup.Wait] data race.
// Unlike [sync.WaitGroup], WaitGroup owns its underlying [sync.WaitGroup]
// and provides [WaitGroup.Add], which panics instead of causing a data race.
//
// WaitGroup supports both the classic Add/Done/Wait pattern and the
// TryAdd/TryGo pattern for graceful shutdown.
//
// # TryWaitGroup
//
// [TryWaitGroup] wraps an external [sync.WaitGroup] and provides [TryWaitGroup.TryAdd],
// which refuses to increment the counter after [TryWaitGroup.Stop] has been called.
// Use TryWaitGroup when the [sync.WaitGroup] is shared between multiple components.
//
// TryWaitGroup solves a specific data-race problem that arises when
// [sync.WaitGroup.Add] is called concurrently with [sync.WaitGroup.Wait].
// Standard Go idiom requires Add to happen before Wait starts;
// TryWaitGroup makes the concurrent case safe by providing TryAdd,
// which refuses to increment the counter after Stop has been called.
//
// This pattern is useful when:
//   - External callers may invoke methods concurrently on a long-lived service,
//     and you need to drain them during shutdown (a "reverse" worker pool).
//   - A scheduler or timer may spawn goroutines while shutdown is in progress.
//   - Goroutines are themselves created inside other goroutines,
//     making it impossible to guarantee Add runs before Wait.
//
// # Alternatives
//
// Most graceful-shutdown needs are better served by
// [golang.org/x/sync/errgroup] with context cancellation:
//
//	g, ctx := errgroup.WithContext(ctx)
//	g.Go(func() error {
//	    for {
//	        select {
//	        case <-ctx.Done():
//	            return nil   // shutdown requested
//	        case job := <-jobs:
//	            process(job)
//	        }
//	    }
//	})
//	// ... on signal:
//	cancel()
//	g.Wait()
//
// TryWaitGroup is only needed when the code that adds work is outside your control —
// for example, when you expose public methods that can be called by an external framework,
// and you cannot restructure them to check a context before starting.
package syncx

import "sync"

// TryWaitGroup provides an alternative to [sync.WaitGroup.Add]
// that can be used concurrently with [sync.WaitGroup.Wait].
// Without it, calling Add with a positive delta once Wait might have started is a data race;
// TryAdd makes that case safe by refusing to add after Stop.
//
// TryWaitGroup does not own the underlying WaitGroup — it wraps an external one.
// The caller is responsible for the overall lifecycle:
//  1. Create a [sync.WaitGroup] and a TryWaitGroup via [NewTryWaitGroup].
//  2. Goroutines call [TryWaitGroup.TryAdd] before starting work and
//     [TryWaitGroup.Done] when finished.
//  3. On shutdown, call [TryWaitGroup.Stop] to prevent new work from
//     registering, then call [sync.WaitGroup.Wait] on the underlying group
//     to wait for already-registered work to complete.
//
// Usage:
//
//	var wg sync.WaitGroup
//	twg := syncx.NewTryWaitGroup(&wg)
//
//	// In a goroutine that may race with shutdown:
//	func worker(twg *syncx.TryWaitGroup) {
//	    if !twg.TryAdd(1) {
//	        return // shutdown already in progress
//	    }
//	    defer twg.Done()
//	    // do work ...
//	}
//
//	// On shutdown:
//	twg.Stop()
//	wg.Wait()
type TryWaitGroup struct {
	wg *sync.WaitGroup

	mu      sync.Mutex
	stopped bool
}

// NewTryWaitGroup creates and returns a TryWaitGroup related to wg.
//
// Do not forget to call Stop on each TryWaitGroup related to wg before wg.Wait.
//
// Panics if wg is nil — use [NewWaitGroup] if you need a self-owned WaitGroup.
func NewTryWaitGroup(wg *sync.WaitGroup) *TryWaitGroup {
	if wg == nil {
		panic("syncx: NewTryWaitGroup called with nil *sync.WaitGroup; use NewWaitGroup() for a self-owned WaitGroup")
	}
	return &TryWaitGroup{wg: wg}
}

// Stop marks the TryWaitGroup as stopped.
// After Stop returns, any subsequent call to TryAdd with a positive delta will return false.
//
// Stop must be called before the underlying [sync.WaitGroup.Wait]
// to avoid the data race it is designed to prevent.
// Stop is idempotent.
func (twg *TryWaitGroup) Stop() {
	twg.mu.Lock()
	defer twg.mu.Unlock()

	twg.stopped = true
}

// TryAdd works like [sync.WaitGroup.Add] except it returns false if called
// with a positive delta after Stop.
// This makes it safe to use TryAdd concurrently with [sync.WaitGroup.Wait]:
//
//	if !twg.TryAdd(1) {
//	    return // shutdown in progress, bail out
//	}
//	defer twg.Done()
//	// ...
//
// The positive-delta check means that Done (which calls Add(-1)) always passes through,
// even after Stop.
func (twg *TryWaitGroup) TryAdd(delta int) bool {
	twg.mu.Lock()
	defer twg.mu.Unlock()

	if twg.stopped && delta > 0 {
		return false
	}

	twg.wg.Add(delta)
	return true
}

// TryGo starts fn in a new goroutine, registered via [TryWaitGroup.TryAdd].
// It returns false if the group has been stopped, in which case fn is not called.
func (twg *TryWaitGroup) TryGo(fn func()) bool {
	if fn == nil {
		panic("syncx: TryGo called with nil function")
	}
	if !twg.TryAdd(1) {
		return false
	}
	go func() {
		defer twg.Done()
		fn()
	}()
	return true
}

// Done works like [sync.WaitGroup.Done].
func (twg *TryWaitGroup) Done() {
	twg.wg.Done()
}

// WaitGroup is a drop-in replacement for [sync.WaitGroup]
// that adds protection against the [sync.WaitGroup.Add]/[sync.WaitGroup.Wait] data race.
// Unlike [TryWaitGroup], WaitGroup owns its underlying [sync.WaitGroup].
//
// A zero WaitGroup is not valid. Use [NewWaitGroup] to create one.
type WaitGroup struct {
	*TryWaitGroup
}

// NewWaitGroup creates and returns a new WaitGroup.
func NewWaitGroup() *WaitGroup {
	return &WaitGroup{
		TryWaitGroup: NewTryWaitGroup(new(sync.WaitGroup)),
	}
}

// Add works like [sync.WaitGroup.Add].
// It panics if called with a positive delta after [WaitGroup.Wait] has been called.
func (wg *WaitGroup) Add(delta int) {
	if !wg.TryAdd(delta) {
		panic("syncx.WaitGroup: Add called after Wait")
	}
}

// Wait blocks until the WaitGroup counter is zero.
// It calls [WaitGroup.Stop] before waiting to prevent new goroutines from registering.
// After Wait returns, any subsequent [WaitGroup.Add] with a positive delta panics.
func (wg *WaitGroup) Wait() {
	wg.Stop()
	wg.wg.Wait()
}
