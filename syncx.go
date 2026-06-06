// Package syncx provides generic helpers extending the standard sync package.
//
// # TryWaitGroup
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
func NewTryWaitGroup(wg *sync.WaitGroup) *TryWaitGroup {
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

// Done works like [sync.WaitGroup.Done].
func (twg *TryWaitGroup) Done() {
	twg.wg.Done()
}
