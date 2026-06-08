package syncx_test

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/powerman/syncx"
)

// This example shows the core pattern:
// workers that take varying time to start register via TryAdd,
// and the orchestrator uses Stop to prevent any further registrations
// before waiting on the underlying WaitGroup.
//
// Workers with delays 40ms and 50ms arrive after Stop (called at ~33ms)
// and are rejected — TryAdd returns false and they return early.
func ExampleTryWaitGroup_basic() {
	var wg sync.WaitGroup
	twg := syncx.NewTryWaitGroup(&wg)

	delays := []int{10, 20, 30, 40, 50}

	var started atomic.Int64

	for _, d := range delays {
		go func() {
			// Simulate variable-length setup before registration.
			time.Sleep(time.Duration(d) * time.Millisecond)

			if !twg.TryAdd(1) {
				return // shutdown already in progress, bail out
			}
			defer twg.Done()
			started.Add(1)
		}()
	}

	// Give the first three workers time to register,
	// but not the last two.
	time.Sleep(33 * time.Millisecond)

	// Stop accepting new work, then wait for active workers.
	twg.Stop()
	wg.Wait()

	fmt.Println("started workers:", started.Load())
	// Output:
	// started workers: 3
}

// This example shows graceful shutdown of a service whose public methods
// are called concurrently by external code —
// a pattern where the callers are outside your control, so TryAdd is needed.
//
// Without TryWaitGroup, calling wg.Add inside a method while another
// goroutine calls wg.Wait would be a data race. TryAdd makes this safe.
func ExampleTryWaitGroup_gracefulShutdown() {
	var wg sync.WaitGroup
	twg := syncx.NewTryWaitGroup(&wg)

	var mu sync.Mutex
	var processed []string

	// A service that processes requests.
	// Its Process method can be called from any goroutine.
	process := func(msg string) {
		if !twg.TryAdd(1) {
			return // shutdown in progress, silently drop
		}
		defer twg.Done()

		mu.Lock()
		processed = append(processed, msg)
		mu.Unlock()
	}

	// Start a few concurrent requests.
	go process("request-a")
	go process("request-b")
	go process("request-c")

	// Give them time to register.
	time.Sleep(5 * time.Millisecond)

	// Initiate graceful shutdown.
	twg.Stop()
	wg.Wait() // waits for a, b, c to finish

	// Any new request after Stop is silently dropped.
	process("request-d") // never added to processed

	// Sort for deterministic output.
	slices.Sort(processed)
	for _, msg := range processed {
		fmt.Println("processed:", msg)
	}

	// Output:
	// processed: request-a
	// processed: request-b
	// processed: request-c
}
