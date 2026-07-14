// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThrottleFraction(t *testing.T) {
	const q = 1000
	p := DefaultThrottleParams(q, q) // softStart 0.80, minFraction 0.02

	tests := []struct {
		name              string
		physical, logical int64
		wantFrac          float64
		wantThrottled     bool
	}{
		{"empty", 0, 0, 1, false},
		{"below soft start", 500, 500, 1, false},
		{"at soft start", 800, 0, 1, false},
		{"midway 0.90", 900, 0, 1 - 0.5*(1-0.02), true}, // t=0.5
		{"at full", 1000, 0, 0.02, true},
		{"over full", 2000, 0, 0.02, true},
		{"logical governs", 100, 900, 1 - 0.5*(1-0.02), true},
		{"physical governs", 900, 100, 1 - 0.5*(1-0.02), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.InDelta(t, tc.wantFrac, p.Fraction(tc.physical, tc.logical), 1e-9)
			require.Equal(t, tc.wantThrottled, p.Throttled(tc.physical, tc.logical))
		})
	}
}

func TestThrottleDisabledQuota(t *testing.T) {
	// Physical disabled, logical active: only logical governs.
	p := DefaultThrottleParams(0, 1000)
	require.Equal(t, 1.0, p.Fraction(1_000_000, 500)) // huge physical ignored
	require.InDelta(t, 0.02, p.Fraction(1_000_000, 1000), 1e-9)

	// Both disabled: never throttles.
	off := DefaultThrottleParams(0, 0)
	require.False(t, off.Throttled(1_000_000, 1_000_000))
	require.Equal(t, 1.0, off.Fraction(1_000_000, 1_000_000))
}

// TestWriteThrottleConcurrentRate proves the shared limiter caps the AGGREGATE
// write rate across many concurrent writers — the failure mode of the earlier
// per-request-delay approach, where N writers each sleeping produced N× the
// intended rate.
func TestWriteThrottleConcurrentRate(t *testing.T) {
	var phys, logi atomic.Int64
	w := NewWriteThrottle(DefaultThrottleParams(1000, 0),
		func() int64 { return phys.Load() },
		func() int64 { return logi.Load() }, 0)
	w.baseRate = 2000 // pretend the workload's full-speed rate is 2000/s

	// u = 900/1000 = 0.90 -> fraction 0.51 -> target ~= 1020 ops/s.
	phys.Store(900)
	w.apply(0)

	const (
		writers = 12
		window  = 1500 * time.Millisecond
	)
	var admitted int64
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if err := w.Wait(ctx); err != nil {
					return
				}
				atomic.AddInt64(&admitted, 1)
			}
		}()
	}
	wg.Wait()

	got := float64(atomic.LoadInt64(&admitted)) / window.Seconds()
	t.Logf("aggregate throughput = %.0f ops/s (target ~1020, %d writers)", got, writers)
	// Aggregate must track the ~1020/s target, NOT scale with writer count.
	require.Greater(t, got, 600.0, "throttle too aggressive")
	require.Less(t, got, 1600.0, "throttle failed to cap aggregate rate")
}

// TestWriteThrottleUnthrottledAndDisabled confirms no capping when below the
// soft-start utilization or when both quotas are disabled.
func TestWriteThrottleUnthrottledAndDisabled(t *testing.T) {
	var phys atomic.Int64
	w := NewWriteThrottle(DefaultThrottleParams(1000, 0),
		func() int64 { return phys.Load() }, func() int64 { return 0 }, 0)
	phys.Store(500) // u = 0.5 < softStart -> fraction 1 -> +Inf limit
	w.apply(0)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var n int64
	for ctx.Err() == nil {
		if w.Wait(ctx) == nil {
			n++
		}
	}
	require.Greater(t, n, int64(5000), "unthrottled path should not cap")

	off := NewWriteThrottle(DefaultThrottleParams(0, 0), func() int64 { return 1 << 40 }, func() int64 { return 1 << 40 }, 0)
	require.True(t, off.disabled)
	require.NoError(t, off.Wait(context.Background()))
}

func TestNewThrottleParamsClamping(t *testing.T) {
	// Out-of-range / zero curve values fall back to the defaults.
	for _, ss := range []float64{0, -0.5, 1, 1.5} {
		require.Equal(t, DefaultThrottleSoftStart, NewThrottleParams(1, 1, ss, 0.02).SoftStart, "softStart=%v", ss)
	}
	for _, mf := range []float64{0, -0.1, 1.5} {
		require.Equal(t, DefaultThrottleMinFraction, NewThrottleParams(1, 1, 0.8, mf).MinFraction, "minFraction=%v", mf)
	}
	// In-range values are honored.
	p := NewThrottleParams(10, 20, 0.5, 0.1)
	require.Equal(t, 0.5, p.SoftStart)
	require.Equal(t, 0.1, p.MinFraction)
	require.Equal(t, int64(10), p.PhysicalQuota)
	require.Equal(t, int64(20), p.LogicalQuota)
}

// TestWriteThrottleFixedBaseRate verifies a configured base rate is used
// verbatim and never moved by observed traffic, giving deterministic throttling.
func TestWriteThrottleFixedBaseRate(t *testing.T) {
	var phys atomic.Int64
	w := NewWriteThrottle(DefaultThrottleParams(1000, 0),
		func() int64 { return phys.Load() }, func() int64 { return 0 }, 2000)
	require.True(t, w.fixedBase)
	require.Equal(t, 2000.0, w.baseRate)

	// An un-throttled interval with huge observed traffic must not move it.
	phys.Store(500) // u=0.5 -> fraction 1 (un-throttled)
	w.apply(99999)
	require.Equal(t, 2000.0, w.baseRate)

	// Throttling scales against the fixed base: u=0.9 -> fraction 0.51.
	phys.Store(900)
	w.apply(0)
	require.InDelta(t, 0.51*2000, float64(w.lim.Limit()), 1)
}

// TestWriteThrottleNeverLearnedUsesSeed validates the fallback the review
// flagged: a store persistently over its quota never sees an un-throttled
// interval, so the auto base rate stays at the seed and the limit is a sane,
// non-zero floor rather than stalling or exploding.
func TestWriteThrottleNeverLearnedUsesSeed(t *testing.T) {
	var phys atomic.Int64
	w := NewWriteThrottle(DefaultThrottleParams(1000, 0),
		func() int64 { return phys.Load() }, func() int64 { return 0 }, 0)
	require.False(t, w.fixedBase)
	require.Equal(t, throttleDefaultBaseRate, w.baseRate)

	phys.Store(1000) // at quota -> fraction = minFraction; never un-throttled
	w.apply(0)
	require.Equal(t, throttleDefaultBaseRate, w.baseRate, "seed unchanged")
	got := float64(w.lim.Limit())
	require.Greater(t, got, 0.0)
	require.InDelta(t, DefaultThrottleMinFraction*throttleDefaultBaseRate, got, 1)
}

func TestThrottleFractionMonotonic(t *testing.T) {
	p := DefaultThrottleParams(1000, 0)
	prev := 2.0
	for size := int64(0); size <= 1200; size += 25 {
		f := p.Fraction(size, 0)
		require.LessOrEqual(t, f, prev+1e-12, "fraction must be non-increasing as size grows")
		require.GreaterOrEqual(t, f, p.MinFraction-1e-12)
		require.LessOrEqual(t, f, 1.0+1e-12)
		prev = f
	}
}
