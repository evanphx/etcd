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
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	// DefaultThrottleSoftStart is the utilization (fraction of a soft quota) at
	// which write throttling begins. Below it, writes run at full speed.
	DefaultThrottleSoftStart = 0.80
	// DefaultThrottleMinFraction is the floor on the permitted write-rate
	// fraction once a soft quota is reached or exceeded. It is intentionally
	// non-zero: a soft quota throttles hard but never fully stops writes (the
	// only hard stop is the physical NOSPACE backstop, handled elsewhere).
	DefaultThrottleMinFraction = 0.02

	// throttleInterval is how often the controller recomputes the shared rate
	// limit from current utilization. Short enough to react, long enough that
	// the recompute cost is negligible on the write path.
	throttleInterval = 200 * time.Millisecond
	// throttleDefaultBaseRate is the assumed full-speed write rate (ops/sec)
	// used until the controller has observed the workload's natural rate during
	// an un-throttled interval.
	throttleDefaultBaseRate = 5000.0
	// throttleMinRateFloor is an absolute floor (ops/sec) on the throttled rate
	// so writes never stall completely even if the measured base rate is tiny.
	throttleMinRateFloor = 50.0
	// throttleBurst is the token-bucket burst for the shared limiter. It lets
	// short bursts through while the sustained rate is held to the target.
	throttleBurst = 100
)

// ThrottleParams is the policy for the write-path backpressure controller. It
// maps the current physical and logical sizes to a permitted fraction of the
// base write rate, from two independent soft quotas:
//
//   - PhysicalQuota: the backend's on-disk Size() vs --quota-backend-bytes. As
//     physical usage climbs (revision history + engine slack: bbolt freelist or
//     Pebble compaction backlog), throttling buys background reclamation time.
//   - LogicalQuota:  the live-keyspace bytes vs --quota-logical-bytes. As real
//     data approaches the limit, throttling governs smooth growth.
//
// The more-constrained signal governs (max utilization). The mapping is pure so
// it can be unit-tested and tuned independently of the rate-limiting mechanism
// that consumes it.
type ThrottleParams struct {
	// PhysicalQuota is the soft physical (on-disk) limit in bytes; <=0 disables
	// the physical signal.
	PhysicalQuota int64
	// LogicalQuota is the soft logical (live-keyspace) limit in bytes; <=0
	// disables the logical signal.
	LogicalQuota int64
	// SoftStart is the utilization in [0,1) at which throttling begins.
	SoftStart float64
	// MinFraction is the floor on the permitted rate fraction in (0,1].
	MinFraction float64
}

// NewThrottleParams builds the policy for the given soft quotas and curve. An
// out-of-range softStart (not in (0,1)) or minFraction (not in (0,1]) falls back
// to the default, so a zero/unset config value picks the default. A zero quota
// leaves that signal disabled.
func NewThrottleParams(physicalQuota, logicalQuota int64, softStart, minFraction float64) ThrottleParams {
	if softStart <= 0 || softStart >= 1 {
		softStart = DefaultThrottleSoftStart
	}
	if minFraction <= 0 || minFraction > 1 {
		minFraction = DefaultThrottleMinFraction
	}
	return ThrottleParams{
		PhysicalQuota: physicalQuota,
		LogicalQuota:  logicalQuota,
		SoftStart:     softStart,
		MinFraction:   minFraction,
	}
}

// DefaultThrottleParams returns the policy for the given soft quotas using the
// default curve. A zero quota leaves that signal disabled.
func DefaultThrottleParams(physicalQuota, logicalQuota int64) ThrottleParams {
	return NewThrottleParams(physicalQuota, logicalQuota, 0, 0)
}

// utilization returns size/quota clamped at 0 when the quota is disabled.
func utilization(size, quota int64) float64 {
	if quota <= 0 || size <= 0 {
		return 0
	}
	return float64(size) / float64(quota)
}

// Utilization returns the governing utilization: the larger of the physical and
// logical utilizations. 0 means both signals are disabled or empty.
func (p ThrottleParams) Utilization(physical, logical int64) float64 {
	return max(utilization(physical, p.PhysicalQuota), utilization(logical, p.LogicalQuota))
}

// Fraction returns the permitted fraction of the base write rate in
// [MinFraction, 1] for the current sizes. It is 1 (full speed) below SoftStart,
// ramps linearly down to MinFraction as the governing utilization rises from
// SoftStart to 1.0, and holds at MinFraction at or above 1.0.
func (p ThrottleParams) Fraction(physical, logical int64) float64 {
	u := p.Utilization(physical, logical)
	if u <= p.SoftStart {
		return 1
	}
	if u >= 1 {
		return p.MinFraction
	}
	// Linear ramp: 1 at SoftStart, MinFraction at 1.0.
	t := (u - p.SoftStart) / (1 - p.SoftStart)
	return 1 - t*(1-p.MinFraction)
}

// Throttled reports whether any throttling applies at the current sizes.
func (p ThrottleParams) Throttled(physical, logical int64) bool {
	return p.Fraction(physical, logical) < 1
}

// WriteThrottle applies the ThrottleParams policy as client-facing backpressure
// on the write path. All writers share a single token-bucket rate limiter so
// the aggregate write rate — not the per-request delay — is what gets capped;
// this is essential under concurrency (N writers each sleeping would multiply
// the intended rate by N).
//
// The limit is recomputed at most once per throttleInterval, inline on the
// write path (one caller wins the interval via a CAS, the rest skip it), so no
// background goroutine or lifecycle management is needed. baseRate — the
// workload's natural full-speed write rate — is learned from intervals where
// nothing is throttled, then held frozen while throttling so the cap is a
// fraction of true capacity rather than a fraction of the already-throttled
// rate (which would spiral downward).
type WriteThrottle struct {
	params   ThrottleParams
	physical func() int64
	logical  func() int64
	disabled bool

	lim *rate.Limiter

	// lastUpdateNano and admitted are read/written atomically from the write
	// path. baseRate is only touched by the interval's CAS winner.
	lastUpdateNano int64
	admitted       int64
	baseRate       float64
}

// NewWriteThrottle builds a throttle for the given policy, reading current sizes
// via physical (backend on-disk Size) and logical (mvcc live-keyspace bytes).
// When both quotas are disabled the throttle is a no-op.
func NewWriteThrottle(params ThrottleParams, physical, logical func() int64) *WriteThrottle {
	w := &WriteThrottle{
		params:   params,
		physical: physical,
		logical:  logical,
		disabled: params.PhysicalQuota <= 0 && params.LogicalQuota <= 0,
		baseRate: throttleDefaultBaseRate,
	}
	if !w.disabled {
		w.lim = rate.NewLimiter(rate.Inf, throttleBurst)
	}
	return w
}

// Wait blocks the caller until the shared limiter admits one write, applying
// backpressure proportional to the current soft-quota utilization. It returns
// ctx.Err() if the context is cancelled while waiting. It is a no-op when the
// throttle is disabled or nothing is currently throttled (limit is +Inf).
//
// It uses Reserve+sleep rather than rate.Limiter.Wait: Wait rejects outright
// when the predicted delay exceeds the context deadline, and a client that
// retries on that error spins, flooding the limiter and cascading into more
// rejections. Reserve always yields a bounded delay (a synchronous caller has
// at most one reservation outstanding, so the delay is ~concurrency/rate), so
// each writer simply blocks its turn — true backpressure, no spin.
func (w *WriteThrottle) Wait(ctx context.Context) error {
	if w.disabled {
		return nil
	}
	atomic.AddInt64(&w.admitted, 1)
	w.maybeUpdate()

	r := w.lim.Reserve()
	if !r.OK() {
		// Only happens if the burst is too small to ever satisfy the request;
		// with throttleBurst>=1 it cannot. Admit rather than block forever.
		return nil
	}
	delay := r.Delay()
	if delay <= 0 {
		return nil
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		r.Cancel()
		return ctx.Err()
	}
}

// maybeUpdate recomputes the shared limit once per interval; the CAS ensures a
// single caller does the work while the rest return immediately.
func (w *WriteThrottle) maybeUpdate() {
	now := time.Now().UnixNano()
	last := atomic.LoadInt64(&w.lastUpdateNano)
	if now-last < int64(throttleInterval) {
		return
	}
	if !atomic.CompareAndSwapInt64(&w.lastUpdateNano, last, now) {
		return
	}
	if last == 0 {
		// First call: seed the interval clock without a rate sample.
		atomic.StoreInt64(&w.admitted, 0)
		return
	}
	dt := float64(now-last) / float64(time.Second)
	observed := float64(atomic.SwapInt64(&w.admitted, 0)) / dt
	w.apply(observed)
}

// apply sets the shared limit from the current utilization. When nothing is
// throttled it lifts the limit (+Inf) and folds the observed rate into the
// learned base rate; otherwise it caps at fraction*baseRate (with an absolute
// floor) and leaves baseRate frozen.
func (w *WriteThrottle) apply(observed float64) {
	frac := w.params.Fraction(w.physical(), w.logical())
	if frac >= 1 {
		switch {
		case observed > w.baseRate:
			w.baseRate = observed // ratchet up to newly seen capacity
		case observed > 0:
			w.baseRate = 0.7*w.baseRate + 0.3*observed // decay slowly
		}
		w.lim.SetLimit(rate.Inf)
		return
	}
	target := frac * w.baseRate
	if target < throttleMinRateFloor {
		target = throttleMinRateFloor
	}
	w.lim.SetLimit(rate.Limit(target))
}
