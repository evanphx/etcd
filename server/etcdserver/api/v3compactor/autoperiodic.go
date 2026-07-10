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

package v3compactor

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
)

// AutoPeriodic tuning. These are vars so tests can shorten them.
var (
	// AutoPeriodicHighWater is the fraction of the quota the compactor steers
	// the backend to stay below. It compacts before the *predicted* size reaches
	// this, leaving headroom for the reclaim to take effect.
	AutoPeriodicHighWater = 0.7

	// AutoPeriodicSampleInterval is how often the size/rate is sampled.
	AutoPeriodicSampleInterval = 1 * time.Second

	// AutoPeriodicLeadTime is how far ahead the size is projected when deciding
	// to compact. It should cover the time for a compaction + reclaim to actually
	// lower the reported size (longer for pebble's online reclaim).
	AutoPeriodicLeadTime = 10 * time.Second

	// AutoPeriodicCooldown is the minimum time between compactions, to let a
	// reclaim settle before re-evaluating.
	AutoPeriodicCooldown = 10 * time.Second

	// autoPeriodicRateAlpha is the EWMA smoothing factor for the growth rate.
	autoPeriodicRateAlpha = 0.3
)

// AutoPeriodic is a proactive, workload-adaptive compactor. It samples the
// backend size, estimates the growth rate (bytes/sec), and compacts (keeping the
// last `retention` revisions) when the size is *projected* to reach
// AutoPeriodicHighWater of the quota within AutoPeriodicLeadTime. Fast workloads
// compact frequently, idle ones rarely, and the size is steered to stay well
// below the quota so the reclaim always has room and time — unlike the reactive
// size mode which fires only once the quota is nearly reached.
type AutoPeriodic struct {
	lg *zap.Logger

	clock     clockwork.Clock
	retention int64
	maxBytes  int64

	rg RevGetter
	c  Compactable
	sg SizeGetter

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	paused bool
}

func newAutoPeriodic(lg *zap.Logger, clock clockwork.Clock, retention int64, rg RevGetter, c Compactable, sg SizeGetter, maxBytes int64) *AutoPeriodic {
	ac := &AutoPeriodic{
		lg:        lg,
		clock:     clock,
		retention: retention,
		maxBytes:  maxBytes,
		rg:        rg,
		c:         c,
		sg:        sg,
	}
	ac.ctx, ac.cancel = context.WithCancel(context.Background())
	return ac
}

// Run runs the auto-periodic compactor.
func (ac *AutoPeriodic) Run() {
	highWater := int64(AutoPeriodicHighWater * float64(ac.maxBytes))
	var (
		lastSize      int64
		lastTime      time.Time
		rate          float64 // smoothed bytes/sec, growth only
		prev          int64
		cooldownUntil time.Time
	)
	go func() {
		for {
			select {
			case <-ac.ctx.Done():
				return
			case <-ac.clock.After(AutoPeriodicSampleInterval):
			}
			ac.mu.Lock()
			p := ac.paused
			ac.mu.Unlock()
			if p {
				continue
			}

			now := ac.clock.Now()
			size := ac.sg.Size()

			// Update the smoothed growth rate (ignore drops from compaction).
			if !lastTime.IsZero() {
				if dt := now.Sub(lastTime).Seconds(); dt > 0 {
					inst := float64(size-lastSize) / dt
					if inst < 0 {
						inst = 0
					}
					if rate == 0 {
						rate = inst
					} else {
						rate = autoPeriodicRateAlpha*inst + (1-autoPeriodicRateAlpha)*rate
					}
				}
			}
			lastSize, lastTime = size, now

			if now.Before(cooldownUntil) {
				continue
			}

			// Project the size LeadTime into the future; compact if it would
			// reach the high-water mark by then.
			projected := float64(size) + rate*AutoPeriodicLeadTime.Seconds()
			if projected < float64(highWater) {
				continue
			}

			rev := ac.rg.Rev() - ac.retention
			if rev <= 0 || rev <= prev {
				continue
			}

			eta := time.Duration(math.MaxInt64)
			if rate > 0 {
				eta = time.Duration(float64(highWater-size)/rate) * time.Second
			}
			start := time.Now()
			ac.lg.Info(
				"starting auto-periodic compaction",
				zap.Int64("size-bytes", size),
				zap.Int64("high-water-bytes", highWater),
				zap.Float64("growth-bytes-per-sec", rate),
				zap.Duration("projected-time-to-high-water", eta),
				zap.Int64("revision", rev),
			)
			_, err := ac.c.Compact(ac.ctx, &pb.CompactionRequest{Revision: rev})
			if err != nil && !errors.Is(err, mvcc.ErrCompacted) {
				ac.lg.Warn("failed auto-periodic compaction", zap.Int64("revision", rev), zap.Error(err))
				continue
			}
			prev = rev

			cooldownUntil = ac.clock.Now().Add(AutoPeriodicCooldown)
			ac.lg.Info(
				"completed auto-periodic compaction",
				zap.Int64("revision", rev),
				zap.Int64("size-before-bytes", size),
				zap.Int64("size-after-bytes", ac.sg.Size()),
				zap.Duration("took", time.Since(start)),
			)
		}
	}()
}

// Stop stops the auto-periodic compactor.
func (ac *AutoPeriodic) Stop() { ac.cancel() }

// Pause pauses the auto-periodic compactor.
func (ac *AutoPeriodic) Pause() {
	ac.mu.Lock()
	ac.paused = true
	ac.mu.Unlock()
}

// Resume resumes the auto-periodic compactor.
func (ac *AutoPeriodic) Resume() {
	ac.mu.Lock()
	ac.paused = false
	ac.mu.Unlock()
}
