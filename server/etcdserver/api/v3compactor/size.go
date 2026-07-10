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
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
)

// SizeCompactionThreshold is the fraction of the backend quota at (or above)
// which the size compactor triggers a compaction. Default 0.9 ("within 10% of
// the max").
var SizeCompactionThreshold = 0.9

// SizeCheckInterval is how often the size compactor samples the backend size.
// It is a var so tests can shorten it.
var SizeCheckInterval = 500 * time.Millisecond

// SizeCompactionSettleDelay is how long to wait after scheduling a compaction
// (whose revision deletions are applied asynchronously) before defragging to
// reclaim the freed space.
var SizeCompactionSettleDelay = 300 * time.Millisecond

// Size is a reactive compactor: when the backend size reaches
// SizeCompactionThreshold of the quota, it compacts (keeping the last
// `retention` revisions) and defrags to reclaim the space, as a safety net
// against the NOSPACE alarm. Unlike periodic/revision compaction it is driven by
// actual size, not a schedule.
type Size struct {
	lg *zap.Logger

	clock     clockwork.Clock
	retention int64 // revisions to keep after compaction
	maxBytes  int64 // backend quota

	rg RevGetter
	c  Compactable
	sg SizeGetter
	df Defragger

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	paused bool
}

func newSize(lg *zap.Logger, clock clockwork.Clock, retention int64, rg RevGetter, c Compactable, sg SizeGetter, df Defragger, maxBytes int64) *Size {
	sc := &Size{
		lg:        lg,
		clock:     clock,
		retention: retention,
		maxBytes:  maxBytes,
		rg:        rg,
		c:         c,
		sg:        sg,
		df:        df,
	}
	sc.ctx, sc.cancel = context.WithCancel(context.Background())
	return sc
}

// Run runs the size-based compactor.
func (sc *Size) Run() {
	threshold := int64(float64(sc.maxBytes) * SizeCompactionThreshold)
	prev := int64(0)
	go func() {
		for {
			select {
			case <-sc.ctx.Done():
				return
			case <-sc.clock.After(SizeCheckInterval):
				sc.mu.Lock()
				p := sc.paused
				sc.mu.Unlock()
				if p {
					continue
				}
			}

			size := sc.sg.Size()
			if size < threshold {
				continue
			}

			rev := sc.rg.Rev() - sc.retention
			if rev <= 0 || rev <= prev {
				continue
			}

			now := time.Now()
			sc.lg.Info(
				"starting auto size compaction",
				zap.Int64("size-bytes", size),
				zap.Int64("threshold-bytes", threshold),
				zap.Int64("revision", rev),
			)
			_, err := sc.c.Compact(sc.ctx, &pb.CompactionRequest{Revision: rev})
			if err != nil && !errors.Is(err, mvcc.ErrCompacted) {
				sc.lg.Warn("failed auto size compaction", zap.Int64("revision", rev), zap.Error(err))
				continue
			}
			prev = rev

			// The compaction deletes old revisions asynchronously; wait briefly
			// for that to progress, then reclaim so the reported size actually
			// drops. For pebble this defrag is an online compaction; for bbolt it
			// is a stop-the-world file rewrite (writes pause during it).
			select {
			case <-sc.ctx.Done():
				return
			case <-sc.clock.After(SizeCompactionSettleDelay):
			}
			// df is nil for engines that reclaim online (pebble); for bbolt it
			// defrags to actually shrink the file (stop-the-world).
			if sc.df != nil {
				if derr := sc.df.Defrag(); derr != nil {
					sc.lg.Warn("size compaction: defrag failed", zap.Error(derr))
				}
			}
			sc.lg.Info(
				"completed auto size compaction",
				zap.Int64("revision", rev),
				zap.Int64("size-before-bytes", size),
				zap.Int64("size-after-bytes", sc.sg.Size()),
				zap.Duration("took", time.Since(now)),
			)
		}
	}()
}

// Stop stops size-based compactor.
func (sc *Size) Stop() { sc.cancel() }

// Pause pauses size-based compactor.
func (sc *Size) Pause() {
	sc.mu.Lock()
	sc.paused = true
	sc.mu.Unlock()
}

// Resume resumes size-based compactor.
func (sc *Size) Resume() {
	sc.mu.Lock()
	sc.paused = false
	sc.mu.Unlock()
}
