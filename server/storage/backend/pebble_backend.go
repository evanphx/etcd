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

package backend

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
)

// pebbleBackend implements Backend on top of CockroachDB's Pebble (LSM).
//
// It reuses the in-memory read/write buffer overlay (tx_buffer.go) and mirrors
// the bbolt backend's transaction lifecycle: a single buffered batch tx is held
// open across many applies and committed periodically; reads are served from
// the buffer layered over a rolling Pebble snapshot. The consistent-index hook
// (OnPreCommitUnsafe) writes into the same Pebble batch that is committed, so
// the atomic-durable-unit contract is preserved.
//
// Opt-in via --backend-engine=pebble. See pebble_batch_tx.go and
// pebble_read_tx.go for the tx/read implementations.
type pebbleBackend struct {
	// size, sizeInUse, commits, openReadTxN are used atomically and must stay
	// 64-bit aligned (kept first for 32-bit targets).
	size        int64
	sizeInUse   int64
	commits     int64
	openReadTxN int64

	mu    sync.RWMutex
	db    *pebble.DB
	cache *pebble.Cache
	// path is the Pebble store directory.
	path string

	batchInterval time.Duration
	batchLimit    int
	batchTx       *pebbleBatchTxBuffered

	readTx            *pebbleReadTx
	txReadBufferCache txReadBufferCache

	stopc chan struct{}
	donec chan struct{}

	// snapWg tracks async snapshot-close goroutines so Close can wait for all
	// snapshots to be released before closing the DB (Pebble refuses to close
	// with open snapshots).
	snapWg sync.WaitGroup

	hooks Hooks

	// txPostLockInsideApplyHook is called each time right after locking the tx.
	txPostLockInsideApplyHook func()

	unsafeNoFsync bool
	lg            *zap.Logger
}

func newPebbleBackend(bcfg BackendConfig) Backend {
	if bcfg.Logger == nil {
		bcfg.Logger = zap.NewNop()
	}
	lg := bcfg.Logger

	opts := &pebble.Options{
		Logger: &pebbleZapLogger{lg: lg.Named("pebble")},
	}
	// Apply the memory-profile knobs. Each is applied only when set; zero leaves
	// Pebble's default in place.
	var cache *pebble.Cache
	if bcfg.PebbleCacheBytes > 0 {
		cache = pebble.NewCache(bcfg.PebbleCacheBytes)
		opts.Cache = cache
	}
	if bcfg.PebbleMemTableBytes > 0 {
		opts.MemTableSize = uint64(bcfg.PebbleMemTableBytes)
	}
	if bcfg.PebbleMemTableStopWritesThreshold > 0 {
		opts.MemTableStopWritesThreshold = bcfg.PebbleMemTableStopWritesThreshold
	}
	if bcfg.PebbleMaxOpenFiles > 0 {
		opts.MaxOpenFiles = bcfg.PebbleMaxOpenFiles
	}
	if bcfg.PebbleMaxConcurrentCompactions > 0 {
		n := bcfg.PebbleMaxConcurrentCompactions
		opts.CompactionConcurrencyRange = func() (int, int) { return 1, n }
	}

	db, err := pebble.Open(bcfg.Path, opts)
	if err != nil {
		if cache != nil {
			cache.Unref()
		}
		lg.Panic("failed to open pebble database", zap.String("path", bcfg.Path), zap.Error(err))
	}

	b := &pebbleBackend{
		db:            db,
		cache:         cache,
		path:          bcfg.Path,
		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,
		unsafeNoFsync: bcfg.UnsafeNoFsync,

		readTx: &pebbleReadTx{
			pebbleBaseReadTx: pebbleBaseReadTx{
				buf: txReadBuffer{
					txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
					bufVersion: 0,
				},
				txMu: new(sync.RWMutex),
				txWg: new(sync.WaitGroup),
				lg:   lg,
			},
		},
		txReadBufferCache: txReadBufferCache{},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),
		lg:    lg,
	}

	b.batchTx = newPebbleBatchTxBuffered(b)
	// Set hooks after the initial (empty) commit so it is skipped.
	b.hooks = bcfg.Hooks

	// Populate size metrics immediately so Size()/SizeInUse() are meaningful for
	// a read-only backend that never commits (e.g. etcdutl snapshot status).
	b.updateSize()

	go b.run()
	return b
}

// writeOptions returns the Pebble write options for a commit: fsync on commit
// unless fsync has been explicitly disabled (UnsafeNoFsync).
func (b *pebbleBackend) writeOptions() *pebble.WriteOptions {
	if b.unsafeNoFsync {
		return pebble.NoSync
	}
	return pebble.Sync
}

// updateSize refreshes the cached size metrics from Pebble.
//
//   - Size (physically allocated) = total on-disk usage including obsolete and
//     zombie tables and the WAL. Quota/NOSPACE keys off this, so it is
//     conservative: space transiently held before compaction counts against the
//     quota (favoring uptime — an early read-only pause over a disk-full crash).
//   - SizeInUse (logically in use) = live SSTable bytes, which excludes the
//     space reclaimable by background compaction.
func (b *pebbleBackend) updateSize() {
	m := b.db.Metrics()
	atomic.StoreInt64(&b.size, int64(m.DiskSpaceUsage()))
	atomic.StoreInt64(&b.sizeInUse, int64(m.Table.Local.LiveSize))
}

// BatchTx returns the current buffered batch tx.
func (b *pebbleBackend) BatchTx() BatchTx {
	return b.batchTx
}

func (b *pebbleBackend) SetTxPostLockInsideApplyHook(hook func()) {
	b.batchTx.lock()
	defer b.batchTx.Unlock()
	b.txPostLockInsideApplyHook = hook
}

func (b *pebbleBackend) ReadTx() ReadTx { return b.readTx }

// ConcurrentReadTx creates a lock-free read view: a copy of the read buffer plus
// the current committed Pebble snapshot, kept alive by txWg until RUnlock. This
// mirrors backend.ConcurrentReadTx, including the read-buffer copy cache.
func (b *pebbleBackend) ConcurrentReadTx() ReadTx {
	b.readTx.RLock()
	defer b.readTx.RUnlock()
	// prevent the snapshot from being closed until the read is done.
	b.readTx.txWg.Add(1)
	atomic.AddInt64(&b.openReadTxN, 1)

	b.txReadBufferCache.mu.Lock()

	curCache := b.txReadBufferCache.buf
	curCacheVer := b.txReadBufferCache.bufVersion
	curBufVer := b.readTx.buf.bufVersion

	isEmptyCache := curCache == nil
	isStaleCache := curCacheVer != curBufVer

	var buf *txReadBuffer
	switch {
	case isEmptyCache:
		curBuf := b.readTx.buf.unsafeCopy()
		buf = &curBuf
	case isStaleCache:
		b.txReadBufferCache.mu.Unlock()
		curBuf := b.readTx.buf.unsafeCopy()
		b.txReadBufferCache.mu.Lock()
		buf = &curBuf
	default:
		buf = curCache
	}
	if isEmptyCache || curCacheVer == b.txReadBufferCache.bufVersion {
		b.txReadBufferCache.buf = buf
		b.txReadBufferCache.bufVersion = curBufVer
	}

	b.txReadBufferCache.mu.Unlock()

	return &pebbleConcurrentReadTx{
		pebbleBaseReadTx: pebbleBaseReadTx{
			buf:         *buf,
			txMu:        b.readTx.txMu,
			snap:        b.readTx.snap,
			txWg:        b.readTx.txWg,
			openReadTxN: &b.openReadTxN,
			lg:          b.lg,
		},
	}
}

// ForceCommit forces the current batching tx to commit.
func (b *pebbleBackend) ForceCommit() {
	b.batchTx.Commit()
}

func (b *pebbleBackend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *pebbleBackend) SizeInUse() int64 {
	return atomic.LoadInt64(&b.sizeInUse)
}

func (b *pebbleBackend) OpenReadTxN() int64 {
	return atomic.LoadInt64(&b.openReadTxN)
}

// Commits returns the total number of commits since start.
func (b *pebbleBackend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}

// Defrag triggers a manual LSM compaction to reclaim obsolete data and
// tombstones. Unlike bbolt's stop-the-world defrag, Pebble compaction is online
// (it does not block reads or writes); Pebble's background compaction also
// performs this automatically over time, so Defrag is rarely required.
//
// It compacts every registered bucket's full prefix range. Deriving the range
// from live keys is not sufficient: a heavy overwrite-then-compact workload
// leaves many point tombstones sorting outside the surviving keys, and those are
// only dropped when their whole key range is compacted.
func (b *pebbleBackend) Defrag() error {
	b.mu.RLock()
	db := b.db
	b.mu.RUnlock()

	buckets := registeredBucketsSorted()
	if len(buckets) == 0 {
		// No registered buckets (e.g. an isolated backend test without schema):
		// fall back to compacting the whole possible keyspace. Bucket IDs are a
		// single prefix byte, so [0x00, 0xff] covers all data.
		if err := db.Compact(context.Background(), []byte{0x00}, []byte{0xff}, true); err != nil {
			return err
		}
		b.updateSize()
		return nil
	}
	for _, bucket := range buckets {
		if err := db.Compact(context.Background(), bucketLowerBound(bucket), bucketUpperBound(bucket), true); err != nil {
			return err
		}
	}
	b.updateSize()
	return nil
}

// Snapshot returns a point-in-time snapshot of the backend as a tar stream of a
// Pebble checkpoint directory. See pebble_snapshot.go.
func (b *pebbleBackend) Snapshot() Snapshot {
	return b.snapshot()
}

func (b *pebbleBackend) run() {
	defer close(b.donec)
	t := time.NewTimer(b.batchInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		if b.batchTx.safePending() != 0 {
			b.batchTx.Commit()
		}
		t.Reset(b.batchInterval)
	}
}

func (b *pebbleBackend) Close() error {
	close(b.stopc)
	<-b.donec
	// CommitAndStop (run loop) detached the final read snapshot for async close;
	// wait for all snapshot-close goroutines so the DB has no open snapshots.
	b.snapWg.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.db.Close()
	if b.cache != nil {
		b.cache.Unref()
	}
	return err
}

// pebbleZapLogger adapts Pebble's Logger interface onto etcd's zap logger.
type pebbleZapLogger struct {
	lg *zap.Logger
}

func (l *pebbleZapLogger) Infof(format string, args ...any) {
	l.lg.Sugar().Infof(format, args...)
}

func (l *pebbleZapLogger) Errorf(format string, args ...any) {
	l.lg.Sugar().Errorf(format, args...)
}

func (l *pebbleZapLogger) Fatalf(format string, args ...any) {
	l.lg.Sugar().Fatalf(format, args...)
}
