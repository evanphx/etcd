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
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
)

// pebbleBatchTx is the Pebble analogue of batchTx. It accumulates writes in a
// single indexed Pebble batch (so reads through the tx observe pending writes,
// matching bbolt's writable-tx read-your-writes) and commits periodically.
type pebbleBatchTx struct {
	sync.Mutex
	backend *pebbleBackend
	batch   *pebble.Batch

	pending int
}

// Lock is supposed to be called only by the unit test.
func (t *pebbleBatchTx) Lock() {
	ValidateCalledInsideUnittest(t.backend.lg)
	t.lock()
}

func (t *pebbleBatchTx) lock() {
	t.Mutex.Lock()
}

func (t *pebbleBatchTx) LockInsideApply() {
	t.lock()
	if t.backend.txPostLockInsideApplyHook != nil {
		ValidateCalledInsideApply(t.backend.lg)
		t.backend.txPostLockInsideApplyHook()
	}
}

func (t *pebbleBatchTx) LockOutsideApply() {
	ValidateCalledOutSideApply(t.backend.lg)
	t.lock()
}

func (t *pebbleBatchTx) Unlock() {
	if t.pending >= t.backend.batchLimit {
		t.commit(false)
	}
	t.Mutex.Unlock()
}

func (t *pebbleBatchTx) UnsafeCreateBucket(bucket Bucket) {
	// Buckets are implicit key prefixes in Pebble; nothing to create. We still
	// count it as a pending op so bucket-creation participates in commit sizing.
	t.pending++
}

func (t *pebbleBatchTx) UnsafeDeleteBucket(bucket Bucket) {
	if err := t.batch.DeleteRange(bucketLowerBound(bucket), bucketUpperBound(bucket), nil); err != nil {
		t.backend.lg.Fatal(
			"failed to delete a bucket",
			zap.Stringer("bucket-name", bucket),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *pebbleBatchTx) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value)
}

// UnsafeSeqPut behaves identically to UnsafePut on Pebble (there is no B+tree
// fill-percent to tune); the distinct method is retained so the buffered layer
// can keep its sequential-write fast path.
func (t *pebbleBatchTx) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value)
}

func (t *pebbleBatchTx) unsafePut(bucket Bucket, key []byte, value []byte) {
	if err := t.batch.Set(physKey(bucket, key), value, nil); err != nil {
		t.backend.lg.Fatal(
			"failed to write to a bucket",
			zap.Stringer("bucket-name", bucket),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx. It reads through the
// indexed batch, so pending (uncommitted) writes are visible.
func (t *pebbleBatchTx) UnsafeRange(bucket Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	return pebbleRange(t.backend.lg, t.batch, bucket, key, endKey, limit)
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *pebbleBatchTx) UnsafeDelete(bucket Bucket, key []byte) {
	if err := t.batch.Delete(physKey(bucket, key), nil); err != nil {
		t.backend.lg.Fatal(
			"failed to delete a key",
			zap.Stringer("bucket-name", bucket),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *pebbleBatchTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	return pebbleForEach(t.backend.lg, t.batch, bucket, visitor)
}

// Commit commits a previous tx and begins a new writable one.
func (t *pebbleBatchTx) Commit() {
	t.lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *pebbleBatchTx) CommitAndStop() {
	t.lock()
	t.commit(true)
	t.Unlock()
}

func (t *pebbleBatchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

func (t *pebbleBatchTx) commit(stop bool) {
	if t.batch != nil {
		if t.pending == 0 && !stop {
			return
		}
		if err := t.batch.Commit(t.backend.writeOptions()); err != nil {
			t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
		}
		atomic.AddInt64(&t.backend.commits, 1)
		t.pending = 0
		if err := t.batch.Close(); err != nil {
			t.backend.lg.Fatal("failed to close committed batch", zap.Error(err))
		}
		t.batch = nil
		t.backend.updateSize()
	}
	if !stop {
		t.batch = t.backend.db.NewIndexedBatch()
	}
}

// pebbleBatchTxBuffered layers the in-memory write buffer (reused verbatim from
// the bbolt implementation) over pebbleBatchTx, mirroring batchTxBuffered.
type pebbleBatchTxBuffered struct {
	pebbleBatchTx
	buf                     txWriteBuffer
	pendingDeleteOperations int
}

func newPebbleBatchTxBuffered(backend *pebbleBackend) *pebbleBatchTxBuffered {
	tx := &pebbleBatchTxBuffered{
		pebbleBatchTx: pebbleBatchTx{backend: backend},
		buf: txWriteBuffer{
			txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
			bucket2seq: make(map[BucketID]bool),
		},
	}
	tx.Commit()
	return tx
}

func (t *pebbleBatchTxBuffered) Unlock() {
	if t.pending != 0 {
		t.backend.readTx.Lock() // blocks txReadBuffer for writing.
		t.buf.writeback(&t.backend.readTx.buf)
		t.backend.readTx.Unlock()
		// Commit at the batch limit, and always when a delete is pending: the
		// read buffer does not carry deletes, so a pending delete must be made
		// durable (and visible to the next snapshot) to preserve linearizable
		// reads. See batchTxBuffered.Unlock for the full rationale.
		if t.pending >= t.backend.batchLimit || t.pendingDeleteOperations > 0 {
			t.commit(false)
		}
	}
	t.pebbleBatchTx.Unlock()
}

func (t *pebbleBatchTxBuffered) Commit() {
	t.lock()
	t.commit(false)
	t.Unlock()
}

func (t *pebbleBatchTxBuffered) CommitAndStop() {
	t.lock()
	t.commit(true)
	t.Unlock()
}

func (t *pebbleBatchTxBuffered) commit(stop bool) {
	// All read txs must observe a consistent point: take the readTx write lock
	// while we roll the snapshot.
	t.backend.readTx.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.Unlock()
}

func (t *pebbleBatchTxBuffered) unsafeCommit(stop bool) {
	if t.backend.hooks != nil {
		// gofail: var commitBeforePreCommitHook struct{}
		t.backend.hooks.OnPreCommitUnsafe(t)
		// gofail: var commitAfterPreCommitHook struct{}
	}

	// Detach the current read snapshot and close it once all readers relying on
	// it have finished (mirrors the async bolt read-tx rollback). snapWg lets
	// Close wait for these goroutines before closing the DB.
	if t.backend.readTx.snap != nil {
		t.backend.snapWg.Add(1)
		go func(s *pebble.Snapshot, wg *sync.WaitGroup) {
			defer t.backend.snapWg.Done()
			wg.Wait()
			_ = s.Close()
		}(t.backend.readTx.snap, t.backend.readTx.txWg)
		t.backend.readTx.reset()
	}

	t.pebbleBatchTx.commit(stop)
	t.pendingDeleteOperations = 0

	if !stop {
		t.backend.readTx.snap = t.backend.db.NewSnapshot()
	}
}

func (t *pebbleBatchTxBuffered) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.pebbleBatchTx.UnsafePut(bucket, key, value)
	t.buf.put(bucket, key, value)
}

func (t *pebbleBatchTxBuffered) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.pebbleBatchTx.UnsafeSeqPut(bucket, key, value)
	t.buf.putSeq(bucket, key, value)
}

func (t *pebbleBatchTxBuffered) UnsafeDelete(bucket Bucket, key []byte) {
	t.pebbleBatchTx.UnsafeDelete(bucket, key)
	t.pendingDeleteOperations++
}

func (t *pebbleBatchTxBuffered) UnsafeDeleteBucket(bucket Bucket) {
	t.pebbleBatchTx.UnsafeDeleteBucket(bucket)
	t.pendingDeleteOperations++
}
