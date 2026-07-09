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
	"bytes"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
)

// pebbleReader is the subset of the Pebble read API shared by *pebble.DB,
// *pebble.Snapshot and (indexed) *pebble.Batch. It is the "durable leaf" of the
// read overlay; the in-memory write buffer is layered on top exactly as in the
// bbolt implementation (see read_tx.go, which this mirrors).
type pebbleReader interface {
	NewIter(o *pebble.IterOptions) (*pebble.Iterator, error)
}

// pebbleRange mirrors unsafeRange (batch_tx.go) over a Pebble reader. It returns
// freshly-allocated key/value copies, since Pebble iterator memory is only valid
// until the iterator advances or closes.
func pebbleRange(lg *zap.Logger, r pebbleReader, bucket Bucket, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	lower := physKey(bucket, key)
	var upper []byte
	pointGet := len(endKey) == 0
	if pointGet {
		// Single-key get: bound the range to exactly the requested key.
		limit = 1
		upper = append(append([]byte{}, lower...), 0x00)
	} else {
		upper = physKey(bucket, endKey)
	}

	iter, err := r.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		lg.Fatal("failed to create pebble iterator", zap.Error(err))
	}
	defer iter.Close()

	for ok := iter.SeekGE(lower); ok; ok = iter.Next() {
		pk := iter.Key()
		if pointGet && !bytes.Equal(pk, lower) {
			break
		}
		keys = append(keys, logicalKey(pk))
		v := iter.Value()
		vc := make([]byte, len(v))
		copy(vc, v)
		vals = append(vals, vc)
		if int64(len(keys)) == limit {
			break
		}
	}
	return keys, vals
}

// pebbleForEach mirrors unsafeForEach over a Pebble reader: an ascending scan of
// the whole bucket. Keys/values passed to the visitor are freshly allocated.
func pebbleForEach(lg *zap.Logger, r pebbleReader, bucket Bucket, visitor func(k, v []byte) error) error {
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: bucketLowerBound(bucket),
		UpperBound: bucketUpperBound(bucket),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		k := logicalKey(iter.Key())
		v := iter.Value()
		vc := make([]byte, len(v))
		copy(vc, v)
		if err := visitor(k, vc); err != nil {
			return err
		}
	}
	return nil
}

// pebbleBaseReadTx is the Pebble analogue of baseReadTx. It layers the in-memory
// txReadBuffer (reused verbatim) over a Pebble snapshot for the durable reads.
type pebbleBaseReadTx struct {
	// mu protects accesses to the txReadBuffer.
	mu  sync.RWMutex
	buf txReadBuffer

	// txMu protects snap during the readTx snapshot roll at commit time.
	txMu *sync.RWMutex
	// snap is the point-in-time committed view. For a readTx it is rolled at
	// each commit; for a concurrentReadTx it is a captured pointer kept alive
	// by txWg until the read completes.
	snap *pebble.Snapshot
	// txWg keeps snap from being closed until all reads using it are done.
	txWg *sync.WaitGroup
	// openReadTxN, when non-nil (concurrentReadTx only), is decremented on
	// RUnlock to track the number of in-flight concurrent reads.
	openReadTxN *int64
	lg          *zap.Logger
}

func (rt *pebbleBaseReadTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	if err := rt.buf.ForEach(bucket, getDups); err != nil {
		return err
	}
	rt.txMu.Lock()
	err := pebbleForEach(rt.lg, rt.snap, bucket, visitNoDup)
	rt.txMu.Unlock()
	if err != nil {
		return err
	}
	return rt.buf.ForEach(bucket, visitor)
}

func (rt *pebbleBaseReadTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bucketType.IsSafeRangeBucket() {
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := rt.buf.Range(bucketType, key, endKey, limit)
	if int64(len(keys)) == limit {
		return keys, vals
	}
	rt.txMu.RLock()
	k2, v2 := pebbleRange(rt.lg, rt.snap, bucketType, key, endKey, limit-int64(len(keys)))
	rt.txMu.RUnlock()
	return append(k2, keys...), append(v2, vals...)
}

// pebbleReadTx is the shared, mutable read transaction rolled every commit.
type pebbleReadTx struct {
	pebbleBaseReadTx
}

func (rt *pebbleReadTx) Lock()    { rt.mu.Lock() }
func (rt *pebbleReadTx) Unlock()  { rt.mu.Unlock() }
func (rt *pebbleReadTx) RLock()   { rt.mu.RLock() }
func (rt *pebbleReadTx) RUnlock() { rt.mu.RUnlock() }

// reset clears the buffer and detaches the snapshot; the caller is responsible
// for closing the previous snapshot once txWg drains.
func (rt *pebbleReadTx) reset() {
	rt.buf.reset()
	rt.snap = nil
	rt.txWg = new(sync.WaitGroup)
}

// pebbleConcurrentReadTx is a lock-free read view over a captured snapshot.
type pebbleConcurrentReadTx struct {
	pebbleBaseReadTx
}

func (rt *pebbleConcurrentReadTx) Lock()   {}
func (rt *pebbleConcurrentReadTx) Unlock() {}
func (rt *pebbleConcurrentReadTx) RLock()  {}

// RUnlock signals the end of the concurrent read.
func (rt *pebbleConcurrentReadTx) RUnlock() {
	if rt.openReadTxN != nil {
		atomic.AddInt64(rt.openReadTxN, -1)
	}
	rt.txWg.Done()
}
