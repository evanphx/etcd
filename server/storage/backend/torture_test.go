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

package backend_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// Buckets exercised by the torture tests: a mix of the safe-range Key bucket
// (the only one that permits multi-key range reads) and non-safe buckets.
var tortureBuckets = []backend.Bucket{schema.Key, schema.Lease, schema.Alarm, schema.Meta}

const tortureKeyspace = 40 // number of distinct keys per bucket

func tortureKey(n int) []byte { return []byte(fmt.Sprintf("k%03d", n)) }

// tortureValue encodes the owning bucket and key so any read can self-verify it
// was not torn, misrouted across buckets, or aliased to another key.
func tortureValue(b backend.Bucket, key []byte, seq int) []byte {
	return []byte(fmt.Sprintf("%d|%s|%d", b.ID(), key, seq))
}

func tortureValuePrefix(b backend.Bucket, key []byte) string {
	return fmt.Sprintf("%d|%s|", b.ID(), key)
}

func createTortureBuckets(be backend.Backend) {
	tx := be.BatchTx()
	tx.Lock()
	for _, b := range tortureBuckets {
		tx.UnsafeCreateBucket(b)
	}
	tx.Unlock()
	be.ForceCommit()
}

func dumpBucket(tb testing.TB, rtx backend.ReadTx, b backend.Bucket) map[string]string {
	out := map[string]string{}
	require.NoError(tb, rtx.UnsafeForEach(b, func(k, v []byte) error {
		out[string(k)] = string(v)
		return nil
	}))
	return out
}

func dumpAll(tb testing.TB, be backend.Backend, useConcurrent bool) map[string]map[string]string {
	var rtx backend.ReadTx
	if useConcurrent {
		rtx = be.ConcurrentReadTx()
	} else {
		rtx = be.ReadTx()
	}
	rtx.RLock()
	defer rtx.RUnlock()
	out := map[string]map[string]string{}
	for _, b := range tortureBuckets {
		out[b.String()] = dumpBucket(tb, rtx, b)
	}
	return out
}

// rangeVia performs a read via a fresh ConcurrentReadTx and returns string copies.
func rangeVia(be backend.Backend, bucket backend.Bucket, key, endKey []byte, limit int64) ([]string, []string) {
	rtx := be.ConcurrentReadTx()
	rtx.RLock()
	defer rtx.RUnlock()
	ks, vs := rtx.UnsafeRange(bucket, key, endKey, limit)
	sk := make([]string, len(ks))
	sv := make([]string, len(vs))
	for i := range ks {
		sk[i] = string(ks[i])
		sv[i] = string(vs[i])
	}
	return sk, sv
}

// ---- Differential torture: identical op stream on bbolt and pebble ----

type diffEngine struct {
	t      *testing.T
	engine backend.Engine
	path   string
	be     backend.Backend
}

func newDiffEngine(t *testing.T, engine backend.Engine) *diffEngine {
	path := filepath.Join(t.TempDir(), "db")
	d := &diffEngine{t: t, engine: engine, path: path}
	d.be = newEngineBackend(t, engine, path)
	createTortureBuckets(d.be)
	return d
}

func (d *diffEngine) reopen() {
	require.NoError(d.t, d.be.Close())
	d.be = newEngineBackend(d.t, d.engine, d.path)
}

// TestBackendDifferentialTorture replays one deterministic, randomized stream of
// operations against both engines and asserts they remain observably identical:
// full-state dumps, range/point-get results, ForEach ordering, and behavior
// across ForceCommit / Defrag / reopen. bbolt acts as the oracle for pebble.
func TestBackendDifferentialTorture(t *testing.T) {
	seeds := []int64{0xC0FFEE, 1, 42, 0xDEAD}
	if testing.Short() {
		seeds = seeds[:1]
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runBackendDifferential(t, seed)
		})
	}
}

func runBackendDifferential(t *testing.T, seed int64) {
	ops := 6000
	if testing.Short() {
		ops = 1000
	}
	rng := rand.New(rand.NewSource(seed))

	b := newDiffEngine(t, backend.EngineBBolt)
	p := newDiffEngine(t, backend.EnginePebble)
	// Close the CURRENT backend at exit (reopen replaces b.be/p.be).
	defer func() { b.be.Close() }()
	defer func() { p.be.Close() }()

	seq := 0
	assertSame := func(stage string) {
		b.be.ForceCommit()
		p.be.ForceCommit()
		require.Equal(t, dumpAll(t, b.be, false), dumpAll(t, p.be, false), "state mismatch (ReadTx) at %s", stage)
		require.Equal(t, dumpAll(t, b.be, true), dumpAll(t, p.be, true), "state mismatch (ConcurrentReadTx) at %s", stage)
	}

	both := []*diffEngine{b, p}
	for i := 0; i < ops; i++ {
		switch r := rng.Intn(100); {
		case r < 45: // put
			bucket := tortureBuckets[rng.Intn(len(tortureBuckets))]
			key := tortureKey(rng.Intn(tortureKeyspace))
			seq++
			val := tortureValue(bucket, key, seq)
			useSeq := rng.Intn(2) == 0
			for _, d := range both {
				tx := d.be.BatchTx()
				tx.Lock()
				if useSeq {
					tx.UnsafeSeqPut(bucket, key, val)
				} else {
					tx.UnsafePut(bucket, key, val)
				}
				tx.Unlock()
			}
		case r < 60: // delete
			bucket := tortureBuckets[rng.Intn(len(tortureBuckets))]
			key := tortureKey(rng.Intn(tortureKeyspace))
			for _, d := range both {
				tx := d.be.BatchTx()
				tx.Lock()
				tx.UnsafeDelete(bucket, key)
				tx.Unlock()
			}
		case r < 80: // read-check
			// Commit both engines first: UnsafeRange returns durable results
			// followed by buffered results, so the transient split (which the
			// background commit ticker moves nondeterministically per engine)
			// would otherwise make the *order* differ even when the set matches.
			// etcd only relies on sorted order for the monotonic-key Key bucket;
			// committing makes the durable read path deterministic to diff.
			b.be.ForceCommit()
			p.be.ForceCommit()
			if rng.Intn(2) == 0 { // multi-key range on the safe Key bucket
				lo := rng.Intn(tortureKeyspace)
				hi := lo + rng.Intn(tortureKeyspace)
				limit := int64(rng.Intn(tortureKeyspace + 2))
				bk, bv := rangeVia(b.be, schema.Key, tortureKey(lo), tortureKey(hi), limit)
				pk, pv := rangeVia(p.be, schema.Key, tortureKey(lo), tortureKey(hi), limit)
				require.Equalf(t, bk, pk, "range keys mismatch [%d,%d) limit %d", lo, hi, limit)
				require.Equalf(t, bv, pv, "range vals mismatch [%d,%d) limit %d", lo, hi, limit)
			} else { // point-get on a random bucket
				bucket := tortureBuckets[rng.Intn(len(tortureBuckets))]
				key := tortureKey(rng.Intn(tortureKeyspace))
				bk, bv := rangeVia(b.be, bucket, key, nil, 0)
				pk, pv := rangeVia(p.be, bucket, key, nil, 0)
				require.Equal(t, bk, pk, "point-get keys mismatch")
				require.Equal(t, bv, pv, "point-get vals mismatch")
			}
		case r < 84: // ForceCommit
			b.be.ForceCommit()
			p.be.ForceCommit()
		case r < 86: // Defrag (must not change observable state)
			require.NoError(t, b.be.Defrag())
			require.NoError(t, p.be.Defrag())
			assertSame(fmt.Sprintf("op %d (defrag)", i))
		case r < 88 && !testing.Short(): // reopen (persistence + restore parity)
			b.reopen()
			p.reopen()
			assertSame(fmt.Sprintf("op %d (reopen)", i))
		}

		if i%250 == 0 {
			assertSame(fmt.Sprintf("op %d", i))
		}
	}
	assertSame("final")

	b.reopen()
	p.reopen()
	assertSame("post-final-reopen")
}

// ---- Concurrent model-based torture (per engine, run under -race) ----

type oracle struct {
	mu   sync.Mutex
	data map[backend.BucketID]map[string][]byte
}

func newOracle() *oracle {
	o := &oracle{data: map[backend.BucketID]map[string][]byte{}}
	for _, b := range tortureBuckets {
		o.data[b.ID()] = map[string][]byte{}
	}
	return o
}

// TestBackendConcurrentTorture hammers each engine with concurrent writers and
// concurrent ConcurrentReadTx readers, then checks a shared oracle for exact
// correctness after quiescing (and after reopen), while every concurrent read
// self-verifies its value. Intended to be run with -race.
func TestBackendConcurrentTorture(t *testing.T) {
	for _, engine := range []backend.Engine{backend.EngineBBolt, backend.EnginePebble} {
		t.Run("engine="+string(engine), func(t *testing.T) {
			bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
			bcfg.Engine = engine
			bcfg.Path = filepath.Join(t.TempDir(), "db")
			bcfg.BatchInterval = 2 * time.Millisecond // frequent background commits
			bcfg.BatchLimit = 37                      // frequent size-triggered commits
			be := backend.New(bcfg)
			createTortureBuckets(be)

			o := newOracle()
			perWriter := 4000
			if testing.Short() {
				perWriter = 800
			}
			const writers, readers = 4, 4
			var seq int64

			var writersWG, readersWG sync.WaitGroup
			stop := make(chan struct{})

			for w := 0; w < writers; w++ {
				writersWG.Add(1)
				go func(w int) {
					defer writersWG.Done()
					rng := rand.New(rand.NewSource(int64(w) + 1))
					for i := 0; i < perWriter; i++ {
						bucket := tortureBuckets[rng.Intn(len(tortureBuckets))]
						key := tortureKey(rng.Intn(tortureKeyspace))
						o.mu.Lock()
						tx := be.BatchTx()
						tx.Lock()
						if rng.Intn(4) == 0 {
							tx.UnsafeDelete(bucket, key)
							delete(o.data[bucket.ID()], string(key))
						} else {
							val := tortureValue(bucket, key, int(atomic.AddInt64(&seq, 1)))
							tx.UnsafePut(bucket, key, val)
							o.data[bucket.ID()][string(key)] = val
						}
						tx.Unlock()
						o.mu.Unlock()
						if i%50 == 0 {
							be.ForceCommit()
						}
					}
				}(w)
			}

			for rd := 0; rd < readers; rd++ {
				readersWG.Add(1)
				go func(rd int) {
					defer readersWG.Done()
					rng := rand.New(rand.NewSource(int64(rd) + 1000))
					for {
						select {
						case <-stop:
							return
						default:
						}
						bucket := tortureBuckets[rng.Intn(len(tortureBuckets))]
						key := tortureKey(rng.Intn(tortureKeyspace))
						rtx := be.ConcurrentReadTx()
						rtx.RLock()
						ks, vs := rtx.UnsafeRange(bucket, key, nil, 0)
						rtx.RUnlock()
						if len(ks) == 1 {
							assert.Truef(t, strings.HasPrefix(string(vs[0]), tortureValuePrefix(bucket, key)),
								"corrupt/misrouted read: bucket=%s key=%s val=%q", bucket, key, vs[0])
						}
					}
				}(rd)
			}

			writersWG.Wait() // writers finish their fixed budget (readers still running)
			close(stop)      // stop readers
			readersWG.Wait()

			be.ForceCommit()

			o.mu.Lock()
			defer o.mu.Unlock()
			verifyAgainstOracle(t, be, o, "after concurrent phase")

			require.NoError(t, be.Close())
			be2 := backend.New(bcfg)
			defer be2.Close()
			verifyAgainstOracle(t, be2, o, "after reopen")
		})
	}
}

func verifyAgainstOracle(t *testing.T, be backend.Backend, o *oracle, stage string) {
	for _, concurrent := range []bool{false, true} {
		got := dumpAll(t, be, concurrent)
		for _, b := range tortureBuckets {
			want := map[string]string{}
			for k, v := range o.data[b.ID()] {
				want[k] = string(v)
			}
			require.Equalf(t, want, got[b.String()], "bucket %s mismatch %s (concurrent=%v)", b, stage, concurrent)
		}
	}
}
