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

package mvcc

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
)

func tortureEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

type tortureStore struct {
	t      *testing.T
	engine backend.Engine
	path   string
	be     backend.Backend
	s      *store
}

func newTortureStore(t *testing.T, engine backend.Engine) *tortureStore {
	path := filepath.Join(t.TempDir(), "db")
	ts := &tortureStore{t: t, engine: engine, path: path}
	ts.open()
	return ts
}

func (ts *tortureStore) open() {
	ts.be = newEngineBackend(ts.t, ts.engine, ts.path)
	ts.s = NewStore(zaptest.NewLogger(ts.t), ts.be, &lease.FakeLessor{}, StoreConfig{})
}

func (ts *tortureStore) reopen() {
	require.NoError(ts.t, ts.s.Close())
	require.NoError(ts.t, ts.be.Close())
	ts.open() // NewStore rebuilds the treeIndex from the backend (restore path)
}

// rangeAll returns all live key/values with their revisions, as comparable strings.
func (ts *tortureStore) rangeAll() []string {
	r, err := ts.s.Range(ts.t.Context(), []byte("key"), []byte("kez"), RangeOptions{})
	require.NoError(ts.t, err)
	out := make([]string, 0, len(r.KVs))
	for _, kv := range r.KVs {
		out = append(out, fmt.Sprintf("%s=%s cr=%d mr=%d ver=%d", kv.Key, kv.Value, kv.CreateRevision, kv.ModRevision, kv.Version))
	}
	return out
}

// TestMVCCDifferentialTorture replays one deterministic, randomized stream of
// mvcc operations (put/delete/txn/compact/reopen) against a store on each
// engine and asserts they stay bit-identical: returned revisions, full range
// results, current revision, and — the strongest check — HashByRev, which is
// computed engine-agnostically over stored revision keys and values and must
// therefore match exactly between bbolt and pebble.
func TestMVCCDifferentialTorture(t *testing.T) {
	seeds := []int64{0xBEEF, 7, 99, 0xF00D}
	if testing.Short() {
		seeds = seeds[:1]
	}
	base := int64(tortureEnvInt("TORTURE_SEED_BASE", 0))
	for _, seed := range seeds {
		seed += base
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runMVCCDifferential(t, seed)
		})
	}
}

func runMVCCDifferential(t *testing.T, seed int64) {
	ops := tortureEnvInt("TORTURE_OPS", 4000)
	if testing.Short() {
		ops = 800
	}
	rng := rand.New(rand.NewSource(seed))

	b := newTortureStore(t, backend.EngineBBolt)
	p := newTortureStore(t, backend.EnginePebble)
	defer func() { b.s.Close(); b.be.Close() }()
	defer func() { p.s.Close(); p.be.Close() }()

	const nkeys = 25
	key := func(i int) []byte { return []byte(fmt.Sprintf("key%03d", i)) }

	var compactedRev int64
	checkSame := func(stage string) {
		require.Equal(t, b.s.Rev(), p.s.Rev(), "Rev mismatch at %s", stage)
		require.Equal(t, b.rangeAll(), p.rangeAll(), "range mismatch at %s", stage)
		rev := b.s.Rev()
		if rev > compactedRev {
			hb, _, err := b.s.hashByRev(rev)
			require.NoError(t, err)
			hp, _, err := p.s.hashByRev(rev)
			require.NoError(t, err)
			require.Equalf(t, hb.Hash, hp.Hash, "HashByRev(%d) mismatch at %s", rev, stage)
		}
	}

	for i := 0; i < ops; i++ {
		switch r := rng.Intn(100); {
		case r < 55: // put
			k := key(rng.Intn(nkeys))
			v := []byte(fmt.Sprintf("v%d", rng.Intn(1_000_000)))
			rb := b.s.Put(k, v, lease.NoLease)
			rp := p.s.Put(k, v, lease.NoLease)
			require.Equalf(t, rb, rp, "Put rev mismatch op %d", i)
		case r < 75: // delete range [k, k+1)
			k := key(rng.Intn(nkeys))
			nb, rvb := b.s.DeleteRange(k, nil)
			np, rvp := p.s.DeleteRange(k, nil)
			require.Equalf(t, nb, np, "DeleteRange count mismatch op %d", i)
			require.Equalf(t, rvb, rvp, "DeleteRange rev mismatch op %d", i)
		case r < 82: // multi-put "txn"-ish burst (several keys at once)
			for j := 0; j < 3; j++ {
				k := key(rng.Intn(nkeys))
				v := []byte(fmt.Sprintf("burst%d", rng.Intn(1_000_000)))
				require.Equal(t, b.s.Put(k, v, lease.NoLease), p.s.Put(k, v, lease.NoLease))
			}
		case r < 88: // compact to a revision at/below current
			rev := b.s.Rev()
			if rev > compactedRev+1 {
				target := compactedRev + 1 + int64(rng.Intn(int(rev-compactedRev)))
				chb, err := b.s.Compact(traceutil.TODO(), target)
				require.NoError(t, err)
				chp, err := p.s.Compact(traceutil.TODO(), target)
				require.NoError(t, err)
				<-chb
				<-chp
				compactedRev = target
				b.be.ForceCommit()
				p.be.ForceCommit()
				checkSame(fmt.Sprintf("op %d (compact %d)", i, target))
			}
		case r < 90 && !testing.Short(): // reopen both (restore parity)
			b.reopen()
			p.reopen()
			checkSame(fmt.Sprintf("op %d (reopen)", i))
		}

		if i%200 == 0 {
			checkSame(fmt.Sprintf("op %d", i))
		}
	}
	checkSame("final")

	b.reopen()
	p.reopen()
	checkSame("post-final-reopen")
}
