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
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// engines under test. Every parity test runs against both so the Pebble backend
// is held to the exact same observable behavior as bbolt.
var engines = []backend.Engine{backend.EngineBBolt, backend.EnginePebble}

// newEngineBackend builds a backend for the given engine at a stable path so the
// same path can be reopened.
func newEngineBackend(t *testing.T, engine backend.Engine, path string) backend.Backend {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
	bcfg.Engine = engine
	bcfg.Path = path
	bcfg.BatchInterval = 10 * time.Millisecond
	bcfg.BatchLimit = 10000
	return backend.New(bcfg)
}

func forEachEngine(t *testing.T, fn func(t *testing.T, engine backend.Engine)) {
	for _, engine := range engines {
		t.Run("engine="+string(engine), func(t *testing.T) {
			fn(t, engine)
		})
	}
}

func TestEnginePutGetRange(t *testing.T) {
	forEachEngine(t, func(t *testing.T, engine backend.Engine) {
		be := newEngineBackend(t, engine, filepath.Join(t.TempDir(), "db"))
		defer be.Close()

		tx := be.BatchTx()
		tx.Lock()
		tx.UnsafeCreateBucket(schema.Key)
		tx.UnsafePut(schema.Key, []byte("k1"), []byte("v1"))
		tx.UnsafePut(schema.Key, []byte("k2"), []byte("v2"))
		tx.UnsafePut(schema.Key, []byte("k3"), []byte("v3"))
		tx.Unlock()
		be.ForceCommit()

		// Point get and multi-key range through both read tx flavors.
		for name, rtx := range map[string]backend.ReadTx{"ReadTx": be.ReadTx(), "ConcurrentReadTx": be.ConcurrentReadTx()} {
			t.Run(name, func(t *testing.T) {
				rtx.RLock()
				defer rtx.RUnlock()

				ks, vs := rtx.UnsafeRange(schema.Key, []byte("k2"), nil, 0)
				require.Len(t, ks, 1)
				assert.Equal(t, []byte("v2"), vs[0])

				ks, vs = rtx.UnsafeRange(schema.Key, []byte("k1"), []byte("k9"), 0)
				require.Equal(t, [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}, ks)
				require.Equal(t, [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}, vs)

				// limit is honored
				ks, _ = rtx.UnsafeRange(schema.Key, []byte("k1"), []byte("k9"), 2)
				require.Len(t, ks, 2)
			})
		}
	})
}

// TestEngineReadYourWrites verifies that reads through the (uncommitted) batch tx
// observe pending writes, and that reads through the shared read buffer overlay
// observe applied-but-not-yet-committed writes.
func TestEngineReadYourWrites(t *testing.T) {
	forEachEngine(t, func(t *testing.T, engine backend.Engine) {
		be := newEngineBackend(t, engine, filepath.Join(t.TempDir(), "db"))
		defer be.Close()

		tx := be.BatchTx()
		tx.Lock()
		tx.UnsafeCreateBucket(schema.Key)
		tx.UnsafePut(schema.Key, []byte("a"), []byte("1"))
		// read-your-writes within the same batch tx (before commit)
		ks, vs := tx.UnsafeRange(schema.Key, []byte("a"), nil, 0)
		require.Len(t, ks, 1)
		assert.Equal(t, []byte("1"), vs[0])
		tx.Unlock()

		// applied but not force-committed: visible through the read buffer overlay
		rtx := be.ReadTx()
		rtx.RLock()
		ks, vs = rtx.UnsafeRange(schema.Key, []byte("a"), nil, 0)
		rtx.RUnlock()
		require.Len(t, ks, 1)
		assert.Equal(t, []byte("1"), vs[0])
	})
}

func TestEngineDelete(t *testing.T) {
	forEachEngine(t, func(t *testing.T, engine backend.Engine) {
		be := newEngineBackend(t, engine, filepath.Join(t.TempDir(), "db"))
		defer be.Close()

		tx := be.BatchTx()
		tx.Lock()
		tx.UnsafeCreateBucket(schema.Key)
		tx.UnsafePut(schema.Key, []byte("k"), []byte("v"))
		tx.Unlock()
		be.ForceCommit()

		tx.Lock()
		tx.UnsafeDelete(schema.Key, []byte("k"))
		tx.Unlock()
		be.ForceCommit()

		rtx := be.ReadTx()
		rtx.RLock()
		ks, _ := rtx.UnsafeRange(schema.Key, []byte("k"), nil, 0)
		rtx.RUnlock()
		assert.Empty(t, ks)
	})
}

func TestEngineForEach(t *testing.T) {
	forEachEngine(t, func(t *testing.T, engine backend.Engine) {
		be := newEngineBackend(t, engine, filepath.Join(t.TempDir(), "db"))
		defer be.Close()

		tx := be.BatchTx()
		tx.Lock()
		tx.UnsafeCreateBucket(schema.Lease)
		tx.UnsafePut(schema.Lease, []byte("l1"), []byte("a"))
		tx.UnsafePut(schema.Lease, []byte("l2"), []byte("b"))
		tx.Unlock()
		be.ForceCommit()

		got := map[string]string{}
		rtx := be.ReadTx()
		rtx.RLock()
		err := rtx.UnsafeForEach(schema.Lease, func(k, v []byte) error {
			got[string(k)] = string(v)
			return nil
		})
		rtx.RUnlock()
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"l1": "a", "l2": "b"}, got)
	})
}

// TestEngineReopen verifies committed data survives a Close/reopen cycle.
func TestEngineReopen(t *testing.T) {
	forEachEngine(t, func(t *testing.T, engine backend.Engine) {
		path := filepath.Join(t.TempDir(), "db")
		be := newEngineBackend(t, engine, path)

		tx := be.BatchTx()
		tx.Lock()
		tx.UnsafeCreateBucket(schema.Key)
		tx.UnsafePut(schema.Key, []byte("persist"), []byte("yes"))
		tx.Unlock()
		be.ForceCommit()
		require.NoError(t, be.Close())

		be2 := newEngineBackend(t, engine, path)
		defer be2.Close()
		rtx := be2.ReadTx()
		rtx.RLock()
		ks, vs := rtx.UnsafeRange(schema.Key, []byte("persist"), nil, 0)
		rtx.RUnlock()
		require.Len(t, ks, 1)
		assert.Equal(t, []byte("yes"), vs[0])
	})
}

// TestEngineCommitHook verifies OnPreCommitUnsafe writes land atomically in the
// same commit as the data — the mechanism etcd uses for the consistent index.
func TestEngineCommitHook(t *testing.T) {
	forEachEngine(t, func(t *testing.T, engine backend.Engine) {
		path := filepath.Join(t.TempDir(), "db")
		bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
		bcfg.Engine = engine
		bcfg.Path = path
		bcfg.BatchInterval = 10 * time.Millisecond
		bcfg.BatchLimit = 10000

		hookRuns := 0
		bcfg.Hooks = backend.NewHooks(func(tx backend.UnsafeReadWriter) {
			hookRuns++
			tx.UnsafePut(schema.Meta, []byte("consistent_index"), []byte("42"))
		})
		be := backend.New(bcfg)
		defer be.Close()

		tx := be.BatchTx()
		tx.Lock()
		tx.UnsafeCreateBucket(schema.Meta)
		tx.UnsafePut(schema.Meta, []byte("data"), []byte("d"))
		tx.Unlock()
		be.ForceCommit()

		require.Positive(t, hookRuns)
		rtx := be.ReadTx()
		rtx.RLock()
		ks, vs := rtx.UnsafeRange(schema.Meta, []byte("consistent_index"), nil, 0)
		rtx.RUnlock()
		require.Len(t, ks, 1)
		assert.Equal(t, []byte("42"), vs[0])
	})
}

// TestPebbleSnapshotRoundTrip validates the Pebble snapshot format: a checkpoint
// archived to a tar with an exact Size(), streamed out, extracted, and reopened
// as a working backend with the original data intact.
func TestPebbleSnapshotRoundTrip(t *testing.T) {
	be := newEngineBackend(t, backend.EnginePebble, filepath.Join(t.TempDir(), "db"))

	tx := be.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(schema.Key)
	for _, k := range []string{"a", "b", "c"} {
		tx.UnsafePut(schema.Key, []byte(k), []byte("v-"+k))
	}
	tx.Unlock()
	be.ForceCommit()

	snap := be.Snapshot()
	var buf bytes.Buffer
	n, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	// Size() must exactly match the streamed byte count (RemainingBytes protocol).
	require.Equal(t, snap.Size(), n)
	require.Equal(t, int64(buf.Len()), snap.Size())
	require.NoError(t, snap.Close())
	require.NoError(t, be.Close())

	// Extract and reopen.
	destDir := t.TempDir()
	ckptDir, err := backend.UntarPebbleSnapshot(&buf, destDir)
	require.NoError(t, err)

	be2 := newEngineBackend(t, backend.EnginePebble, ckptDir)
	defer be2.Close()
	rtx := be2.ReadTx()
	rtx.RLock()
	ks, vs := rtx.UnsafeRange(schema.Key, []byte("a"), []byte("z"), 0)
	rtx.RUnlock()
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, ks)
	require.Equal(t, [][]byte{[]byte("v-a"), []byte("v-b"), []byte("v-c")}, vs)
}

// TestUntarPebbleSnapshotRejectsNonPebble ensures a non-Pebble stream is refused.
func TestUntarPebbleSnapshotRejectsNonPebble(t *testing.T) {
	_, err := backend.UntarPebbleSnapshot(bytes.NewReader([]byte("not a tar")), t.TempDir())
	require.Error(t, err)
}

// TestPebbleHash verifies the Pebble whole-DB hash is deterministic, changes
// with data, and honors the ignores predicate.
func TestPebbleHash(t *testing.T) {
	writeAB := func(be backend.Backend) {
		tx := be.BatchTx()
		tx.Lock()
		tx.UnsafeCreateBucket(schema.Key)
		tx.UnsafePut(schema.Key, []byte("a"), []byte("1"))
		tx.UnsafePut(schema.Key, []byte("b"), []byte("2"))
		tx.Unlock()
		be.ForceCommit()
	}

	be1 := newEngineBackend(t, backend.EnginePebble, filepath.Join(t.TempDir(), "db"))
	defer be1.Close()
	writeAB(be1)

	h1, err := be1.Hash(nil)
	require.NoError(t, err)
	require.NotZero(t, h1)

	// Deterministic: an independent store with identical data hashes the same.
	be2 := newEngineBackend(t, backend.EnginePebble, filepath.Join(t.TempDir(), "db"))
	defer be2.Close()
	writeAB(be2)
	h2, err := be2.Hash(nil)
	require.NoError(t, err)
	assert.Equal(t, h1, h2)

	// Adding a key changes the hash.
	tx := be2.BatchTx()
	tx.Lock()
	tx.UnsafePut(schema.Key, []byte("c"), []byte("3"))
	tx.Unlock()
	be2.ForceCommit()
	h3, err := be2.Hash(nil)
	require.NoError(t, err)
	assert.NotEqual(t, h1, h3)

	// Ignoring the new key restores the original hash (name already counted).
	hIgnoreC, err := be2.Hash(func(_ /*bucket*/, k []byte) bool {
		return string(k) == "c"
	})
	require.NoError(t, err)
	assert.Equal(t, h1, hIgnoreC)
}

// TestPebbleSize verifies Size/SizeInUse report sane, non-negative values after
// writes, with logical usage not exceeding total.
func TestPebbleSize(t *testing.T) {
	be := newEngineBackend(t, backend.EnginePebble, filepath.Join(t.TempDir(), "db"))
	defer be.Close()

	tx := be.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(schema.Key)
	for i := 0; i < 1000; i++ {
		tx.UnsafePut(schema.Key, []byte(fmt.Sprintf("k%05d", i)), make([]byte, 128))
	}
	tx.Unlock()
	be.ForceCommit()
	require.NoError(t, be.Defrag()) // forces a flush+compaction so tables exist

	assert.Positive(t, be.Size())
	assert.GreaterOrEqual(t, be.Size(), be.SizeInUse())
	assert.GreaterOrEqual(t, be.SizeInUse(), int64(0))
}
