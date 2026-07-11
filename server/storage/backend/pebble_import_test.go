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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// TestImportBboltIntoPebbleRoundTrip populates a bbolt database, converts it
// into a Pebble store, and verifies every bucket/key/value survives byte-for-
// byte — the correctness contract of the cross-engine restore primitive.
func TestImportBboltIntoPebbleRoundTrip(t *testing.T) {
	lg := zaptest.NewLogger(t)
	dir := t.TempDir()
	bboltPath := filepath.Join(dir, "bbolt.db")

	buckets := []backend.Bucket{schema.Key, schema.Meta, schema.Lease, schema.Auth}
	// want[bucketName][key] = value
	want := map[string]map[string]string{}

	src := backend.NewDefaultBackend(lg, bboltPath, backend.WithEngine(backend.EngineBBolt))
	tx := src.BatchTx()
	tx.Lock()
	for _, b := range buckets {
		tx.UnsafeCreateBucket(b)
		want[string(b.Name())] = map[string]string{}
	}
	for i := 0; i < 500; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("value-%04d-payload", i))
		tx.UnsafePut(schema.Key, k, v)
		want["key"][string(k)] = string(v)
	}
	tx.UnsafePut(schema.Meta, []byte("consistent_index"), []byte{0, 0, 0, 0, 0, 0, 0, 42})
	want["meta"]["consistent_index"] = string([]byte{0, 0, 0, 0, 0, 0, 0, 42})
	tx.UnsafePut(schema.Lease, []byte("lease-1"), []byte("ttl=30"))
	want["lease"]["lease-1"] = "ttl=30"
	tx.UnsafePut(schema.Auth, []byte("authRevision"), []byte("7"))
	want["auth"]["authRevision"] = "7"
	tx.Unlock()
	src.ForceCommit()
	require.NoError(t, src.Close())

	// Convert bbolt -> Pebble.
	pebbleDir := filepath.Join(dir, "pebble")
	require.NoError(t, backend.ImportBboltIntoPebble(lg, bboltPath, pebbleDir))

	// The converted artifact is a Pebble store directory, not a bbolt file.
	require.False(t, backend.IsPebbleSnapshot(bboltPath), "source is a bbolt file")

	// Open the converted store as Pebble and read everything back.
	dst := backend.NewDefaultBackend(lg, pebbleDir, backend.WithEngine(backend.EnginePebble))
	defer dst.Close()

	got := map[string]map[string]string{}
	rtx := dst.ReadTx()
	rtx.RLock()
	for _, b := range buckets {
		m := map[string]string{}
		require.NoError(t, rtx.UnsafeForEach(b, func(k, v []byte) error {
			m[string(k)] = string(v)
			return nil
		}))
		got[string(b.Name())] = m
	}
	rtx.RUnlock()

	require.Equal(t, want, got, "converted Pebble store must match the bbolt source exactly")
}

// TestImportBboltRejectsUnknownBucket ensures conversion fails loudly rather
// than silently dropping data it cannot map to a Pebble bucket ID.
func TestImportBboltRejectsUnknownBucket(t *testing.T) {
	lg := zaptest.NewLogger(t)
	dir := t.TempDir()
	bboltPath := filepath.Join(dir, "bbolt.db")

	src := backend.NewDefaultBackend(lg, bboltPath, backend.WithEngine(backend.EngineBBolt))
	tx := src.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(unknownBucket{})
	tx.UnsafePut(unknownBucket{}, []byte("k"), []byte("v"))
	tx.Unlock()
	src.ForceCommit()
	require.NoError(t, src.Close())

	err := backend.ImportBboltIntoPebble(lg, bboltPath, filepath.Join(dir, "pebble"))
	require.ErrorContains(t, err, "unknown bucket")
}

// unknownBucket is a bucket whose name is not registered in the schema.
type unknownBucket struct{}

func (unknownBucket) ID() backend.BucketID    { return 250 }
func (unknownBucket) Name() []byte            { return []byte("not-a-real-etcd-bucket") }
func (unknownBucket) String() string          { return "not-a-real-etcd-bucket" }
func (unknownBucket) IsSafeRangeBucket() bool { return false }
