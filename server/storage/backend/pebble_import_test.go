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
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// TestExportPebbleToBbolt populates a Pebble store, exports it to a bbolt file
// via the bboltfile codec, and verifies the result opens in the real bbolt
// library, passes tx.Check(), and holds the same data.
func TestExportPebbleToBbolt(t *testing.T) {
	lg := zaptest.NewLogger(t)
	dir := t.TempDir()
	pebbleDir := filepath.Join(dir, "pebble")

	src := backend.NewDefaultBackend(lg, pebbleDir, backend.WithEngine(backend.EnginePebble))
	tx := src.BatchTx()
	tx.Lock()
	for _, b := range []backend.Bucket{schema.Key, schema.Meta, schema.Lease} {
		tx.UnsafeCreateBucket(b)
	}
	want := map[string]map[string]string{"key": {}, "meta": {}, "lease": {}}
	for i := 0; i < 400; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("v-%04d-payload", i))
		tx.UnsafePut(schema.Key, k, v)
		want["key"][string(k)] = string(v)
	}
	tx.UnsafePut(schema.Meta, []byte("consistent_index"), []byte{0, 0, 0, 0, 0, 0, 0, 9})
	want["meta"]["consistent_index"] = string([]byte{0, 0, 0, 0, 0, 0, 0, 9})
	tx.UnsafePut(schema.Lease, []byte("l1"), []byte("ttl"))
	want["lease"]["l1"] = "ttl"
	tx.Unlock()
	src.ForceCommit()
	require.NoError(t, src.Close())

	bboltPath := filepath.Join(dir, "out.db")
	require.NoError(t, backend.ExportPebbleToBbolt(lg, pebbleDir, bboltPath, schema.AllBuckets))

	db, err := bolt.Open(bboltPath, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var errs []error
		for e := range tx.Check() {
			errs = append(errs, e)
		}
		require.Empty(t, errs, "exported bbolt file must pass Check()")
		for name, kv := range want {
			b := tx.Bucket([]byte(name))
			require.NotNil(t, b, "bucket %q", name)
			got := map[string]string{}
			require.NoError(t, b.ForEach(func(k, v []byte) error {
				got[string(k)] = string(v)
				return nil
			}))
			require.Equal(t, kv, got, "bucket %q content", name)
		}
		return nil
	}))
}

// TestPebbleBboltPebbleRoundTrip exercises both conversion directions through
// the codec: Pebble -> bbolt (export) -> Pebble (import), and checks the data
// survives.
func TestPebbleBboltPebbleRoundTrip(t *testing.T) {
	lg := zaptest.NewLogger(t)
	dir := t.TempDir()
	p1 := filepath.Join(dir, "p1")

	src := backend.NewDefaultBackend(lg, p1, backend.WithEngine(backend.EnginePebble))
	tx := src.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(schema.Key)
	want := map[string]string{}
	for i := 0; i < 250; i++ {
		k := []byte(fmt.Sprintf("k-%04d", i))
		v := []byte(fmt.Sprintf("val-%04d", i))
		tx.UnsafePut(schema.Key, k, v)
		want[string(k)] = string(v)
	}
	tx.Unlock()
	src.ForceCommit()
	require.NoError(t, src.Close())

	bboltPath := filepath.Join(dir, "mid.db")
	require.NoError(t, backend.ExportPebbleToBbolt(lg, p1, bboltPath, schema.AllBuckets))

	p2 := filepath.Join(dir, "p2")
	require.NoError(t, backend.ImportBboltIntoPebble(lg, bboltPath, p2))

	dst := backend.NewDefaultBackend(lg, p2, backend.WithEngine(backend.EnginePebble))
	defer dst.Close()
	rtx := dst.ReadTx()
	rtx.RLock()
	got := map[string]string{}
	require.NoError(t, rtx.UnsafeForEach(schema.Key, func(k, v []byte) error {
		got[string(k)] = string(v)
		return nil
	}))
	rtx.RUnlock()
	require.Equal(t, want, got)
}

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
