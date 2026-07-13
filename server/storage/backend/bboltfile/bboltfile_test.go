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

package bboltfile_test

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"go.etcd.io/etcd/server/v3/storage/backend/bboltfile"
)

// bucketData maps bucket name -> (key -> value).
type bucketData map[string]map[string]string

// sample builds a representative data set: an empty bucket, a large bucket that
// forces multiple leaves and branch pages, a small bucket, and a bucket with a
// value larger than a page (forcing an overflow page).
func sample() bucketData {
	d := bucketData{
		"aaa-empty": {},
		"key":       {},
		"meta":      {"consistent_index": "\x00\x00\x00\x00\x00\x00\x00\x2a", "term": "5"},
		"zzz-big":   {"bigval": string(make([]byte, 12*1024))},
	}
	for i := 0; i < 3000; i++ {
		d["key"][fmt.Sprintf("key-%06d", i)] = fmt.Sprintf("value-%06d-%s", i, "payloadpayload")
	}
	return d
}

func sortedNames(d bucketData) []string {
	names := make([]string, 0, len(d))
	for n := range d {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// writeWithWriter builds a bbolt file at path using bboltfile.Writer.
func writeWithWriter(t *testing.T, path string, d bucketData) {
	t.Helper()
	w, err := bboltfile.Create(path)
	require.NoError(t, err)
	for _, name := range sortedNames(d) {
		require.NoError(t, w.BeginBucket([]byte(name)))
		for _, k := range sortedKeys(d[name]) {
			require.NoError(t, w.Put([]byte(k), []byte(d[name][k])))
		}
		require.NoError(t, w.EndBucket())
	}
	require.NoError(t, w.Close())
}

// readWithReader reads a bbolt file with bboltfile.Reader into a bucketData.
func readWithReader(t *testing.T, path string) bucketData {
	t.Helper()
	r, err := bboltfile.Open(path)
	require.NoError(t, err)
	defer r.Close()
	got := bucketData{}
	require.NoError(t, r.ForEach(func(bucket, key, value []byte) error {
		if got[string(bucket)] == nil {
			got[string(bucket)] = map[string]string{}
		}
		got[string(bucket)][string(key)] = string(value)
		return nil
	}))
	return got
}

// readWithBolt reads a bbolt file with the real bbolt library into a bucketData.
func readWithBolt(t *testing.T, path string) bucketData {
	t.Helper()
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()
	got := bucketData{}
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			m := map[string]string{}
			got[string(name)] = m
			return b.ForEach(func(k, v []byte) error {
				m[string(k)] = string(v)
				return nil
			})
		})
	}))
	return got
}

// TestWriterProducesLibraryValidFile is the core safety check: a file built by
// bboltfile.Writer opens in the real bbolt library, passes tx.Check(), and reads
// back byte-for-byte.
func TestWriterProducesLibraryValidFile(t *testing.T) {
	d := sample()
	path := filepath.Join(t.TempDir(), "written.db")
	writeWithWriter(t, path, d)

	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		var errs []error
		for err := range tx.Check() {
			errs = append(errs, err)
		}
		require.Empty(t, errs, "bbolt Check() must find no inconsistencies")
		return nil
	}))

	require.Equal(t, d, readWithBolt(t, path), "bbolt library must read back the exact data")
}

// TestReaderReadsLibraryFile checks bboltfile.Reader against a file written by
// the real bbolt library.
func TestReaderReadsLibraryFile(t *testing.T) {
	d := sample()
	path := filepath.Join(t.TempDir(), "lib.db")

	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		for _, name := range sortedNames(d) {
			b, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			for _, k := range sortedKeys(d[name]) {
				if err := b.Put([]byte(k), []byte(d[name][k])); err != nil {
					return err
				}
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	// A key/value ForEach cannot surface an empty bucket (it has no entries to
	// emit); compare against the non-empty buckets. Empty buckets don't matter
	// for conversion — Pebble has no bucket concept, and the Writer side creates
	// buckets explicitly.
	nonEmpty := bucketData{}
	for n, m := range d {
		if len(m) > 0 {
			nonEmpty[n] = m
		}
	}
	require.Equal(t, nonEmpty, readWithReader(t, path), "reader must match the library-written data")
}

// TestRoundTrip runs library -> Reader -> Writer -> library, proving both
// directions compose losslessly.
func TestRoundTrip(t *testing.T) {
	d := sample()
	dir := t.TempDir()
	libPath := filepath.Join(dir, "lib.db")

	db, err := bolt.Open(libPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		for _, name := range sortedNames(d) {
			b, _ := tx.CreateBucket([]byte(name))
			for _, k := range sortedKeys(d[name]) {
				require.NoError(t, b.Put([]byte(k), []byte(d[name][k])))
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	// Reader -> Writer into a new file.
	r, err := bboltfile.Open(libPath)
	require.NoError(t, err)
	outPath := filepath.Join(dir, "rebuilt.db")
	w, err := bboltfile.Create(outPath)
	require.NoError(t, err)
	var curBucket string
	require.NoError(t, r.ForEach(func(bucket, key, value []byte) error {
		if string(bucket) != curBucket {
			if curBucket != "" {
				if err := w.EndBucket(); err != nil {
					return err
				}
			}
			if err := w.BeginBucket(bucket); err != nil {
				return err
			}
			curBucket = string(bucket)
		}
		return w.Put(key, value)
	}))
	if curBucket != "" {
		require.NoError(t, w.EndBucket())
	}
	require.NoError(t, w.Close())
	require.NoError(t, r.Close())

	// Note: the empty bucket has no keys, so ForEach never emits it; the rebuilt
	// file omits it. Compare against the non-empty subset.
	nonEmpty := bucketData{}
	for n, m := range d {
		if len(m) > 0 {
			nonEmpty[n] = m
		}
	}
	require.Equal(t, nonEmpty, readWithBolt(t, outPath))
}
