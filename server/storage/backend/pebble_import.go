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
	"archive/tar"
	"bytes"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/v2"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// pebbleImportFlushBytes bounds how much converted data is buffered in a Pebble
// batch before it is committed, so importing a large database does not require
// memory proportional to the whole database.
const pebbleImportFlushBytes = 8 << 20

// bucketByName looks up a registered bucket by its logical name. Every bucket
// etcd uses is registered at schema init (see schema.AllBuckets), so an unknown
// name means the source database is not a normal etcd store.
func bucketByName(name []byte) (Bucket, bool) {
	bucketRegistryMu.RLock()
	defer bucketRegistryMu.RUnlock()
	for _, b := range bucketRegistry {
		if bytes.Equal(b.Name(), name) {
			return b, true
		}
	}
	return nil, false
}

// IsPebbleSnapshot reports whether the file at path is a Pebble snapshot (a tar
// whose first entry is the Pebble marker file), as opposed to a raw bbolt
// database file. It is used to pick the right install/convert path for a
// received or restored snapshot.
func IsPebbleSnapshot(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	h, err := tar.NewReader(f).Next()
	if err != nil {
		return false // not a tar (e.g. a bbolt file), so not a pebble snapshot
	}
	return h.Name == PebbleSnapshotVersionFile
}

// ImportBboltIntoPebble reads a bbolt database file (a db or a bbolt-format
// snapshot) and writes all of its buckets' key/value pairs into a fresh Pebble
// store at pebbleDir, using the Pebble backend's 1-byte bucket-ID key encoding.
// It is the cross-engine restore/migration primitive: because both engines
// store byte-identical logical content (including the meta/consistent_index
// keys), the resulting Pebble store is an exact logical copy of the source.
//
// The source is opened read-only as a normal bbolt database. That mmaps the
// file; for very large inputs a streaming bbolt page scanner would avoid the
// mapping, but opening as a normal DB is the simplest correct reader and this
// runs during a one-shot restore, not steady-state.
func ImportBboltIntoPebble(lg *zap.Logger, bboltPath, pebbleDir string) error {
	if lg == nil {
		lg = zap.NewNop()
	}
	if err := os.MkdirAll(pebbleDir, 0o700); err != nil {
		return fmt.Errorf("create pebble import dir: %w", err)
	}

	src, err := bolt.Open(bboltPath, 0o400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("open bbolt source %q: %w", bboltPath, err)
	}
	defer src.Close()

	db, err := pebble.Open(pebbleDir, &pebble.Options{
		Logger: &pebbleZapLogger{lg: lg.Named("pebble-import")},
	})
	if err != nil {
		return fmt.Errorf("open pebble target %q: %w", pebbleDir, err)
	}
	defer db.Close()

	batch := db.NewBatch()
	batchBytes := 0
	keys := 0
	flush := func() error {
		if batch.Empty() {
			return nil
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		batch.Close()
		batch = db.NewBatch()
		batchBytes = 0
		return nil
	}

	err = src.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			bucket, ok := bucketByName(name)
			if !ok {
				return fmt.Errorf("bbolt source has unknown bucket %q; not a recognized etcd database", name)
			}
			return b.ForEach(func(k, v []byte) error {
				if k == nil {
					return nil // a nested-bucket entry; etcd uses none
				}
				// physKey copies k; pebble Batch.Set copies both key and value,
				// so neither needs a separate copy despite bbolt's slice reuse.
				if err := batch.Set(physKey(bucket, k), v, nil); err != nil {
					return err
				}
				keys++
				batchBytes += 1 + len(k) + len(v)
				if batchBytes >= pebbleImportFlushBytes {
					return flush()
				}
				return nil
			})
		})
	})
	if err != nil {
		batch.Close()
		return err
	}
	if err := flush(); err != nil {
		batch.Close()
		return err
	}
	batch.Close()
	if err := db.Flush(); err != nil {
		return fmt.Errorf("flush pebble target: %w", err)
	}
	lg.Info("imported bbolt database into pebble store",
		zap.String("source", bboltPath),
		zap.String("target", pebbleDir),
		zap.Int("keys", keys),
	)
	return nil
}
