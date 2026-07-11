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
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/storage/backend/bboltfile"
)

// ExportPebbleToBbolt reads a Pebble store directory and writes an equivalent
// bbolt database file using the bboltfile codec — the reverse of
// ImportBboltIntoPebble and the Pebble->bbolt migration primitive.
func ExportPebbleToBbolt(lg *zap.Logger, pebbleDir, bboltPath string) error {
	if lg == nil {
		lg = zap.NewNop()
	}
	db, err := pebble.Open(pebbleDir, &pebble.Options{
		ReadOnly: true,
		Logger:   &pebbleZapLogger{lg: lg.Named("pebble-export")},
	})
	if err != nil {
		return fmt.Errorf("open pebble source %q: %w", pebbleDir, err)
	}
	defer db.Close()
	snap := db.NewSnapshot()
	defer snap.Close()
	return exportPebbleSnapshot(lg, snap, bboltPath)
}

// pebbleSnapshotReader is the subset of *pebble.Snapshot that the exporter needs
// (satisfied by *pebble.Snapshot), so a snapshot of a live store can be exported
// without reopening it.
type pebbleSnapshotReader interface {
	NewIter(o *pebble.IterOptions) (*pebble.Iterator, error)
}

// exportPebbleSnapshot writes the buckets present in a Pebble snapshot to a
// bbolt file, streaming each bucket's keys from a Pebble iterator into a
// bboltfile Writer. Only non-empty buckets are written; empty schema buckets are
// recreated when etcd next opens the database, so omitting them is safe and it
// keeps the output free of buckets that merely happen to be registered. Memory
// use is independent of database size.
func exportPebbleSnapshot(lg *zap.Logger, snap pebbleSnapshotReader, bboltPath string) error {
	w, err := bboltfile.Create(bboltPath)
	if err != nil {
		return fmt.Errorf("create bbolt target %q: %w", bboltPath, err)
	}

	keys := 0
	if err := func() error {
		// bbolt requires buckets in ascending name order (Pebble bucket IDs are
		// not name-ordered).
		for _, bucket := range registeredBucketsByName() {
			iter, err := snap.NewIter(&pebble.IterOptions{
				LowerBound: bucketLowerBound(bucket),
				UpperBound: bucketUpperBound(bucket),
			})
			if err != nil {
				return err
			}
			if !iter.First() {
				iter.Close() // empty bucket; recreated on next open
				continue
			}
			if err := w.BeginBucket(bucket.Name()); err != nil {
				iter.Close()
				return err
			}
			for valid := true; valid; valid = iter.Next() {
				// Strip the 1-byte bucket-ID prefix; the writer copies the key
				// and value, so the iterator's transient slices are safe.
				if err := w.Put(iter.Key()[1:], iter.Value()); err != nil {
					iter.Close()
					return err
				}
				keys++
			}
			if err := iter.Close(); err != nil {
				return err
			}
			if err := w.EndBucket(); err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		w.Close()
		return err
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("finalize bbolt target: %w", err)
	}
	lg.Info("exported pebble store into bbolt database",
		zap.String("target", bboltPath),
		zap.Int("keys", keys),
	)
	return nil
}

// registeredBucketsByName returns the registered buckets ordered by name, as the
// bbolt writer requires.
func registeredBucketsByName() []Bucket {
	bucketRegistryMu.RLock()
	out := make([]Bucket, 0, len(bucketRegistry))
	for _, b := range bucketRegistry {
		out = append(out, b)
	}
	bucketRegistryMu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Name(), out[j].Name()) < 0 })
	return out
}
