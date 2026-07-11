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

// ExportPebbleToBbolt reads a Pebble store and writes an equivalent bbolt
// database file using the bboltfile codec — the reverse of ImportBboltIntoPebble
// and the Pebble->bbolt migration primitive. It streams each bucket's keys
// straight from a Pebble iterator into the bbolt writer, so memory use is
// independent of the database size.
//
// buckets lists the buckets to materialize, in the set a bbolt etcd expects
// (typically schema.AllBuckets); each is created even if empty, so the output
// matches what a bbolt member would hold. Keys within a Pebble bucket prefix are
// already in ascending order, which is what the bbolt writer requires.
func ExportPebbleToBbolt(lg *zap.Logger, pebbleDir, bboltPath string, buckets []Bucket) error {
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

	// bbolt requires buckets in ascending name order (the Pebble bucket IDs are
	// not name-ordered).
	ordered := append([]Bucket(nil), buckets...)
	sort.Slice(ordered, func(i, j int) bool { return bytes.Compare(ordered[i].Name(), ordered[j].Name()) < 0 })

	w, err := bboltfile.Create(bboltPath)
	if err != nil {
		return fmt.Errorf("create bbolt target %q: %w", bboltPath, err)
	}

	keys := 0
	if err := func() error {
		for _, bucket := range ordered {
			if err := w.BeginBucket(bucket.Name()); err != nil {
				return err
			}
			iter, err := snap.NewIter(&pebble.IterOptions{
				LowerBound: bucketLowerBound(bucket),
				UpperBound: bucketUpperBound(bucket),
			})
			if err != nil {
				return err
			}
			for valid := iter.First(); valid; valid = iter.Next() {
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
		zap.String("source", pebbleDir),
		zap.String("target", bboltPath),
		zap.Int("keys", keys),
	)
	return nil
}
