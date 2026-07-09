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
	"hash/crc32"
	"sort"
	"sync"
)

// The Pebble store is a flat keyspace with a 1-byte bucket-ID prefix; unlike
// bbolt it does not persist logical bucket names. To compute a whole-DB hash
// that respects the ignores predicate (which is keyed by bucket name), the
// schema package registers its buckets here at init time so the backend can map
// the ID prefix back to a name.
var (
	bucketRegistryMu sync.RWMutex
	bucketRegistry   = map[byte]Bucket{}
)

// RegisterBucket records a bucket so the Pebble backend's Hash can recover its
// name from the 1-byte ID prefix. Called from schema package init.
func RegisterBucket(b Bucket) {
	bucketRegistryMu.Lock()
	defer bucketRegistryMu.Unlock()
	bucketRegistry[byte(b.ID())] = b
}

// registeredBucketsSorted returns the registered buckets ordered by ID, giving a
// deterministic iteration order for hashing across members.
func registeredBucketsSorted() []Bucket {
	bucketRegistryMu.RLock()
	defer bucketRegistryMu.RUnlock()
	out := make([]Bucket, 0, len(bucketRegistry))
	for _, b := range bucketRegistry {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}

// Hash returns a CRC32-Castagnoli checksum over all buckets' names, keys and
// values, in deterministic (bucket-ID, then key) order, skipping any (bucket,
// key) for which ignores returns true. A bucket's name is included only if the
// bucket is non-empty, mirroring the bbolt implementation's "bucket exists"
// behavior. The hash is stable across Pebble members; it is not intended to be
// bit-compatible with the bbolt engine's Hash.
func (b *pebbleBackend) Hash(ignores func(bucketName, keyName []byte) bool) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	b.mu.RLock()
	defer b.mu.RUnlock()
	snap := b.db.NewSnapshot()
	defer snap.Close()

	for _, bucket := range registeredBucketsSorted() {
		name := bucket.Name()
		wroteName := false
		err := pebbleForEach(b.lg, snap, bucket, func(k, v []byte) error {
			if !wroteName {
				h.Write(name)
				wroteName = true
			}
			if ignores != nil && ignores(name, k) {
				return nil
			}
			h.Write(k)
			h.Write(v)
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return h.Sum32(), nil
}
