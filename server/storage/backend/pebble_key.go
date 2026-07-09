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

// Pebble is a single flat, byte-sorted keyspace with no buckets. etcd's logical
// buckets (Key, Meta, Lease, ...) are emulated by prefixing every physical key
// with a single byte derived from the bucket's ID. Bucket IDs are small (<= 100
// in schema/bucket.go), so one byte is sufficient and keeps keys compact.
//
// Physical key layout:  [ 1 byte bucketID ] [ logical key bytes ]
//
// Because the prefix is fixed-width, byte ordering within a bucket is identical
// to bbolt's per-bucket ordering (plain bytes.Compare), which every mvcc
// revision-encoding assumption relies on. A whole bucket is the half-open range
// [ {id}, {id+1} ).

// physKey returns the physical Pebble key for a logical key in the given bucket.
func physKey(bucket Bucket, key []byte) []byte {
	id := bucket.ID()
	if id < 0 || id > 0xff {
		panic("backend: bucket ID does not fit in a single byte prefix")
	}
	pk := make([]byte, 1+len(key))
	pk[0] = byte(id)
	copy(pk[1:], key)
	return pk
}

// bucketLowerBound returns the inclusive lower bound of a bucket's key range.
func bucketLowerBound(bucket Bucket) []byte {
	return []byte{byte(bucket.ID())}
}

// bucketUpperBound returns the exclusive upper bound of a bucket's key range,
// i.e. the start of the next bucket's prefix.
func bucketUpperBound(bucket Bucket) []byte {
	return []byte{byte(bucket.ID()) + 1}
}

// logicalKey strips the 1-byte bucket prefix from a physical key, returning a
// freshly-allocated copy that the caller owns (Pebble iterator keys are only
// valid until the iterator advances).
func logicalKey(physical []byte) []byte {
	lk := make([]byte, len(physical)-1)
	copy(lk, physical[1:])
	return lk
}
