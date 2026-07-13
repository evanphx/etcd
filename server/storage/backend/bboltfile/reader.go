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

package bboltfile

import (
	"fmt"
	"hash/fnv"
	"os"
)

// Reader streams key/value pairs out of a bbolt database file by reading pages
// on demand (no mmap, no lock). It reads files written by this package or by the
// bbolt library, including inline buckets and overflow pages.
type Reader struct {
	f        *os.File
	pageSize int
	rootPgid uint64
}

// Open opens a bbolt file for reading, validating and selecting its meta page.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &Reader{f: f}
	if err := r.readMeta(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

// Close releases the underlying file.
func (r *Reader) Close() error { return r.f.Close() }

// readMeta reads both meta pages and adopts the valid one with the higher txid.
func (r *Reader) readMeta() error {
	// The page size is stored in the meta page, so read a fixed prefix first
	// (large enough to contain page 0's header + meta) to learn it.
	head := make([]byte, DefaultPageSize)
	if _, err := r.f.ReadAt(head, 0); err != nil {
		return fmt.Errorf("bboltfile: read meta page 0: %w", err)
	}
	root0, ps0, txid0, ok0 := parseMeta(head[pageHeaderSize:])
	if !ok0 {
		return fmt.Errorf("bboltfile: not a bbolt file (bad meta page 0)")
	}
	r.pageSize = ps0
	r.rootPgid = root0

	// Meta page 1 lives at the declared page size; prefer it if it has a higher
	// valid txid (matching bbolt's newest-meta selection).
	m1 := make([]byte, pageHeaderSize+metaSize)
	if _, err := r.f.ReadAt(m1, int64(ps0)); err == nil {
		if root1, ps1, txid1, ok1 := parseMeta(m1[pageHeaderSize:]); ok1 && ps1 == ps0 && txid1 > txid0 {
			r.rootPgid = root1
		}
	}
	return nil
}

// parseMeta validates a 64-byte meta blob and returns its root pgid, page size
// and txid.
func parseMeta(m []byte) (root uint64, pageSize int, txid uint64, ok bool) {
	if len(m) < metaSize {
		return 0, 0, 0, false
	}
	if le.Uint32(m[0:]) != boltMagic || le.Uint32(m[4:]) != boltVersion {
		return 0, 0, 0, false
	}
	h := fnv.New64a()
	h.Write(m[:56])
	if h.Sum64() != le.Uint64(m[56:]) {
		return 0, 0, 0, false
	}
	return le.Uint64(m[16:]), int(le.Uint32(m[8:])), le.Uint64(m[48:]), true
}

// ForEach calls fn for every (bucket, key, value) in the file, buckets in name
// order and keys in ascending order within each bucket. Slices passed to fn are
// only valid for the duration of the call.
func (r *Reader) ForEach(fn func(bucket, key, value []byte) error) error {
	// The root bucket's leaves hold one entry per top-level bucket (bucketLeaf
	// flag), value = InBucket header (optionally followed by inline page data).
	return r.walk(r.rootPgid, func(_ uint64, name, value []byte, flags uint32) error {
		if flags&bucketLeafFlag == 0 {
			return fmt.Errorf("bboltfile: unexpected non-bucket entry %q at root", name)
		}
		return r.walkBucket(name, value, fn)
	})
}

// walkBucket iterates one top-level bucket, resolving inline vs on-page storage.
func (r *Reader) walkBucket(name, inbucket []byte, fn func(bucket, key, value []byte) error) error {
	if len(inbucket) < inBucketSize {
		return fmt.Errorf("bboltfile: short bucket header for %q", name)
	}
	root := le.Uint64(inbucket[0:])
	if root == 0 {
		// Inline bucket: the page data follows the 16-byte header in the value.
		return r.walkPage(inbucket[inBucketSize:], func(_ uint64, k, v []byte, flags uint32) error {
			if flags&bucketLeafFlag != 0 {
				return nil // etcd has no nested buckets; skip defensively
			}
			return fn(name, k, v)
		})
	}
	return r.walk(root, func(_ uint64, k, v []byte, flags uint32) error {
		if flags&bucketLeafFlag != 0 {
			return nil
		}
		return fn(name, k, v)
	})
}

// walk reads the page at pgid and visits its entries in order, descending into
// branch children. The visitor receives (pgid-unused, key, value, flags).
func (r *Reader) walk(pgid uint64, visit func(pgid uint64, key, value []byte, flags uint32) error) error {
	page, err := r.readPage(pgid)
	if err != nil {
		return err
	}
	return r.walkPage(page, visit)
}

func (r *Reader) walkPage(page []byte, visit func(pgid uint64, key, value []byte, flags uint32) error) error {
	if len(page) < pageHeaderSize {
		return fmt.Errorf("bboltfile: truncated page")
	}
	flags := pageFlags(page)
	count := int(pageCount(page))
	switch flags {
	case leafPageFlag:
		for i := 0; i < count; i++ {
			elem := page[pageHeaderSize+elementSize*i:]
			eflags := le.Uint32(elem[0:])
			pos := le.Uint32(elem[4:])
			ksize := le.Uint32(elem[8:])
			vsize := le.Uint32(elem[12:])
			base := pageHeaderSize + elementSize*i + int(pos)
			if base+int(ksize)+int(vsize) > len(page) {
				return fmt.Errorf("bboltfile: leaf element out of bounds")
			}
			key := page[base : base+int(ksize)]
			val := page[base+int(ksize) : base+int(ksize)+int(vsize)]
			if err := visit(0, key, val, eflags); err != nil {
				return err
			}
		}
		return nil
	case branchPageFlag:
		for i := 0; i < count; i++ {
			elem := page[pageHeaderSize+elementSize*i:]
			childPgid := le.Uint64(elem[8:])
			if err := r.walk(childPgid, visit); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("bboltfile: unexpected page type %#x", flags)
	}
}

// readPage reads the full (overflow-aware) span of the page at pgid.
func (r *Reader) readPage(pgid uint64) ([]byte, error) {
	hdr := make([]byte, pageHeaderSize)
	base := int64(pgid) * int64(r.pageSize)
	if _, err := r.f.ReadAt(hdr, base); err != nil {
		return nil, fmt.Errorf("bboltfile: read page %d header: %w", pgid, err)
	}
	span := (int(pageOverflow(hdr)) + 1) * r.pageSize
	page := make([]byte, span)
	if _, err := r.f.ReadAt(page, base); err != nil {
		return nil, fmt.Errorf("bboltfile: read page %d (span %d): %w", pgid, span, err)
	}
	return page, nil
}
