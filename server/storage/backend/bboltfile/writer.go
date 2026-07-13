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
	"bytes"
	"fmt"
	"hash/fnv"
	"os"
)

// Writer builds a bbolt database file from buckets of sorted key/value pairs.
//
// Usage is streaming and append-only: buckets must be added in ascending name
// order, and keys within a bucket in ascending order. It buffers only the
// current leaf page plus one separator key per flushed page, so building a large
// database uses memory proportional to the tree height, not the data size.
//
//	w, _ := Create(path)
//	w.BeginBucket([]byte("key"))
//	w.Put(k1, v1); w.Put(k2, v2)
//	w.EndBucket()
//	... more buckets in ascending name order ...
//	w.Close()
type Writer struct {
	f        *os.File
	pageSize int
	nextPgid uint64 // next page id to allocate (0,1 meta; 2 freelist)

	inBucket   bool
	bucketName []byte
	lastKey    []byte      // enforce ascending keys within a bucket
	leaf       []pageEntry // entries of the leaf page currently being filled
	leafBytes  int
	leaves     []childRef // flushed leaves of the current bucket

	buckets  []bucketRef // (name, root pgid) for the root bucket tree
	lastName []byte
	closed   bool
}

type childRef struct {
	key  []byte // first key of the child subtree (separator)
	pgid uint64
}

type bucketRef struct {
	name     []byte
	rootPgid uint64
}

// Create opens path for writing a new bbolt file with the default page size.
func Create(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	return &Writer{
		f:        f,
		pageSize: DefaultPageSize,
		nextPgid: 3, // pages 0,1 = meta, 2 = freelist
	}, nil
}

func (w *Writer) allocate(npages int) uint64 {
	id := w.nextPgid
	w.nextPgid += uint64(npages)
	return id
}

// writePages writes buf (a multiple of pageSize) at the offset of pgid.
func (w *Writer) writePages(pgid uint64, buf []byte) error {
	_, err := w.f.WriteAt(buf, int64(pgid)*int64(w.pageSize))
	return err
}

// BeginBucket starts a new bucket. Bucket names must ascend across calls.
func (w *Writer) BeginBucket(name []byte) error {
	if w.inBucket {
		return fmt.Errorf("bboltfile: BeginBucket while bucket %q is open", w.bucketName)
	}
	if w.lastName != nil && bytes.Compare(name, w.lastName) <= 0 {
		return fmt.Errorf("bboltfile: bucket names must ascend: %q after %q", name, w.lastName)
	}
	w.inBucket = true
	w.bucketName = append([]byte(nil), name...)
	w.lastKey = nil
	w.leaf = w.leaf[:0]
	w.leafBytes = pageHeaderSize
	w.leaves = nil
	return nil
}

// Put appends a key/value to the current bucket. Keys must ascend.
func (w *Writer) Put(key, value []byte) error {
	if !w.inBucket {
		return fmt.Errorf("bboltfile: Put outside a bucket")
	}
	if len(key) == 0 {
		return fmt.Errorf("bboltfile: empty key not allowed")
	}
	if w.lastKey != nil && bytes.Compare(key, w.lastKey) <= 0 {
		return fmt.Errorf("bboltfile: keys must ascend within bucket %q", w.bucketName)
	}
	need := elementSize + len(key) + len(value)
	// Flush the current leaf if this entry won't fit and the leaf is non-empty.
	if len(w.leaf) > 0 && w.leafBytes+need > w.pageSize {
		if err := w.flushLeaf(); err != nil {
			return err
		}
	}
	w.leaf = append(w.leaf, pageEntry{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
	w.leafBytes += need
	w.lastKey = w.leaf[len(w.leaf)-1].key
	return nil
}

// flushLeaf writes the buffered leaf page and records it for the branch layer.
func (w *Writer) flushLeaf() error {
	pgid, err := w.writeLeaf(w.leaf)
	if err != nil {
		return err
	}
	w.leaves = append(w.leaves, childRef{key: w.leaf[0].key, pgid: pgid})
	w.leaf = w.leaf[:0]
	w.leafBytes = pageHeaderSize
	return nil
}

func (w *Writer) writeLeaf(entries []pageEntry) (uint64, error) {
	npages := pageSpan(entries, true, w.pageSize)
	pgid := w.allocate(npages)
	return pgid, w.writePages(pgid, encodePage(pgid, true, entries, w.pageSize))
}

// EndBucket finalizes the current bucket's tree and records its root.
func (w *Writer) EndBucket() error {
	if !w.inBucket {
		return fmt.Errorf("bboltfile: EndBucket without BeginBucket")
	}
	// An empty bucket is a single empty leaf page (as in a fresh bbolt DB).
	if len(w.leaves) == 0 && len(w.leaf) == 0 {
		pgid, err := w.writeLeaf(nil)
		if err != nil {
			return err
		}
		w.buckets = append(w.buckets, bucketRef{name: w.bucketName, rootPgid: pgid})
		w.finishBucket()
		return nil
	}
	if len(w.leaf) > 0 {
		if err := w.flushLeaf(); err != nil {
			return err
		}
	}
	root, err := w.buildBranches(w.leaves)
	if err != nil {
		return err
	}
	w.buckets = append(w.buckets, bucketRef{name: w.bucketName, rootPgid: root})
	w.finishBucket()
	return nil
}

func (w *Writer) finishBucket() {
	w.lastName = w.bucketName
	w.inBucket = false
	w.bucketName = nil
	w.leaves = nil
}

// buildBranches assembles branch pages over a sorted list of child pages,
// recursing until a single root page remains.
func (w *Writer) buildBranches(children []childRef) (uint64, error) {
	if len(children) == 1 {
		return children[0].pgid, nil
	}
	var parents []childRef
	i := 0
	for i < len(children) {
		group := []pageEntry{}
		size := pageHeaderSize
		for i < len(children) {
			need := elementSize + len(children[i].key)
			if len(group) > 0 && size+need > w.pageSize {
				break
			}
			group = append(group, pageEntry{key: children[i].key, pgid: children[i].pgid})
			size += need
			i++
		}
		npages := pageSpan(group, false, w.pageSize)
		pgid := w.allocate(npages)
		if err := w.writePages(pgid, encodePage(pgid, false, group, w.pageSize)); err != nil {
			return 0, err
		}
		parents = append(parents, childRef{key: group[0].key, pgid: pgid})
	}
	return w.buildBranches(parents)
}

// Close writes the root bucket tree, the (empty) freelist, and both meta pages,
// then syncs and closes the file.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.inBucket {
		w.f.Close()
		return fmt.Errorf("bboltfile: Close with bucket %q still open", w.bucketName)
	}

	// Root bucket: keys are bucket names, values are InBucket headers, marked
	// with the bucket-leaf flag. Buckets were added in ascending name order.
	rootEntries := make([]pageEntry, 0, len(w.buckets))
	for _, b := range w.buckets {
		hdr := make([]byte, inBucketSize)
		le.PutUint64(hdr[0:], b.rootPgid) // root
		le.PutUint64(hdr[8:], 0)          // sequence (unused by etcd)
		rootEntries = append(rootEntries, pageEntry{key: b.name, value: hdr, flags: bucketLeafFlag})
	}
	rootPgid, err := w.writeRootBucket(rootEntries)
	if err != nil {
		w.f.Close()
		return err
	}

	// Empty freelist page at pgid 2.
	fl := make([]byte, w.pageSize)
	le.PutUint64(fl[0:], 2)
	le.PutUint16(fl[8:], freelistPageFlag)
	if err := w.writePages(2, fl); err != nil {
		w.f.Close()
		return err
	}

	// Two meta pages (txid 0 and 1) pointing at the root bucket.
	highWater := w.nextPgid
	for i := uint64(0); i < 2; i++ {
		if err := w.writePages(i, w.encodeMeta(i, i, rootPgid, highWater)); err != nil {
			w.f.Close()
			return err
		}
	}

	if err := w.f.Truncate(int64(highWater) * int64(w.pageSize)); err != nil {
		w.f.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// writeRootBucket builds the top-level bucket index the same way as any bucket:
// a leaf if it fits in one page, otherwise a leaf/branch tree.
func (w *Writer) writeRootBucket(entries []pageEntry) (uint64, error) {
	// Fast path: everything fits in one leaf page (the common case — a handful
	// of buckets).
	if pageSpan(entries, true, w.pageSize) == 1 {
		pgid := w.allocate(1)
		return pgid, w.writePages(pgid, encodePage(pgid, true, entries, w.pageSize))
	}
	// Otherwise split into multiple leaves and build branches over them.
	var leaves []childRef
	i := 0
	for i < len(entries) {
		group := []pageEntry{}
		size := pageHeaderSize
		for i < len(entries) {
			need := elementSize + len(entries[i].key) + len(entries[i].value)
			if len(group) > 0 && size+need > w.pageSize {
				break
			}
			group = append(group, entries[i])
			size += need
			i++
		}
		pgid, err := w.writeLeaf(group)
		if err != nil {
			return 0, err
		}
		leaves = append(leaves, childRef{key: group[0].key, pgid: pgid})
	}
	return w.buildBranches(leaves)
}

// encodeMeta renders a meta page: a page header (id, meta flag) followed by the
// 64-byte meta struct with an fnv-1a checksum over its first 56 bytes.
func (w *Writer) encodeMeta(pageID, txid, rootPgid, highWater uint64) []byte {
	buf := make([]byte, w.pageSize)
	le.PutUint64(buf[0:], pageID)
	le.PutUint16(buf[8:], metaPageFlag)

	m := buf[pageHeaderSize : pageHeaderSize+metaSize]
	le.PutUint32(m[0:], boltMagic)
	le.PutUint32(m[4:], boltVersion)
	le.PutUint32(m[8:], uint32(w.pageSize))
	le.PutUint32(m[12:], 0)         // flags
	le.PutUint64(m[16:], rootPgid)  // root.root
	le.PutUint64(m[24:], 0)         // root.sequence
	le.PutUint64(m[32:], 2)         // freelist pgid
	le.PutUint64(m[40:], highWater) // pgid (high water mark)
	le.PutUint64(m[48:], txid)      // txid
	h := fnv.New64a()
	h.Write(m[:56])
	le.PutUint64(m[56:], h.Sum64()) // checksum
	return buf
}

// pageSpan returns how many physical pages a page of entries occupies.
func pageSpan(entries []pageEntry, isLeaf bool, pageSize int) int {
	total := pageHeaderSize + elementSize*len(entries)
	for _, e := range entries {
		total += len(e.key)
		if isLeaf {
			total += len(e.value)
		}
	}
	n := (total + pageSize - 1) / pageSize
	if n == 0 {
		n = 1
	}
	return n
}
