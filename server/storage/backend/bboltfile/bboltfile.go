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

// Package bboltfile is a minimal, dependency-free codec for the bbolt on-disk
// database format. It can stream key/value pairs out of a bbolt file (Reader)
// and build a bbolt file from sorted key/value pairs (Writer) without opening
// the file through the bbolt library — no mmap, no file lock, no COW rewrite.
//
// It targets exactly the shape etcd produces: a flat set of top-level buckets,
// each a B+tree of byte keys to byte values, no nested buckets, keys non-empty.
// The Reader additionally understands inline buckets and overflow pages so it
// can read files written by the bbolt library; the Writer always emits
// non-inline buckets, which the bbolt library reads back and validates.
//
// bbolt files are little-endian (the library casts structs over mmap'd bytes),
// and are not portable across byte order; this codec matches by using
// little-endian encoding, which is correct on etcd's supported architectures.
package bboltfile

import "encoding/binary"

// Page header and element layout (see go.etcd.io/bbolt internal/common).
const (
	pageHeaderSize = 16 // id(8) flags(2) count(2) overflow(4)
	elementSize    = 16 // leaf: flags,pos,ksize,vsize; branch: pos,ksize,pgid
	metaSize       = 64 // magic..checksum
	inBucketSize   = 16 // root pgid(8) + sequence(8)

	// Page flags.
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10

	// Leaf element flag marking a sub-bucket value.
	bucketLeafFlag = 0x01

	boltMagic   = 0xED0CDAED
	boltVersion = 2

	// DefaultPageSize is the page size the Writer uses. The Reader honors
	// whatever page size a file declares in its meta page.
	DefaultPageSize = 4096
)

var le = binary.LittleEndian

// pageEntry is one element to write into a leaf or branch page. For a leaf,
// value is the payload and flags marks sub-buckets; for a branch, pgid is the
// child page and value is unused.
type pageEntry struct {
	key   []byte
	value []byte
	flags uint32
	pgid  uint64
}

// encodePage renders entries into a page buffer of (overflow+1) page-size
// blocks, matching bbolt's WriteInodeToPage layout: a page header, then a
// fixed-width element array, then each element's key (and value, for leaves)
// packed in order. pos in each element is the byte offset from that element's
// own start to its key.
func encodePage(pgid uint64, isLeaf bool, entries []pageEntry, pageSize int) []byte {
	dataBytes := 0
	for _, e := range entries {
		dataBytes += len(e.key)
		if isLeaf {
			dataBytes += len(e.value)
		}
	}
	total := pageHeaderSize + elementSize*len(entries) + dataBytes
	npages := (total + pageSize - 1) / pageSize
	if npages == 0 {
		npages = 1
	}
	buf := make([]byte, npages*pageSize)

	le.PutUint64(buf[0:], pgid)
	if isLeaf {
		le.PutUint16(buf[8:], leafPageFlag)
	} else {
		le.PutUint16(buf[8:], branchPageFlag)
	}
	le.PutUint16(buf[10:], uint16(len(entries)))
	le.PutUint32(buf[12:], uint32(npages-1)) // overflow

	off := pageHeaderSize + elementSize*len(entries)
	for i, e := range entries {
		elemOff := pageHeaderSize + elementSize*i
		pos := uint32(off - elemOff)
		if isLeaf {
			le.PutUint32(buf[elemOff:], e.flags)
			le.PutUint32(buf[elemOff+4:], pos)
			le.PutUint32(buf[elemOff+8:], uint32(len(e.key)))
			le.PutUint32(buf[elemOff+12:], uint32(len(e.value)))
			off += copy(buf[off:], e.key)
			off += copy(buf[off:], e.value)
		} else {
			le.PutUint32(buf[elemOff:], pos)
			le.PutUint32(buf[elemOff+4:], uint32(len(e.key)))
			le.PutUint64(buf[elemOff+8:], e.pgid)
			off += copy(buf[off:], e.key)
		}
	}
	return buf
}

// pageOverflow reads the overflow count from a page header.
func pageOverflow(hdr []byte) uint32 { return le.Uint32(hdr[12:]) }
func pageFlags(hdr []byte) uint16    { return le.Uint16(hdr[8:]) }
func pageCount(hdr []byte) uint16    { return le.Uint16(hdr[10:]) }
