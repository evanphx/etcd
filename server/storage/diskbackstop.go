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

package storage

// DefaultDiskReserveBytes is the default free-space margin the disk backstop
// keeps on the backend filesystem. Writes are hard-rejected (NOSPACE) once free
// space would fall below it, so etcd fails into read-only rather than crashing
// on a real ENOSPC. It is the last-resort backstop behind the soft physical
// quota's write throttle.
const DefaultDiskReserveBytes = int64(100 << 20) // 100 MiB

// DiskBackstop is the hard physical safety valve: it rejects writes when the
// backend filesystem is near exhaustion, independent of the soft
// (--quota-backend-bytes / --quota-logical-bytes) quotas that only throttle.
// It measures real free space via statfs, so unlike a physical-size quota it
// never trips on an engine's transient space amplification (bbolt freelist /
// Pebble compaction backlog) — only on genuine disk pressure.
type DiskBackstop struct {
	path    string
	reserve int64
}

// NewDiskBackstop watches the filesystem containing path, keeping reserve bytes
// free. reserve == 0 uses DefaultDiskReserveBytes; reserve < 0 disables the
// backstop.
func NewDiskBackstop(path string, reserve int64) *DiskBackstop {
	if reserve == 0 {
		reserve = DefaultDiskReserveBytes
	}
	return &DiskBackstop{path: path, reserve: reserve}
}

// Admits reports whether a write of the given cost can proceed without pushing
// the backend filesystem below its free-space reserve. It fails open (true)
// when the backstop is disabled (reserve < 0), the path is unset, or free space
// cannot be measured — availability is preferred to a false disk-full.
func (d *DiskBackstop) Admits(cost int64) bool {
	if d == nil || d.reserve < 0 || d.path == "" {
		return true
	}
	avail, ok := availableDiskBytes(d.path)
	if !ok {
		return true
	}
	return avail-cost >= d.reserve
}

// AvailableBytes returns the measured free bytes on the backend filesystem and
// whether the measurement succeeded.
func (d *DiskBackstop) AvailableBytes() (int64, bool) {
	if d == nil || d.path == "" {
		return 0, false
	}
	return availableDiskBytes(d.path)
}
