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

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/storage/backend"
)

// fixedSizeBackend is a Backend stub whose only meaningful method is Size().
type fixedSizeBackend struct {
	backend.Backend
	physical int64
}

func (f *fixedSizeBackend) Size() int64 { return f.physical }

// TestHardQuotaPhysicalSize verifies that hard mode (the default) rejects writes
// once the backend's physical size would exceed --quota-backend-bytes.
func TestHardQuotaPhysicalSize(t *testing.T) {
	lg := zaptest.NewLogger(t)
	const maxBytes = 4096
	put := &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}

	full := NewQuota(QuotaModeHard, lg, maxBytes, &fixedSizeBackend{physical: 100 << 10}, "t", "", 0)
	require.False(t, full.Available(put), "physical over quota should be rejected")

	room := NewQuota(QuotaModeHard, lg, maxBytes, &fixedSizeBackend{physical: 100}, "t", "", 0)
	require.True(t, room.Available(put), "physical under quota should be admitted")
}

// TestSoftQuotaUsesDiskBackstop verifies that soft mode ignores the physical
// size entirely and gates only on real free disk: a huge physical size still
// admits writes as long as the filesystem has room beyond the reserve.
func TestSoftQuotaUsesDiskBackstop(t *testing.T) {
	lg := zaptest.NewLogger(t)
	dir := t.TempDir()
	avail, ok := availableDiskBytes(dir)
	if !ok {
		t.Skip("statfs unavailable")
	}
	put := &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}

	// Physical size wildly over any byte quota, but the disk has room: admitted.
	q := NewQuota(QuotaModeSoft, lg, 4096, &fixedSizeBackend{physical: 1 << 40}, "t", dir, avail/2)
	require.True(t, q.Available(put), "soft mode must not reject on physical size")

	// Reserve above free disk: the backstop rejects (real disk-full).
	full := NewQuota(QuotaModeSoft, lg, 4096, &fixedSizeBackend{physical: 0}, "t", dir, avail+(1<<30))
	require.False(t, full.Available(put), "disk backstop should reject near disk-full")

	// Negative reserve disables the backstop -> passthrough (always available).
	off := NewQuota(QuotaModeSoft, lg, 4096, &fixedSizeBackend{physical: 0}, "t", dir, -1)
	require.True(t, off.Available(put))
}
