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

package mvcc

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
)

func diskBytes(path string) int64 {
	var total int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func mib(n int64) string { return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20)) }

type heavyWriteResult struct {
	writeDisk, compactDisk, defragDisk, reopenDisk int64
	compactInUse                                   int64
}

func runHeavySingleKey(t *testing.T, engine backend.Engine, overwrites int) heavyWriteResult {
	path := filepath.Join(t.TempDir(), "db")
	bcfg := backend.DefaultBackendConfig(zap.NewNop())
	bcfg.Engine = engine
	bcfg.Path = path
	be := backend.New(bcfg)
	s := NewStore(zap.NewNop(), be, &lease.FakeLessor{}, StoreConfig{})

	val := make([]byte, 64)
	for i := 0; i < overwrites; i++ {
		s.Put([]byte("foo"), val, lease.NoLease)
	}
	be.ForceCommit()
	var r heavyWriteResult
	r.writeDisk = diskBytes(path)

	// etcd compaction: keep only the latest revision of "foo".
	ch, err := s.Compact(traceutil.TODO(), s.Rev())
	require.NoError(t, err)
	<-ch
	be.ForceCommit()
	r.compactDisk = diskBytes(path)
	r.compactInUse = be.SizeInUse()

	require.NoError(t, be.Defrag())
	be.ForceCommit()
	r.defragDisk = diskBytes(path)

	require.NoError(t, s.Close())
	require.NoError(t, be.Close())

	// Reopen replays+rotates the WAL, dropping any WAL-held bytes.
	be2 := backend.New(bcfg)
	r.reopenDisk = diskBytes(path)
	require.NoError(t, be2.Close())

	t.Logf("engine=%-6s writes=%s compact=%s defrag=%s reopen=%s (compact SizeInUse=%s)",
		engine, mib(r.writeDisk), mib(r.compactDisk), mib(r.defragDisk), mib(r.reopenDisk), mib(r.compactInUse))
	return r
}

// TestHeavySingleKeyWrite documents and guards the on-disk behavior when one key
// is overwritten many times and then compacted, on each engine:
//
//   - Revision accumulation is engine-agnostic; both grow during the burst.
//   - bbolt's B+tree file does NOT shrink on etcd compaction (only SizeInUse
//     drops); reclaiming the file requires a stop-the-world Defrag.
//   - Pebble reclaims space online after compaction (no Defrag needed), uses far
//     less disk during the burst (LSM compression of repetitive data), and any
//     residual WAL bytes clear on reopen.
func TestHeavySingleKeyWrite(t *testing.T) {
	const overwrites = 300_000

	bb := runHeavySingleKey(t, backend.EngineBBolt, overwrites)
	pb := runHeavySingleKey(t, backend.EnginePebble, overwrites)

	// bbolt: compaction frees revisions logically (SizeInUse ~0) but the file
	// does not shrink until Defrag.
	assert.Less(t, bb.compactInUse, bb.writeDisk/10, "bbolt compaction should free live bytes")
	assert.GreaterOrEqual(t, bb.compactDisk, bb.writeDisk*9/10, "bbolt file should NOT shrink on compaction (needs defrag)")
	assert.Less(t, bb.defragDisk, bb.writeDisk/10, "bbolt defrag should reclaim the file")

	// pebble: reclaims online after compaction WITHOUT a defrag, and uses less
	// disk than bbolt during the burst.
	assert.Less(t, pb.compactDisk, pb.writeDisk*8/10, "pebble should reclaim space on compaction without defrag")
	assert.Less(t, pb.writeDisk, bb.writeDisk, "pebble should use less disk than bbolt during the write burst")

	// both engines end small after their reclaim + reopen.
	assert.Less(t, bb.reopenDisk, bb.writeDisk/10)
	assert.Less(t, pb.reopenDisk, pb.writeDisk/2)
}
