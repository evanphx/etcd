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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestPebbleTuningKnobsApplied verifies the --pebble-* memory knobs are actually
// wired into the Pebble Options: a small configured memtable must take effect
// (well below Pebble's 64 MiB default), proving the plumbing works.
func TestPebbleTuningKnobsApplied(t *testing.T) {
	const memTableBytes = 1 << 20 // 1 MiB, vs Pebble's 64 MiB default

	bcfg := DefaultBackendConfig(zaptest.NewLogger(t))
	bcfg.Engine = EnginePebble
	bcfg.Path = filepath.Join(t.TempDir(), "db")
	bcfg.PebbleMemTableBytes = memTableBytes
	bcfg.PebbleCacheBytes = 8 << 20
	bcfg.PebbleMemTableStopWritesThreshold = 2
	bcfg.PebbleMaxOpenFiles = 100
	bcfg.PebbleMaxConcurrentCompactions = 1

	be := New(bcfg)
	defer be.Close()

	pb, ok := be.(*pebbleBackend)
	require.True(t, ok, "expected *pebbleBackend")

	// The configured memtable size must be reflected (arena allocated up front),
	// proving the knob overrode the default rather than being ignored.
	m := pb.db.Metrics()
	assert.LessOrEqual(t, int64(m.MemTable.Size), int64(2*memTableBytes),
		"memtable size should reflect the small configured value, not the 64MiB default")

	// The cache was created and will be released on Close.
	assert.NotNil(t, pb.cache)
}
