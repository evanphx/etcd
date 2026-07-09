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

package snapshot

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// savePebbleSnapshot boots a Pebble-backed etcd, writes n keys, and saves a
// client snapshot (tar + sha256) to a file, returning its path.
func savePebbleSnapshot(t *testing.T, n int) string {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.BackendEngine = "pebble"
	cfg.BackendBatchLimit = 1
	cfg.LogLevel = "fatal"
	cfg.Dir = t.TempDir()

	etcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	defer etcd.Close()
	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		t.Fatal("etcd (pebble) not ready")
	}

	cli := v3client.New(etcd.Server)
	defer cli.Close()
	ctx := t.Context()
	for i := 0; i < n; i++ {
		_, err = cli.Put(ctx, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i))
		require.NoError(t, err)
	}

	snapPath := filepath.Join(t.TempDir(), "snapshot.db")
	rc, err := cli.Snapshot(ctx)
	require.NoError(t, err)
	f, err := os.Create(snapPath)
	require.NoError(t, err)
	_, err = io.Copy(f, rc)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, rc.Close())
	return snapPath
}

// TestPebbleSnapshotStatus verifies etcdutl snapshot status auto-detects a
// Pebble snapshot and reports its contents.
func TestPebbleSnapshotStatus(t *testing.T) {
	snapPath := savePebbleSnapshot(t, 10)

	// The snapshot must be detected as pebble, not bbolt.
	require.Equal(t, backend.EnginePebble, detectSnapshotEngine(snapPath))

	st, err := NewV3(zaptest.NewLogger(t)).Status(snapPath)
	require.NoError(t, err)
	assert.Equal(t, 10, st.TotalKey)
	assert.Positive(t, st.Revision)
	assert.Positive(t, st.TotalSize)
	assert.NotZero(t, st.Hash)
}

// TestPebbleSnapshotRestore verifies etcdutl snapshot restore reconstructs a
// data directory from a Pebble snapshot, and the restored store contains data.
func TestPebbleSnapshotRestore(t *testing.T) {
	lg := zaptest.NewLogger(t)
	snapPath := savePebbleSnapshot(t, 10)

	outDir := filepath.Join(t.TempDir(), "restored")
	err := NewV3(lg).Restore(RestoreConfig{
		SnapshotPath:        snapPath,
		Name:                "default",
		OutputDataDir:       outDir,
		PeerURLs:            []string{"http://localhost:2380"},
		InitialCluster:      "default=http://localhost:2380",
		InitialClusterToken: "etcd-cluster",
		SkipHashCheck:       false,
	})
	require.NoError(t, err)

	// The restored backend is a Pebble store directory holding the snapshot data.
	dbPath := filepath.Join(outDir, "member", "snap", "db")
	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "pebble backend path should be a directory")

	be := backend.NewDefaultBackend(lg, dbPath, backend.WithEngine(backend.EnginePebble))
	defer be.Close()
	rtx := be.ReadTx()
	rtx.RLock()
	count := 0
	err = rtx.UnsafeForEach(schema.Key, func(_, _ []byte) error {
		count++
		return nil
	})
	rtx.RUnlock()
	require.NoError(t, err)
	assert.Positive(t, count, "restored Key bucket should contain revisions")
}
