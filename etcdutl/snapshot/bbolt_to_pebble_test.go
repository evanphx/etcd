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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// saveBboltSnapshot boots a bbolt-backed etcd, writes n keys, and saves a client
// snapshot (a bbolt file + sha256) to a file, returning its path.
func saveBboltSnapshot(t *testing.T, n int) string {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.BackendEngine = "bbolt"
	cfg.BackendBatchLimit = 1
	cfg.LogLevel = "fatal"
	cfg.Dir = t.TempDir()

	etcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	defer etcd.Close()
	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		t.Fatal("etcd (bbolt) not ready")
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

// TestRestoreBboltSnapshotIntoPebble verifies etcdutl snapshot restore
// --backend-engine=pebble converts a bbolt snapshot into a Pebble data
// directory that holds the same data — the offline cross-engine migration path.
func TestRestoreBboltSnapshotIntoPebble(t *testing.T) {
	lg := zaptest.NewLogger(t)
	snapPath := saveBboltSnapshot(t, 10)

	outDir := filepath.Join(t.TempDir(), "restored")
	err := NewV3(lg).Restore(RestoreConfig{
		SnapshotPath:        snapPath,
		Name:                "default",
		OutputDataDir:       outDir,
		PeerURLs:            []string{"http://localhost:2380"},
		InitialCluster:      "default=http://localhost:2380",
		InitialClusterToken: "etcd-cluster",
		BackendEngine:       "pebble", // convert bbolt -> pebble
	})
	require.NoError(t, err)

	// The restored backend is now a Pebble store directory holding the data.
	dbPath := filepath.Join(outDir, "member", "snap", "db")
	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "converted pebble backend path should be a directory")

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
	require.Positive(t, count, "restored pebble store should contain keyspace revisions")
}

// TestRestorePebbleSnapshotIntoBbolt verifies the reverse conversion: a Pebble
// snapshot restored with --backend-engine=bbolt becomes a bbolt data directory.
func TestRestorePebbleSnapshotIntoBbolt(t *testing.T) {
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
		BackendEngine:       "bbolt", // convert pebble -> bbolt
	})
	require.NoError(t, err)

	// The restored backend is now a bbolt file holding the data.
	dbPath := filepath.Join(outDir, "member", "snap", "db")
	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.False(t, info.IsDir(), "bbolt backend path should be a file")

	be := backend.NewDefaultBackend(lg, dbPath, backend.WithEngine(backend.EngineBBolt))
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
	require.Positive(t, count, "restored bbolt store should contain keyspace revisions")
}
