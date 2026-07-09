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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// TestInstallPebbleSnapshot verifies the server-side snapshot install path:
// a Pebble snapshot tar is extracted and directory-swapped into the backend
// path, replacing any pre-existing store, and the installed store serves the
// snapshot's data.
func TestInstallPebbleSnapshot(t *testing.T) {
	lg := zaptest.NewLogger(t)
	dataDir := t.TempDir()
	cfg := config.ServerConfig{
		Logger:        lg,
		DataDir:       dataDir,
		BackendEngine: string(backend.EnginePebble),
	}

	// Build a source store and snapshot it to a tar file.
	srcPath := filepath.Join(t.TempDir(), "src")
	srcCfg := backend.DefaultBackendConfig(lg)
	srcCfg.Engine = backend.EnginePebble
	srcCfg.Path = srcPath
	src := backend.New(srcCfg)
	tx := src.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(schema.Key)
	tx.UnsafePut(schema.Key, []byte("snap-key"), []byte("snap-val"))
	tx.Unlock()
	src.ForceCommit()

	snap := src.Snapshot()
	snapPath := filepath.Join(t.TempDir(), "0001.snap.db")
	f, err := os.Create(snapPath)
	require.NoError(t, err)
	_, err = snap.WriteTo(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, snap.Close())
	require.NoError(t, src.Close())

	// Pre-existing (stale) store at the backend path, to be replaced.
	require.NoError(t, os.MkdirAll(filepath.Dir(cfg.BackendPath()), 0o700))
	require.NoError(t, os.MkdirAll(cfg.BackendPath(), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(cfg.BackendPath(), "stale"), []byte("x"), 0o600))

	// Install the snapshot.
	require.NoError(t, installPebbleSnapshot(cfg, snapPath))
	// The snapshot artifact is consumed.
	assert.NoFileExists(t, snapPath)

	// Open the installed backend and confirm the snapshot data is present.
	be := newBackend(cfg, backend.NewHooks(func(backend.UnsafeReadWriter) {}))
	defer be.Close()
	rtx := be.ReadTx()
	rtx.RLock()
	ks, vs := rtx.UnsafeRange(schema.Key, []byte("snap-key"), nil, 0)
	rtx.RUnlock()
	require.Len(t, ks, 1)
	assert.Equal(t, []byte("snap-val"), vs[0])
}
