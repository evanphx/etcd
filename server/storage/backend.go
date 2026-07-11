// Copyright 2017 The etcd Authors
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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/raft/v3/raftpb"
)

func newBackend(cfg config.ServerConfig, hooks backend.Hooks) backend.Backend {
	bcfg := backend.DefaultBackendConfig(cfg.Logger)
	bcfg.Path = cfg.BackendPath()
	bcfg.UnsafeNoFsync = cfg.UnsafeNoFsync
	if cfg.BackendBatchLimit != 0 {
		bcfg.BatchLimit = cfg.BackendBatchLimit
		if cfg.Logger != nil {
			cfg.Logger.Info("setting backend batch limit", zap.Int("batch limit", cfg.BackendBatchLimit))
		}
	}
	if cfg.BackendBatchInterval != 0 {
		bcfg.BatchInterval = cfg.BackendBatchInterval
		if cfg.Logger != nil {
			cfg.Logger.Info("setting backend batch interval", zap.Duration("batch interval", cfg.BackendBatchInterval))
		}
	}
	bcfg.BackendFreelistType = cfg.BackendFreelistType
	bcfg.Engine = backend.Engine(cfg.BackendEngine)
	bcfg.PebbleCacheBytes = cfg.PebbleCacheBytes
	bcfg.PebbleMemTableBytes = cfg.PebbleMemTableBytes
	bcfg.PebbleMemTableStopWritesThreshold = cfg.PebbleMemTableStopWritesThreshold
	bcfg.PebbleMaxOpenFiles = cfg.PebbleMaxOpenFiles
	bcfg.PebbleMaxConcurrentCompactions = cfg.PebbleMaxConcurrentCompactions
	bcfg.Logger = cfg.Logger
	if cfg.QuotaBackendBytes > 0 && cfg.QuotaBackendBytes != DefaultQuotaBytes {
		// permit 10% excess over quota for disarm
		bcfg.MmapSize = uint64(cfg.QuotaBackendBytes + cfg.QuotaBackendBytes/10)
	}
	bcfg.Mlock = cfg.MemoryMlock
	bcfg.Hooks = hooks
	return backend.New(bcfg)
}

// OpenSnapshotBackend installs a received snapshot db as the current etcd db and
// opens it. For bbolt the snapshot artifact is a single file (renamed into
// place); for pebble it is a tar of a checkpoint directory (extracted and
// directory-swapped into place).
func OpenSnapshotBackend(cfg config.ServerConfig, ss *snap.Snapshotter, snapshot *raftpb.Snapshot, hooks *BackendHooks) (backend.Backend, error) {
	snapPath, err := ss.DBFilePath(snapshot.Metadata.GetIndex())
	if err != nil {
		return nil, fmt.Errorf("failed to find database snapshot file (%w)", err)
	}
	if backend.Engine(cfg.BackendEngine) == backend.EnginePebble {
		// A Pebble member installs either a Pebble checkpoint tar (same-engine)
		// or a bbolt-format snapshot (from a bbolt leader), converting the
		// latter into a Pebble store. See installPebbleSnapshot.
		if err := installPebbleSnapshot(cfg, snapPath); err != nil {
			return nil, fmt.Errorf("failed to install pebble snapshot (%w)", err)
		}
	} else if backend.IsPebbleSnapshot(snapPath) {
		// A bbolt member cannot install a Pebble snapshot; converting Pebble to
		// bbolt is not supported.
		return nil, fmt.Errorf("received a Pebble-format snapshot but this member runs the bbolt engine; cross-engine restore from Pebble to bbolt is not supported")
	} else if err := os.Rename(snapPath, cfg.BackendPath()); err != nil {
		return nil, fmt.Errorf("failed to rename database snapshot file (%w)", err)
	}
	return OpenBackend(cfg, hooks), nil
}

// installPebbleSnapshot installs a received snapshot into a Pebble backend and
// atomically swaps the resulting store directory into the backend path,
// replacing any existing store. The snapshot may be a Pebble checkpoint tar
// (same-engine, extracted) or a raw bbolt database file (from a bbolt leader,
// converted into a Pebble store). The snapshot artifact is consumed on success
// (mirroring the bbolt rename semantics).
func installPebbleSnapshot(cfg config.ServerConfig, snapPath string) error {
	bp := cfg.BackendPath()
	staging := bp + ".snap.tmp"
	if err := os.RemoveAll(staging); err != nil {
		return err
	}

	// storeDir is the freshly-materialized Pebble store to swap into place.
	var storeDir string
	if backend.IsPebbleSnapshot(snapPath) {
		if err := os.MkdirAll(staging, 0o700); err != nil {
			return err
		}
		f, err := os.Open(snapPath)
		if err != nil {
			os.RemoveAll(staging)
			return err
		}
		ckptDir, err := backend.UntarPebbleSnapshot(f, staging)
		f.Close()
		if err != nil {
			os.RemoveAll(staging)
			return err
		}
		storeDir = ckptDir
	} else {
		// A bbolt-format snapshot: convert it into a fresh Pebble store.
		if err := backend.ImportBboltIntoPebble(cfg.Logger, snapPath, staging); err != nil {
			os.RemoveAll(staging)
			return fmt.Errorf("failed to convert bbolt snapshot into pebble: %w", err)
		}
		storeDir = staging
	}

	// Replace the live store directory with the materialized one.
	if err := os.RemoveAll(bp); err != nil {
		os.RemoveAll(staging)
		return err
	}
	if err := os.Rename(storeDir, bp); err != nil {
		os.RemoveAll(staging)
		return err
	}
	os.RemoveAll(staging)
	os.Remove(snapPath) // consume the snapshot artifact

	// fsync the parent directory so the rename is durable across a crash.
	d, err := os.Open(filepath.Dir(bp))
	if err != nil {
		return err
	}
	defer d.Close()
	return fileutil.Fsync(d)
}

// OpenBackend returns a backend using the current etcd db.
func OpenBackend(cfg config.ServerConfig, hooks backend.Hooks) backend.Backend {
	fn := cfg.BackendPath()

	now, beOpened := time.Now(), make(chan backend.Backend)
	go func() {
		beOpened <- newBackend(cfg, hooks)
	}()

	defer func() {
		cfg.Logger.Info("opened backend db", zap.String("path", fn), zap.Duration("took", time.Since(now)))
	}()

	select {
	case be := <-beOpened:
		return be

	case <-time.After(10 * time.Second):
		cfg.Logger.Info(
			"db file is flocked by another process, or taking too long",
			zap.String("path", fn),
			zap.Duration("took", time.Since(now)),
		)
	}

	return <-beOpened
}

// RecoverSnapshotBackend recovers the DB from a snapshot in case etcd crashes
// before updating the backend db after persisting raft snapshot to disk,
// violating the invariant snapshot.Metadata.Index < db.consistentIndex. In this
// case, replace the db with the snapshot db sent by the leader.
func RecoverSnapshotBackend(cfg config.ServerConfig, oldbe backend.Backend, snapshot *raftpb.Snapshot, beExist bool, hooks *BackendHooks) (backend.Backend, error) {
	consistentIndex := uint64(0)
	if beExist {
		consistentIndex, _ = schema.ReadConsistentIndex(oldbe.ReadTx())
	}
	if snapshot.Metadata.GetIndex() <= consistentIndex {
		cfg.Logger.Info("Skipping snapshot backend", zap.Uint64("consistent-index", consistentIndex), zap.Uint64("snapshot-index", snapshot.Metadata.GetIndex()))
		return oldbe, nil
	}
	cfg.Logger.Info("Recovering from snapshot backend", zap.Uint64("consistent-index", consistentIndex), zap.Uint64("snapshot-index", snapshot.Metadata.GetIndex()))
	oldbe.Close()
	return OpenSnapshotBackend(cfg, snap.New(cfg.Logger, cfg.SnapDir()), snapshot, hooks)
}
