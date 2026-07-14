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
// opens it. The snapshot artifact is a bbolt-format file regardless of the
// sender's engine: a bbolt member renames it into place; a Pebble member
// converts it into a Pebble store.
func OpenSnapshotBackend(cfg config.ServerConfig, ss *snap.Snapshotter, snapshot *raftpb.Snapshot, hooks *BackendHooks) (backend.Backend, error) {
	snapPath, err := ss.DBFilePath(snapshot.Metadata.GetIndex())
	if err != nil {
		return nil, fmt.Errorf("failed to find database snapshot file (%w)", err)
	}
	if backend.Engine(cfg.BackendEngine) == backend.EnginePebble {
		if err := installPebbleFromBboltSnapshot(cfg, snapPath); err != nil {
			return nil, fmt.Errorf("failed to install snapshot into pebble (%w)", err)
		}
	} else if err := os.Rename(snapPath, cfg.BackendPath()); err != nil {
		return nil, fmt.Errorf("failed to rename database snapshot file (%w)", err)
	}
	return OpenBackend(cfg, hooks), nil
}

// installPebbleFromBboltSnapshot converts a received bbolt-format snapshot into
// a Pebble store, atomically swapping it into the backend path and consuming the
// snapshot artifact.
func installPebbleFromBboltSnapshot(cfg config.ServerConfig, snapPath string) error {
	bp := cfg.BackendPath()
	staging := bp + ".snap.tmp"
	if err := os.RemoveAll(staging); err != nil {
		return err
	}
	if err := backend.ImportBboltIntoPebble(cfg.Logger, snapPath, staging); err != nil {
		os.RemoveAll(staging)
		return fmt.Errorf("failed to convert bbolt snapshot into pebble: %w", err)
	}
	if err := os.RemoveAll(bp); err != nil {
		os.RemoveAll(staging)
		return err
	}
	if err := os.Rename(staging, bp); err != nil {
		os.RemoveAll(staging)
		return err
	}
	os.Remove(snapPath) // consume the snapshot artifact

	// fsync the parent directory so the rename is durable across a crash.
	d, err := os.Open(filepath.Dir(bp))
	if err != nil {
		return err
	}
	defer d.Close()
	return fileutil.Fsync(d)
}

// maybeConvertBackendEngine converts an existing database in place when its
// on-disk format does not match --backend-engine, so an operator can switch
// engines by only changing the flag. bbolt databases are single files; Pebble
// stores are directories, which is how the current format is detected. It is a
// no-op for a fresh member or when the format already matches.
func maybeConvertBackendEngine(cfg config.ServerConfig) error {
	bp := cfg.BackendPath()
	info, err := os.Stat(bp)
	if os.IsNotExist(err) {
		return nil // fresh member
	}
	if err != nil {
		return err
	}
	wantPebble := backend.Engine(cfg.BackendEngine) == backend.EnginePebble
	isPebble := info.IsDir()
	if wantPebble == isPebble {
		return nil // already in the configured format
	}

	toBbolt := !wantPebble
	cfg.Logger.Info("converting existing database to the configured backend engine",
		zap.String("from", engineName(isPebble)),
		zap.String("to", cfg.BackendEngine),
		zap.String("path", bp))
	start := time.Now()
	if err := convertDatabaseEngine(cfg, toBbolt); err != nil {
		return err
	}
	cfg.Logger.Info("converted existing database to the configured backend engine",
		zap.String("to", cfg.BackendEngine),
		zap.Duration("took", time.Since(start)))
	return nil
}

func engineName(isDir bool) string {
	if isDir {
		return string(backend.EnginePebble)
	}
	return string(backend.EngineBBolt)
}

// convertDatabaseEngine rebuilds the database at the backend path in the other
// engine's format. It builds the new store beside the original (leaving the
// original untouched during the build), then swaps: move the original aside,
// install the new store, delete the original. A crash during the swap is
// recovered on the next start via the leftover backup.
func convertDatabaseEngine(cfg config.ServerConfig, toBbolt bool) error {
	bp := cfg.BackendPath()
	staging := bp + ".engineconv"
	backup := bp + ".preconv"

	// Recover from a crash during a previous swap (original moved to backup).
	if fileutil.Exist(backup) {
		if err := finishConversionSwap(bp, staging, backup); err != nil {
			return err
		}
		if !conversionNeeded(bp, toBbolt) {
			return nil
		}
	}

	if err := os.RemoveAll(staging); err != nil {
		return err
	}
	if toBbolt {
		if err := backend.ExportPebbleToBbolt(cfg.Logger, bp, staging); err != nil {
			os.RemoveAll(staging)
			return err
		}
	} else {
		if err := backend.ImportBboltIntoPebble(cfg.Logger, bp, staging); err != nil {
			os.RemoveAll(staging)
			return err
		}
	}

	// Swap. Each rename is atomic; the only crash window is between the two,
	// which finishConversionSwap recovers.
	if err := os.Rename(bp, backup); err != nil {
		os.RemoveAll(staging)
		return err
	}
	if err := os.Rename(staging, bp); err != nil {
		os.Rename(backup, bp) // best-effort restore of the original
		return err
	}
	if err := os.RemoveAll(backup); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(bp))
}

// finishConversionSwap completes an interrupted swap, given that the original
// database still exists at backup.
func finishConversionSwap(bp, staging, backup string) error {
	switch {
	case fileutil.Exist(bp):
		// The new store is already installed at bp; drop the leftover backup.
		return os.RemoveAll(backup)
	case fileutil.Exist(staging):
		// The new store was built but not yet installed; install it.
		if err := os.Rename(staging, bp); err != nil {
			return err
		}
		return os.RemoveAll(backup)
	default:
		// Only the original remains; restore it and let conversion retry.
		return os.Rename(backup, bp)
	}
}

// conversionNeeded reports whether the db at bp is still in the source format.
func conversionNeeded(bp string, toBbolt bool) bool {
	info, err := os.Stat(bp)
	if err != nil {
		return false
	}
	if toBbolt {
		return info.IsDir() // still a Pebble store
	}
	return !info.IsDir() // still a bbolt file
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return fileutil.Fsync(d)
}

// OpenBackend returns a backend using the current etcd db.
func OpenBackend(cfg config.ServerConfig, hooks backend.Hooks) backend.Backend {
	// If an existing database is in the other engine's format, convert it in
	// place so an operator can switch engines by just changing --backend-engine.
	if err := maybeConvertBackendEngine(cfg); err != nil {
		cfg.Logger.Panic("failed to convert the existing database to the configured backend engine",
			zap.String("backend-engine", cfg.BackendEngine),
			zap.String("path", cfg.BackendPath()),
			zap.Error(err))
	}

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
