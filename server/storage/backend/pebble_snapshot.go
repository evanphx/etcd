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
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

// pebbleSnapshot is a Snapshot whose bytes are a standard bbolt database file
// exported from a consistent Pebble snapshot. Using the bbolt format (rather
// than a Pebble-specific archive) means a Pebble member's snapshots are
// interchangeable with a bbolt member's: any receiver installs or converts them
// with the same code, and they interoperate with stock etcd tooling.
//
// The file is materialized up front so Size() exactly matches the bytes WriteTo
// streams (required by the snapshot RPC's RemainingBytes protocol).
type pebbleSnapshot struct {
	dbPath string
	tmpDir string
	size   int64
	lg     *zap.Logger

	stopc chan struct{}
	donec chan struct{}
}

// snapshot exports a consistent bbolt image of the store and returns it as a
// streamable Snapshot.
func (b *pebbleBackend) snapshot() Snapshot {
	// Flush pending writes so the snapshot reflects all committed data.
	b.batchTx.Commit()

	b.mu.RLock()
	defer b.mu.RUnlock()

	// Work in a temp dir on the same filesystem as the store.
	parent := filepath.Dir(b.path)
	tmpDir, err := os.MkdirTemp(parent, "pebble-snap-")
	if err != nil {
		b.lg.Fatal("failed to create snapshot temp dir", zap.Error(err))
	}

	dbPath := filepath.Join(tmpDir, "snapshot.db")
	snap := b.db.NewSnapshot()
	err = exportPebbleSnapshot(b.lg, snap, dbPath)
	snap.Close()
	if err != nil {
		os.RemoveAll(tmpDir)
		b.lg.Fatal("failed to export pebble snapshot to bbolt", zap.Error(err))
	}

	fi, err := os.Stat(dbPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.lg.Fatal("failed to stat exported snapshot", zap.Error(err))
	}
	size := fi.Size()

	stopc, donec := make(chan struct{}), make(chan struct{})
	go snapshotWarnLoop(b.lg, size, stopc, donec)

	return &pebbleSnapshot{
		dbPath: dbPath,
		tmpDir: tmpDir,
		size:   size,
		lg:     b.lg,
		stopc:  stopc,
		donec:  donec,
	}
}

func (s *pebbleSnapshot) Size() int64 { return s.size }

func (s *pebbleSnapshot) WriteTo(w io.Writer) (int64, error) {
	f, err := os.Open(s.dbPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(w, f)
}

func (s *pebbleSnapshot) Close() error {
	close(s.stopc)
	<-s.donec
	return os.RemoveAll(s.tmpDir)
}

// snapshotWarnLoop logs a warning if a snapshot transfer takes unusually long,
// mirroring the bbolt backend's behavior.
func snapshotWarnLoop(lg *zap.Logger, dbBytes int64, stopc, donec chan struct{}) {
	defer close(donec)
	var sendRateBytes int64 = 100 * 1024 * 1024
	warningTimeout := time.Duration(int64((float64(dbBytes) / float64(sendRateBytes)) * float64(time.Second)))
	if warningTimeout < minSnapshotWarningTimeout {
		warningTimeout = minSnapshotWarningTimeout
	}
	start := time.Now()
	ticker := time.NewTicker(warningTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			lg.Warn(
				"snapshotting taking too long to transfer",
				zap.Duration("taking", time.Since(start)),
				zap.Int64("bytes", dbBytes),
				zap.String("size", humanize.Bytes(uint64(dbBytes))),
			)
		case <-stopc:
			snapshotTransferSec.Observe(time.Since(start).Seconds())
			return
		}
	}
}
