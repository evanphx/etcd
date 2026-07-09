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
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

// PebbleSnapshotVersionFile is a marker file written into every Pebble snapshot
// tar identifying the archive format. It lets the receive/restore path
// distinguish a Pebble snapshot (a tar of a checkpoint directory) from a legacy
// single-file bbolt snapshot.
const PebbleSnapshotVersionFile = "etcd-pebble-snapshot-v1"

// pebbleSnapshot is a Snapshot whose bytes are a tar archive of a Pebble
// checkpoint. The tar is materialized to a temp file up front so that Size()
// exactly matches the number of bytes WriteTo streams (required by the snapshot
// RPC's RemainingBytes protocol).
type pebbleSnapshot struct {
	tarPath string
	tmpDir  string
	size    int64
	lg      *zap.Logger

	stopc chan struct{}
	donec chan struct{}
}

// snapshot builds the checkpoint + tar and returns a streamable Snapshot.
func (b *pebbleBackend) snapshot() Snapshot {
	// Flush pending writes so the checkpoint reflects all committed data.
	b.batchTx.Commit()

	b.mu.RLock()
	defer b.mu.RUnlock()

	// Work in a temp dir on the same filesystem as the store so the checkpoint
	// can hard-link SSTables cheaply.
	parent := filepath.Dir(b.path)
	tmpDir, err := os.MkdirTemp(parent, "pebble-snap-")
	if err != nil {
		b.lg.Fatal("failed to create snapshot temp dir", zap.Error(err))
	}

	checkpointDir := filepath.Join(tmpDir, "checkpoint")
	if err := b.db.Checkpoint(checkpointDir, pebble.WithFlushedWAL()); err != nil {
		os.RemoveAll(tmpDir)
		b.lg.Fatal("failed to create pebble checkpoint", zap.Error(err))
	}

	tarPath := filepath.Join(tmpDir, "snapshot.tar")
	size, err := buildSnapshotTar(tarPath, checkpointDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.lg.Fatal("failed to build snapshot tar", zap.Error(err))
	}
	// The checkpoint dir has been archived into the tar; drop it to reclaim space.
	os.RemoveAll(checkpointDir)

	stopc, donec := make(chan struct{}), make(chan struct{})
	go snapshotWarnLoop(b.lg, size, stopc, donec)

	return &pebbleSnapshot{
		tarPath: tarPath,
		tmpDir:  tmpDir,
		size:    size,
		lg:      b.lg,
		stopc:   stopc,
		donec:   donec,
	}
}

func (s *pebbleSnapshot) Size() int64 { return s.size }

func (s *pebbleSnapshot) WriteTo(w io.Writer) (int64, error) {
	f, err := os.Open(s.tarPath)
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

// buildSnapshotTar archives every file under srcDir into a tar at tarPath and
// returns the tar's byte length. A marker file is added first so the format is
// self-identifying. The tar is fsynced so its on-disk size is stable.
func buildSnapshotTar(tarPath, srcDir string) (int64, error) {
	f, err := os.Create(tarPath)
	if err != nil {
		return 0, err
	}
	tw := tar.NewWriter(f)

	// Marker entry (empty file) identifying the snapshot format.
	if err := tw.WriteHeader(&tar.Header{
		Name: PebbleSnapshotVersionFile,
		Mode: 0o600,
		Size: 0,
	}); err != nil {
		f.Close()
		return 0, err
	}

	err = filepath.WalkDir(srcDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(filepath.Join("checkpoint", rel))
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		_, err = io.Copy(tw, in)
		return err
	})
	if err != nil {
		tw.Close()
		f.Close()
		return 0, err
	}

	if err := tw.Close(); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return 0, err
	}
	size := fi.Size()
	return size, f.Close()
}

// UntarPebbleSnapshot extracts a Pebble snapshot tar (as produced by
// pebbleSnapshot) from r into destDir, returning the path to the extracted
// Pebble store directory. destDir must already exist. It rejects entries with
// path traversal, and errors if the marker file is absent (i.e. the stream is
// not a Pebble snapshot).
func UntarPebbleSnapshot(r io.Reader, destDir string) (string, error) {
	tr := tar.NewReader(r)
	sawMarker := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if hdr.Name == PebbleSnapshotVersionFile {
			sawMarker = true
			continue
		}
		target := filepath.Join(destDir, filepath.Clean(hdr.Name))
		if !isWithin(destDir, target) {
			return "", fmt.Errorf("pebble snapshot: illegal path in archive: %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return "", err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return "", err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
			if err != nil {
				return "", err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return "", err
			}
			if err := out.Close(); err != nil {
				return "", err
			}
		default:
			// skip other entry types
		}
	}
	if !sawMarker {
		return "", fmt.Errorf("pebble snapshot: missing %s marker (not a pebble snapshot)", PebbleSnapshotVersionFile)
	}
	return filepath.Join(destDir, "checkpoint"), nil
}

// isWithin reports whether target is contained within dir, guarding against
// path traversal in archive entries.
func isWithin(dir, target string) bool {
	rel, err := filepath.Rel(dir, target)
	if err != nil {
		return false
	}
	return rel != ".." && !hasDotDotPrefix(rel)
}

func hasDotDotPrefix(rel string) bool {
	return len(rel) >= 3 && rel[0] == '.' && rel[1] == '.' && (rel[2] == filepath.Separator)
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
