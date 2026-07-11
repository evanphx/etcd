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

	"github.com/stretchr/testify/require"
)

// TestFinishConversionSwap covers recovery from a crash during the two-rename
// swap at the end of an engine conversion, given the original still exists at
// the backup path.
func TestFinishConversionSwap(t *testing.T) {
	write := func(p, content string) {
		require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	}
	readFile := func(p string) string {
		b, err := os.ReadFile(p)
		require.NoError(t, err)
		return string(b)
	}

	t.Run("new-store-already-installed", func(t *testing.T) {
		dir := t.TempDir()
		bp := filepath.Join(dir, "db")
		staging, backup := bp+".engineconv", bp+".preconv"
		write(bp, "new")     // new store already renamed into place
		write(backup, "old") // leftover backup from the interrupted swap

		require.NoError(t, finishConversionSwap(bp, staging, backup))
		require.Equal(t, "new", readFile(bp), "installed store is kept")
		require.NoFileExists(t, backup, "backup is dropped")
	})

	t.Run("install-built-store", func(t *testing.T) {
		dir := t.TempDir()
		bp := filepath.Join(dir, "db")
		staging, backup := bp+".engineconv", bp+".preconv"
		write(staging, "new") // built but not yet installed
		write(backup, "old")  // original moved aside; bp missing

		require.NoError(t, finishConversionSwap(bp, staging, backup))
		require.Equal(t, "new", readFile(bp), "built store is installed")
		require.NoFileExists(t, staging)
		require.NoFileExists(t, backup)
	})

	t.Run("restore-original", func(t *testing.T) {
		dir := t.TempDir()
		bp := filepath.Join(dir, "db")
		staging, backup := bp+".engineconv", bp+".preconv"
		write(backup, "orig") // only the original remains

		require.NoError(t, finishConversionSwap(bp, staging, backup))
		require.Equal(t, "orig", readFile(bp), "original is restored for retry")
		require.NoFileExists(t, backup)
	})
}

// TestConversionNeeded checks the source-format detection used after recovery.
func TestConversionNeeded(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "bboltdb")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	pebbleDir := filepath.Join(dir, "pebbledir")
	require.NoError(t, os.Mkdir(pebbleDir, 0o700))

	// toBbolt: need conversion while the db is still a (pebble) directory.
	require.True(t, conversionNeeded(pebbleDir, true))
	require.False(t, conversionNeeded(file, true))
	// toPebble: need conversion while the db is still a (bbolt) file.
	require.True(t, conversionNeeded(file, false))
	require.False(t, conversionNeeded(pebbleDir, false))
	// Missing path: nothing to convert.
	require.False(t, conversionNeeded(filepath.Join(dir, "nope"), true))
}
