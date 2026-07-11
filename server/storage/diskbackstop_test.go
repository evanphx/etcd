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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiskBackstop(t *testing.T) {
	dir := t.TempDir()

	avail, ok := availableDiskBytes(dir)
	if !ok {
		t.Skip("statfs unavailable on this platform")
	}
	require.Positive(t, avail)

	// A reserve just under the measured free space admits writes...
	ok1 := NewDiskBackstop(dir, avail-(1<<20))
	require.True(t, ok1.Admits(0))

	// ...while a reserve above free space rejects them (disk-full backstop).
	full := NewDiskBackstop(dir, avail+(1<<30))
	require.False(t, full.Admits(0))

	// reserve < 0 disables the backstop entirely.
	disabled := NewDiskBackstop(dir, -1)
	require.True(t, disabled.Admits(1<<40))

	// A nil backstop and an empty path both fail open.
	var nilbs *DiskBackstop
	require.True(t, nilbs.Admits(1<<40))
	require.True(t, NewDiskBackstop("", 0).Admits(1<<40))

	// reserve == 0 picks up the default.
	require.Equal(t, DefaultDiskReserveBytes, NewDiskBackstop(dir, 0).reserve)
}
