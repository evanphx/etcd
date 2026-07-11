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

package embed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

// TestBackendEngineAutoConvert verifies that restarting a member with a
// different --backend-engine converts its existing database in place (both
// directions) and preserves the data.
func TestBackendEngineAutoConvert(t *testing.T) {
	urls := newConfigTestURLs()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "member", "snap", "db")

	start := func(engine string) *Etcd {
		cfg := NewConfig()
		applyTestURLConfig(cfg, urls)
		cfg.Dir = dir
		cfg.BackendEngine = engine
		cfg.LogLevel = "error"
		e, err := StartEtcd(cfg)
		require.NoError(t, err)
		select {
		case <-e.Server.ReadyNotify():
		case <-time.After(30 * time.Second):
			t.Fatalf("etcd (%s) failed to become ready", engine)
		}
		return e
	}

	const n = 50
	key := func(i int) string { return fmt.Sprintf("k%03d", i) }
	val := func(i int) string { return fmt.Sprintf("v%03d-payload", i) }

	// Phase 1: bbolt member writes data.
	e := start("bbolt")
	c := v3client.New(e.Server)
	for i := 0; i < n; i++ {
		_, err := c.Put(context.Background(), key(i), val(i))
		require.NoError(t, err)
	}
	c.Close()
	e.Close()
	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	require.False(t, info.IsDir(), "bbolt db should be a file")

	// Phase 2: restart as pebble -> auto-convert bbolt -> pebble.
	verify := func(engine string, wantDir bool) {
		e := start(engine)
		defer e.Close()
		c := v3client.New(e.Server)
		defer c.Close()
		for i := 0; i < n; i++ {
			gr, err := c.Get(context.Background(), key(i))
			require.NoError(t, err)
			require.Len(t, gr.Kvs, 1, "%s: key %s missing after conversion", engine, key(i))
			require.Equal(t, val(i), string(gr.Kvs[0].Value))
		}
		info, err := os.Stat(dbPath)
		require.NoError(t, err)
		require.Equal(t, wantDir, info.IsDir(), "%s db format", engine)
	}
	verify("pebble", true) // converted to a pebble store directory

	// Phase 3: restart as bbolt -> auto-convert pebble -> bbolt.
	verify("bbolt", false) // converted back to a bbolt file
}
