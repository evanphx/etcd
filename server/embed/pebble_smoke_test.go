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
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

// TestPebbleServerSmoke boots a full single-node etcd on the Pebble backend and
// exercises the primary client paths end to end: Put/Get/Delete, a Txn, a
// Compact, and a client Snapshot (which streams the Pebble checkpoint tar
// through the maintenance RPC).
func TestPebbleServerSmoke(t *testing.T) {
	cfg := NewConfig()
	applyTestURLConfig(cfg, newConfigTestURLs())
	cfg.Dir = t.TempDir()
	cfg.BackendEngine = "pebble"
	// Exercise the memory knobs on the full-server path.
	cfg.PebbleCacheBytes = 8 << 20
	cfg.PebbleMemTableBytes = 2 << 20
	cfg.PebbleMemTableStopWritesThreshold = 2
	cfg.PebbleMaxOpenFiles = 100

	e, err := StartEtcd(cfg)
	require.NoError(t, err)
	defer e.Close()

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("etcd (pebble) failed to become ready")
	}

	client := v3client.New(e.Server)
	defer client.Close()
	ctx := t.Context()

	// Put / Get.
	_, err = client.Put(ctx, "foo", "bar")
	require.NoError(t, err)
	gr, err := client.Get(ctx, "foo")
	require.NoError(t, err)
	require.Len(t, gr.Kvs, 1)
	assert.Equal(t, "bar", string(gr.Kvs[0].Value))

	// Txn: compare-and-set.
	_, err = client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value("foo"), "=", "bar")).
		Then(clientv3.OpPut("foo", "baz")).
		Commit()
	require.NoError(t, err)
	gr, err = client.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, "baz", string(gr.Kvs[0].Value))

	// Delete.
	_, err = client.Delete(ctx, "foo")
	require.NoError(t, err)
	gr, err = client.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Empty(t, gr.Kvs)

	// Write a few keys, then Compact — exercises revision deletion on Pebble.
	for _, k := range []string{"a", "b", "c"} {
		_, err = client.Put(ctx, k, k)
		require.NoError(t, err)
	}
	cur, err := client.Get(ctx, "a")
	require.NoError(t, err)
	_, err = client.Compact(ctx, cur.Header.Revision)
	require.NoError(t, err)

	// Client Snapshot streams the Pebble checkpoint tar through the RPC.
	rc, err := client.Snapshot(ctx)
	require.NoError(t, err)
	n, err := io.Copy(io.Discard, rc)
	rc.Close()
	require.NoError(t, err)
	assert.Positive(t, n, "snapshot should stream a non-empty archive")
}
