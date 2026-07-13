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

package mvcc

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
)

func newEngineBackend(t *testing.T, engine backend.Engine, path string) backend.Backend {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
	bcfg.Engine = engine
	bcfg.Path = path
	bcfg.BatchInterval = 10 * time.Millisecond
	bcfg.BatchLimit = 10000
	return backend.New(bcfg)
}

// TestEngineStoreLifecycle drives a real mvcc store through put/range/delete/
// compact/restore on both bbolt and Pebble, asserting identical behavior. This
// exercises the revision-encoded Key bucket (UnsafeSeqPut, ordered range scans),
// crash-resumable compaction (UnsafeDelete + ForceCommit), and the boot-time
// full-keyspace restore scan against the Pebble backend.
func TestEngineStoreLifecycle(t *testing.T) {
	for _, engine := range []backend.Engine{backend.EngineBBolt, backend.EnginePebble} {
		t.Run("engine="+string(engine), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			be := newEngineBackend(t, engine, path)
			s := NewStore(zaptest.NewLogger(t), be, &lease.FakeLessor{}, StoreConfig{})

			// Put a batch of keys, each creating a new revision.
			const n = 50
			for i := 0; i < n; i++ {
				k := []byte(fmt.Sprintf("key%03d", i))
				s.Put(k, []byte(fmt.Sprintf("val%03d", i)), lease.NoLease)
			}

			// Range the whole keyspace back.
			r, err := s.Range(t.Context(), []byte("key000"), []byte("key999"), RangeOptions{})
			require.NoError(t, err)
			require.Len(t, r.KVs, n)
			assert.Equal(t, []byte("key000"), r.KVs[0].Key)
			assert.Equal(t, []byte("val049"), r.KVs[n-1].Value)

			// Overwrite one key and delete another, creating more revisions.
			s.Put([]byte("key000"), []byte("updated"), lease.NoLease)
			dn, _ := s.DeleteRange([]byte("key001"), nil)
			require.Equal(t, int64(1), dn)

			r, err = s.Range(t.Context(), []byte("key000"), []byte("key999"), RangeOptions{})
			require.NoError(t, err)
			require.Len(t, r.KVs, n-1)
			assert.Equal(t, []byte("updated"), r.KVs[0].Value)

			// Compact away historical revisions, then confirm current view intact.
			rev := s.Rev()
			ch, err := s.Compact(traceutil.TODO(), rev-1)
			require.NoError(t, err)
			<-ch // wait for compaction to finish
			be.ForceCommit()

			r, err = s.Range(t.Context(), []byte("key000"), []byte("key999"), RangeOptions{})
			require.NoError(t, err)
			require.Len(t, r.KVs, n-1)

			require.NoError(t, s.Close())
			require.NoError(t, be.Close())

			// Reopen: NewStore rebuilds the treeIndex from the backend via a full
			// ordered scan (restore()). The data must survive.
			be2 := newEngineBackend(t, engine, path)
			s2 := NewStore(zaptest.NewLogger(t), be2, &lease.FakeLessor{}, StoreConfig{})
			defer func() {
				require.NoError(t, s2.Close())
				require.NoError(t, be2.Close())
			}()

			r, err = s2.Range(t.Context(), []byte("key000"), []byte("key999"), RangeOptions{})
			require.NoError(t, err)
			require.Len(t, r.KVs, n-1)
			assert.Equal(t, []byte("updated"), r.KVs[0].Value)
			assert.Equal(t, []byte("val049"), r.KVs[n-2].Value)
		})
	}
}
