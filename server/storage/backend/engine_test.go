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

package backend_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// TestNewEngineDispatch verifies that the empty engine and the explicit
// "bbolt" engine both construct a functional bbolt backend with identical
// behavior, so selecting the default never diverges from the historical path.
func TestNewEngineDispatch(t *testing.T) {
	for _, engine := range []backend.Engine{"", backend.EngineBBolt} {
		t.Run("engine="+string(engine), func(t *testing.T) {
			bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(t))
			bcfg.Engine = engine
			bcfg.Path = filepath.Join(t.TempDir(), "be.db")

			be := backend.New(bcfg)
			defer be.Close()

			// Exercise a basic write/read round-trip through the dispatched backend.
			tx := be.BatchTx()
			tx.Lock()
			schema.UnsafeCreateMetaBucket(tx)
			tx.UnsafePut(schema.Meta, []byte("k"), []byte("v"))
			tx.Unlock()
			be.ForceCommit()

			rtx := be.ReadTx()
			rtx.RLock()
			ks, vs := rtx.UnsafeRange(schema.Meta, []byte("k"), nil, 0)
			rtx.RUnlock()

			require.Len(t, ks, 1)
			assert.Equal(t, []byte("v"), vs[0])
		})
	}
}
