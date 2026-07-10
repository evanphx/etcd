// Copyright 2021 The etcd Authors
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

package betesting

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

// TestBackendEngineEnv selects the storage engine used by test backends. Setting
// it to "pebble" runs a whole suite against the Pebble engine; unset (or
// "bbolt") keeps the default. This lets the real-backend consumer suites
// (mvcc, lease, backend, ...) run under either engine without duplicating tests.
const TestBackendEngineEnv = "ETCD_TEST_BACKEND_ENGINE"

// MaybeSetEngineFromEnv sets bcfg.Engine from TestBackendEngineEnv when the
// engine is otherwise unset. Callers that build a BackendConfig directly (rather
// than via the helpers below) should call this to stay engine-parameterizable.
func MaybeSetEngineFromEnv(bcfg *backend.BackendConfig) {
	if bcfg.Engine == "" {
		if e := os.Getenv(TestBackendEngineEnv); e != "" {
			bcfg.Engine = backend.Engine(e)
		}
	}
}

// CurrentTestEngine returns the engine selected by TestBackendEngineEnv (empty
// means the default, bbolt). Tests that exercise a bbolt-only property can skip
// when this is EnginePebble.
func CurrentTestEngine() backend.Engine {
	return backend.Engine(os.Getenv(TestBackendEngineEnv))
}

// EngineOptFromEnv is a BackendConfigOption that applies TestBackendEngineEnv,
// for reopening a backend (e.g. via NewDefaultBackend) with the same engine a
// test's initial backend used.
func EngineOptFromEnv() backend.BackendConfigOption {
	return func(bcfg *backend.BackendConfig) { MaybeSetEngineFromEnv(bcfg) }
}

func NewTmpBackendFromCfg(tb testing.TB, bcfg backend.BackendConfig) (backend.Backend, string) {
	dir, err := os.MkdirTemp(tb.TempDir(), "etcd_backend_test")
	if err != nil {
		panic(err)
	}
	tmpPath := filepath.Join(dir, "database")
	bcfg.Path = tmpPath
	bcfg.Logger = zaptest.NewLogger(tb)
	MaybeSetEngineFromEnv(&bcfg)
	return backend.New(bcfg), tmpPath
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(tb testing.TB, batchInterval time.Duration, batchLimit int) (backend.Backend, string) {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(tb))
	bcfg.BatchInterval, bcfg.BatchLimit = batchInterval, batchLimit
	return NewTmpBackendFromCfg(tb, bcfg)
}

func NewDefaultTmpBackend(tb testing.TB) (backend.Backend, string) {
	return NewTmpBackendFromCfg(tb, backend.DefaultBackendConfig(zaptest.NewLogger(tb)))
}

func Close(tb testing.TB, b backend.Backend) {
	assert.NoError(tb, b.Close())
}
