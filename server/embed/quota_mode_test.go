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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

// startQuotaServer starts an embedded etcd tuned by tune and returns a client.
func startQuotaServer(t *testing.T, tune func(*Config)) *clientv3.Client {
	t.Helper()
	cfg := NewConfig()
	applyTestURLConfig(cfg, newConfigTestURLs())
	cfg.Dir = t.TempDir()
	tune(cfg)
	e, err := StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { e.Close() })
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("etcd failed to become ready")
	}
	c := v3client.New(e.Server)
	t.Cleanup(func() { c.Close() })
	return c
}

// TestQuotaModeSoftBehavior locks in the soft-mode contract: soft byte quotas
// throttle rather than reject, and the only hard stop is the real-disk backstop.
func TestQuotaModeSoftBehavior(t *testing.T) {
	t.Run("soft-byte-quota-does-not-reject", func(t *testing.T) {
		c := startQuotaServer(t, func(cfg *Config) {
			cfg.QuotaMode = "soft"
			cfg.QuotaBackendBytes = 8 << 20  // tiny soft physical limit
			cfg.QuotaLogicalBytes = 8 << 20  // tiny soft logical limit
			cfg.QuotaThrottleSoftStart = 0.5 // exercise the custom curve
		})
		// Even with tiny soft quotas, a write is throttled (maybe), never
		// hard-rejected with NOSPACE.
		_, err := c.Put(t.Context(), "k", "v")
		require.NoError(t, err)
	})

	t.Run("disk-backstop-rejects-when-reserve-exceeds-free", func(t *testing.T) {
		c := startQuotaServer(t, func(cfg *Config) {
			cfg.QuotaMode = "soft"
			cfg.QuotaBackendDiskReserveBytes = 1 << 62 // larger than any real disk
		})
		_, err := c.Put(t.Context(), "k", "v")
		require.Error(t, err, "disk backstop should reject when free disk < reserve")
	})

	t.Run("disk-backstop-disabled-serves", func(t *testing.T) {
		c := startQuotaServer(t, func(cfg *Config) {
			cfg.QuotaMode = "soft"
			cfg.QuotaBackendDiskReserveBytes = -1 // disabled
		})
		_, err := c.Put(t.Context(), "k", "v")
		require.NoError(t, err)
	})
}

// TestQuotaModeHardIsDefault confirms the default mode still serves normal
// writes (the backend-bytes hard ceiling is exercised by the integration suite).
func TestQuotaModeHardIsDefault(t *testing.T) {
	require.Equal(t, DefaultQuotaMode, NewConfig().QuotaMode)
	c := startQuotaServer(t, func(cfg *Config) {})
	_, err := c.Put(t.Context(), "k", "v")
	require.NoError(t, err)
}

// TestQuotaModeInvalidRejected verifies config validation of the new knobs.
func TestQuotaModeInvalidRejected(t *testing.T) {
	base := func() *Config {
		cfg := NewConfig()
		applyTestURLConfig(cfg, newConfigTestURLs())
		cfg.Dir = t.TempDir()
		return cfg
	}
	bad := base()
	bad.QuotaMode = "bogus"
	require.Error(t, bad.Validate())

	badStart := base()
	badStart.QuotaThrottleSoftStart = 1.5
	require.Error(t, badStart.Validate())

	badFrac := base()
	badFrac.QuotaThrottleMinFraction = -0.1
	require.Error(t, badFrac.Validate())

	good := base()
	good.QuotaMode = "soft"
	good.QuotaThrottleSoftStart = 0.7
	good.QuotaThrottleMinFraction = 0.05
	require.NoError(t, good.Validate())
}
