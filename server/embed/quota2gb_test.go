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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

// TestQuota2GBInflow drives uncompacted revisions into a full embedded etcd
// until the 2 GiB backend quota trips the NOSPACE alarm, on each engine. It
// reproduces the bbolt "DB blows up to the quota and goes read-only" failure and
// reports how pebble behaves under the identical inflow.
//
// It writes GiB of data and takes minutes; run explicitly:
//
//	go test ./embed/ -run TestQuota2GBInflow -v -timeout 30m
func TestQuota2GBInflow(t *testing.T) {
	const (
		quota     = 2 << 30 // 2 GiB
		valueSize = 4 << 20 // 4 MiB per revision
		maxReq    = 16 << 20
		writeCap  = 1500 // safety cap (~6 GiB) if an engine never trips
	)

	if testing.Short() {
		t.Skip("writes ~4 GiB across both engines; skipped in -short")
	}

	for _, engine := range []string{"bbolt", "pebble"} {
		t.Run(engine, func(t *testing.T) {
			cfg := NewConfig()
			applyTestURLConfig(cfg, newConfigTestURLs())
			cfg.Dir = t.TempDir()
			cfg.BackendEngine = engine
			cfg.QuotaBackendBytes = quota
			cfg.MaxRequestBytes = maxReq
			cfg.LogLevel = "error"

			e, err := StartEtcd(cfg)
			require.NoError(t, err)
			defer e.Close()
			select {
			case <-e.Server.ReadyNotify():
			case <-time.After(30 * time.Second):
				t.Fatal("etcd not ready")
			}

			cli := v3client.New(e.Server)
			defer cli.Close()
			ctx := t.Context()

			// Overwrite ONE key repeatedly: each Put creates a new revision, and
			// with auto-compaction off (the default) they accumulate in the
			// backend until the quota is exceeded.
			rng := rand.New(rand.NewSource(1))
			val := make([]byte, valueSize)

			var tripWrite int
			var tripErr error
			var tripSize int64
			for i := 1; i <= writeCap; i++ {
				rng.Read(val)
				_, perr := cli.Put(ctx, "k", string(val))
				if perr != nil {
					tripWrite = i
					tripErr = perr
					tripSize = e.Server.Backend().Size()
					break
				}
				if i%25 == 0 {
					t.Logf("[%s] writes=%d size=%s sizeInUse=%s",
						engine, i, gib(e.Server.Backend().Size()), gib(e.Server.Backend().SizeInUse()))
				}
			}

			alarms, _ := cli.AlarmList(ctx)

			t.Logf("\n===== engine=%s =====", engine)
			if tripErr != nil {
				t.Logf("TRIPPED at write #%d (%.2f GiB written)", tripWrite, float64(tripWrite)*valueSize/(1<<30))
				t.Logf("  error:      %v", tripErr)
				t.Logf("  Size():     %s", gib(tripSize))
				t.Logf("  SizeInUse():%s", gib(e.Server.Backend().SizeInUse()))
			} else {
				t.Logf("did NOT trip within %d writes (%.2f GiB written); Size()=%s",
					writeCap, float64(writeCap)*valueSize/(1<<30), gib(e.Server.Backend().Size()))
			}
			t.Logf("  alarms:     %v", alarms.Alarms)

			// Confirm the failure mode: the alarm is NOSPACE and writes are now
			// rejected (cluster is read-only).
			if tripErr != nil {
				require.Contains(t, tripErr.Error(), "database space exceeded")
				_, perr := cli.Put(ctx, "after", "blocked")
				require.Error(t, perr, "writes must be rejected once NOSPACE is active")
			}
		})
	}
}

func gib(n int64) string { return fmt.Sprintf("%.3f GiB", float64(n)/(1<<30)) }
