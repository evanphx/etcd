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

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package storage

import "syscall"

// availableDiskBytes returns the bytes available to an unprivileged process on
// the filesystem that contains path, and true. It returns false if the free
// space cannot be determined (the caller then skips the disk backstop).
func availableDiskBytes(path string) (int64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, false
	}
	// Bavail is blocks available to non-root; Bsize is the block size. Cast
	// through int64 because field widths differ across platforms.
	return int64(st.Bavail) * int64(st.Bsize), true
}
