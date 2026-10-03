// Copyright 2026 Google Inc. All Rights Reserved.
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

//go:build linux

package common

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"

	"github.com/google/cadvisor/lib/fs"
)

// dirUsageFsInfo measures directories with fs.GetDirUsage, or fails with
// the error set for a directory in errs.
type dirUsageFsInfo struct {
	fs.FsInfo
	errs map[string]error
}

func (f *dirUsageFsInfo) GetDirUsage(dir string) (fs.UsageInfo, error) {
	if err := f.errs[dir]; err != nil {
		return fs.UsageInfo{}, err
	}
	return fs.GetDirUsage(dir)
}

// captureErrors redirects klog output for the duration of the test.
func captureErrors(t *testing.T) *bytes.Buffer {
	var buf bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&buf)
	t.Cleanup(func() {
		klog.SetOutput(os.Stderr)
		klog.LogToStderr(true)
	})
	return &buf
}

func TestUpdateRemovedDirs(t *testing.T) {
	rootfs, extraDir := t.TempDir(), t.TempDir()
	fh := NewFsHandler(time.Minute, rootfs, extraDir, &dirUsageFsInfo{}).(*realFsHandler)
	require.NoError(t, fh.update())

	require.NoError(t, os.Remove(rootfs))
	require.NoError(t, os.Remove(extraDir))
	err := fh.update()
	var usageErr *usageError
	require.ErrorAs(t, err, &usageErr)
	assert.True(t, usageErr.notExist())
}

func TestUpdateOtherError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	// Stat'ing a path below a regular file fails with ENOTDIR.
	fh := NewFsHandler(time.Minute, filepath.Join(file, "rootfs"), "", &dirUsageFsInfo{}).(*realFsHandler)
	err := fh.update()
	var usageErr *usageError
	require.ErrorAs(t, err, &usageErr)
	assert.False(t, usageErr.notExist())
}

func TestUpdateAndBackoff(t *testing.T) {
	notExist := fmt.Errorf("could not stat: %w", os.ErrNotExist)
	denied := fmt.Errorf("could not stat: %w", os.ErrPermission)

	tests := []struct {
		name string
		// Errors for the root and extra directories at each update.
		updates    [][2]error
		wantPeriod time.Duration
		// Whether each update logs at error level.
		wantErrorLog []bool
	}{
		{
			name:         "success",
			updates:      [][2]error{{nil, nil}},
			wantPeriod:   time.Minute,
			wantErrorLog: []bool{false},
		},
		{
			name:         "dirs removed",
			updates:      [][2]error{{notExist, notExist}},
			wantPeriod:   2 * time.Minute,
			wantErrorLog: []bool{false},
		},
		{
			name:         "extra dir removed",
			updates:      [][2]error{{nil, notExist}},
			wantPeriod:   2 * time.Minute,
			wantErrorLog: []bool{false},
		},
		{
			name:         "dirs still removed at next update",
			updates:      [][2]error{{notExist, notExist}, {notExist, notExist}, {notExist, notExist}},
			wantPeriod:   8 * time.Minute,
			wantErrorLog: []bool{false, true, true},
		},
		{
			name:         "other error",
			updates:      [][2]error{{denied, nil}},
			wantPeriod:   2 * time.Minute,
			wantErrorLog: []bool{true},
		},
		{
			name:         "one dir removed, other error",
			updates:      [][2]error{{notExist, denied}},
			wantPeriod:   2 * time.Minute,
			wantErrorLog: []bool{true},
		},
		{
			name:         "dirs removed after other error",
			updates:      [][2]error{{denied, nil}, {notExist, notExist}},
			wantPeriod:   4 * time.Minute,
			wantErrorLog: []bool{true, false},
		},
		{
			name:         "success resets",
			updates:      [][2]error{{notExist, notExist}, {nil, nil}, {notExist, notExist}},
			wantPeriod:   2 * time.Minute,
			wantErrorLog: []bool{false, false, false},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureErrors(t)
			rootfs, extraDir := t.TempDir(), t.TempDir()
			fsInfo := &dirUsageFsInfo{}
			fh := NewFsHandler(time.Minute, rootfs, extraDir, fsInfo).(*realFsHandler)
			for i, errs := range tc.updates {
				fsInfo.errs = map[string]error{rootfs: errs[0], extraDir: errs[1]}
				buf.Reset()
				fh.updateAndBackoff()
				klog.Flush()
				assert.Equal(t, tc.wantErrorLog[i], strings.Contains(buf.String(), "failed to collect filesystem stats"), "update %d logged: %q", i, buf.String())
			}
			assert.Equal(t, tc.wantPeriod, fh.period)
		})
	}
}
