// Copyright 2015 Google Inc. All Rights Reserved.
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

// Handler for Docker containers.
package common

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/cadvisor/lib/fs"

	"k8s.io/klog/v2"
)

type FsHandler interface {
	Start()
	Usage() FsUsage
	Stop()
}

type FsUsage struct {
	BaseUsageBytes  uint64
	TotalUsageBytes uint64
	InodeUsage      uint64
}

type realFsHandler struct {
	sync.RWMutex
	lastUpdate time.Time
	usage      FsUsage
	period     time.Duration
	minPeriod  time.Duration
	rootfs     string
	extraDir   string
	fsInfo     fs.FsInfo
	// Whether the last update failed only because the directories no
	// longer exist. Only accessed by the trackUsage goroutine.
	dirsVanished bool
	// Tells the container to stop.
	stopChan chan struct{}
}

const (
	maxBackoffFactor = 20
)

const DefaultPeriod = time.Minute

var _ FsHandler = &realFsHandler{}

func NewFsHandler(period time.Duration, rootfs, extraDir string, fsInfo fs.FsInfo) FsHandler {
	return &realFsHandler{
		lastUpdate: time.Time{},
		usage:      FsUsage{},
		period:     period,
		minPeriod:  period,
		rootfs:     rootfs,
		extraDir:   extraDir,
		fsInfo:     fsInfo,
		stopChan:   make(chan struct{}, 1),
	}
}

func (fh *realFsHandler) update() error {
	var (
		rootUsage, extraUsage fs.UsageInfo
		rootErr, extraErr     error
	)
	// TODO(vishh): Add support for external mounts.
	if fh.rootfs != "" {
		rootUsage, rootErr = fh.fsInfo.GetDirUsage(fh.rootfs)
	}

	if fh.extraDir != "" {
		extraUsage, extraErr = fh.fsInfo.GetDirUsage(fh.extraDir)
	}

	// Wait to handle errors until after all operartions are run.
	// An error in one will not cause an early return, skipping others
	fh.Lock()
	defer fh.Unlock()
	fh.lastUpdate = time.Now()
	if fh.rootfs != "" && rootErr == nil {
		fh.usage.InodeUsage = rootUsage.Inodes
		fh.usage.BaseUsageBytes = rootUsage.Bytes
		fh.usage.TotalUsageBytes = rootUsage.Bytes
	}
	if fh.extraDir != "" && extraErr == nil {
		if fh.rootfs != "" {
			fh.usage.TotalUsageBytes += extraUsage.Bytes
		} else {
			// rootfs is empty, totalUsageBytes use extra usage bytes
			fh.usage.TotalUsageBytes = extraUsage.Bytes
		}
	}

	// Combine errors into a single error to return
	if rootErr != nil || extraErr != nil {
		return &usageError{rootErr: rootErr, extraErr: extraErr}
	}
	return nil
}

// usageError combines the errors from measuring the root and extra directories.
type usageError struct {
	rootErr, extraErr error
}

func (e *usageError) Error() string {
	return fmt.Sprintf("rootDiskErr: %v, extraDiskErr: %v", e.rootErr, e.extraErr)
}

// notExist reports whether every failure was a directory that does not exist.
func (e *usageError) notExist() bool {
	return (e.rootErr == nil || errors.Is(e.rootErr, os.ErrNotExist)) &&
		(e.extraErr == nil || errors.Is(e.extraErr, os.ErrNotExist))
}

// updateAndBackoff runs one update and sets the period until the next one.
func (fh *realFsHandler) updateAndBackoff() {
	err := fh.update()
	if err == nil {
		fh.dirsVanished = false
		fh.period = fh.minPeriod
		return
	}
	var usageErr *usageError
	vanished := errors.As(err, &usageErr) && usageErr.notExist()
	if vanished && !fh.dirsVanished {
		// The runtime can remove a container's directories before the
		// handler is stopped. That is expected once; if it is still the
		// case at the next update, the handler was never stopped.
		klog.V(4).Infof("container directories no longer exist, skipping filesystem stats - %v", err)
	} else {
		klog.Errorf("failed to collect filesystem stats - %v", err)
	}
	fh.dirsVanished = vanished
	fh.period = fh.period * 2
	if fh.period > maxBackoffFactor*fh.minPeriod {
		fh.period = maxBackoffFactor * fh.minPeriod
	}
}

func (fh *realFsHandler) trackUsage() {
	longOp := time.Second
	for {
		start := time.Now()
		fh.updateAndBackoff()
		duration := time.Since(start)
		if duration > longOp {
			// adapt longOp time so that message doesn't continue to print
			// if the long duration is persistent either because of slow
			// disk or lots of containers.
			longOp = longOp + time.Second
			klog.V(2).Infof("fs: disk usage and inodes count on following dirs took %v: %v; will not log again for this container unless duration exceeds %v", duration, []string{fh.rootfs, fh.extraDir}, longOp)
		}
		select {
		case <-fh.stopChan:
			return
		case <-time.After(fh.period):
		}
	}
}

func (fh *realFsHandler) Start() {
	go fh.trackUsage()
}

func (fh *realFsHandler) Stop() {
	close(fh.stopChan)
}

func (fh *realFsHandler) Usage() FsUsage {
	fh.RLock()
	defer fh.RUnlock()
	return fh.usage
}
