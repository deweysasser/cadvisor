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

package manager

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/cadvisor/lib/container"
	info "github.com/google/cadvisor/lib/model"
	"github.com/google/cadvisor/lib/watcher"
)

// trackingHandler is a mockHandler that records Cleanup, which containerData
// calls when its housekeeping goroutine exits after Stop.
type trackingHandler struct {
	mockHandler
	cleanedUp *atomic.Int32
}

func (h *trackingHandler) Cleanup() { h.cleanedUp.Add(1) }

// trackingHandlerFactory counts the handlers it creates. The delay widens the
// window between createContainer's existence check and its Store, which is
// where concurrent creates of the same container race.
type trackingHandlerFactory struct {
	delay     time.Duration
	created   atomic.Int32
	cleanedUp atomic.Int32
}

func (f *trackingHandlerFactory) String() string                 { return "tracking" }
func (f *trackingHandlerFactory) DebugInfo() map[string][]string { return map[string][]string{} }
func (f *trackingHandlerFactory) CanHandleAndAccept(string) (bool, bool, error) {
	return true, true, nil
}

func (f *trackingHandlerFactory) NewContainerHandler(name string, _ []string, _ bool) (container.ContainerHandler, error) {
	f.created.Add(1)
	time.Sleep(f.delay)
	return &trackingHandler{
		mockHandler: mockHandler{
			ref:  info.ContainerReference{Name: name, Aliases: []string{name + "-alias"}, Namespace: "tracking"},
			spec: info.ContainerSpec{HasCpu: true},
		},
		cleanedUp: &f.cleanedUp,
	}, nil
}

func newLifecycleTestManager(t *testing.T, delay time.Duration) (*manager, *trackingHandlerFactory) {
	t.Helper()
	container.ClearContainerHandlerFactories()
	t.Cleanup(container.ClearContainerHandlerFactories)
	f := &trackingHandlerFactory{delay: delay}
	container.RegisterContainerHandlerFactory(f, []watcher.ContainerWatchSource{watcher.Raw})
	m := newTestManager()
	m.maxHousekeepingInterval = time.Second
	return m, f
}

// waitForCleanups waits until want handlers have been cleaned up, or fails.
func waitForCleanups(t *testing.T, f *trackingHandlerFactory, want int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for f.cleanedUp.Load() < want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := f.cleanedUp.Load(); got != want {
		t.Fatalf("%d of %d started containers were cleaned up; the rest are leaked and still running housekeeping", got, want)
	}
}

// Concurrent creates of the same container (watch event, global housekeeping
// and on-demand lookup can all call createContainer) must produce exactly one
// running containerData. Before serialization, each racing create started its
// own; all but the last were overwritten in the map, so destroyContainer could
// never stop them and their housekeeping (and fs handlers) ran forever.
func TestConcurrentCreateStartsOneContainer(t *testing.T) {
	m, f := newLifecycleTestManager(t, 5*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.createContainer("/racy", watcher.Raw); err != nil {
				t.Errorf("createContainer: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := f.created.Load(); got != 1 {
		t.Errorf("created %d handlers for one container, want 1", got)
	}
	if err := m.destroyContainer("/racy"); err != nil {
		t.Fatalf("destroyContainer: %v", err)
	}
	waitForCleanups(t, f, f.created.Load())
}

// Interleaved create and destroy of the same container must never leave a
// running containerData or an orphaned alias entry behind (google/cadvisor#3924).
func TestConcurrentCreateDestroyLeavesNothingBehind(t *testing.T) {
	m, f := newLifecycleTestManager(t, time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := m.createContainer("/flappy", watcher.Raw); err != nil {
				t.Errorf("createContainer: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := m.destroyContainer("/flappy"); err != nil {
				t.Errorf("destroyContainer: %v", err)
			}
		}()
	}
	wg.Wait()
	if err := m.destroyContainer("/flappy"); err != nil {
		t.Fatalf("final destroyContainer: %v", err)
	}

	m.containers.Range(func(name namespacedContainerName, _ *containerData) bool {
		t.Errorf("container map still holds %+v after the final destroy", name)
		return true
	})
	waitForCleanups(t, f, f.created.Load())
}
