//go:build linux

// Copyright 2026 Google LLC
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

package actornet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.ate.dev/substrate/internal/netns"
	"go.ate.dev/substrate/internal/nodepath"
)

// NamespacePool prepares gVisor networks once. Acquired networks never return.
type NamespacePool struct {
	mu        sync.Mutex
	ready     []*SandboxNetwork
	changed   chan struct{}
	paused    bool
	closed    bool
	err       error
	size      int
	port      uint16
	interval  time.Duration
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// NewNamespacePool starts one creator. interval bounds its creation rate.
// A creation failure stops replenishment and is returned to waiting callers.
func NewNamespacePool(size int, interval time.Duration, port uint16, paused bool) (*NamespacePool, error) {
	if size < 1 || interval <= 0 || port == 0 {
		return nil, fmt.Errorf("namespace pool requires positive size, interval, and egress port")
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &NamespacePool{
		size: size, interval: interval, port: port, paused: paused,
		changed: make(chan struct{}), cancel: cancel, done: make(chan struct{}),
	}
	go p.fill(ctx)
	return p, nil
}

func (p *NamespacePool) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// SetPaused prevents new creations. One in-flight creation may still finish.
// Existing ready entries can still be acquired while paused.
func (p *NamespacePool) SetPaused(paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = paused
	p.notifyLocked()
}

func (p *NamespacePool) fill(ctx context.Context) {
	defer close(p.done)
	var next time.Time
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		changed := p.changed
		if p.paused || len(p.ready) == p.size {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
			continue
		}
		delay := time.Until(next)
		if delay > 0 {
			p.mu.Unlock()
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-changed:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		// This decision precedes a concurrent pause; only this creator can run.
		p.mu.Unlock()
		// Finish an in-flight creation so shutdown can destroy its result.
		n, err := setupSandboxNetwork(context.WithoutCancel(ctx), SandboxNetworkConfig{
			ActorUID: "pool-" + uuid.NewString(), Veth: true, EgressPort: p.port,
		}, true)
		p.mu.Lock()
		if err != nil {
			p.err = fmt.Errorf("namespace pool creation failed: %w", err)
			p.notifyLocked()
			p.mu.Unlock()
			slog.Error("Namespace pool replenishment stopped", "err", err)
			return
		}
		if p.closed {
			p.mu.Unlock()
			p.closeErr = errors.Join(p.closeErr, CleanupSandboxNetwork(n))
			return
		}
		p.ready = append(p.ready, n)
		next = time.Now().Add(p.interval)
		p.notifyLocked()
		p.mu.Unlock()
	}
}

// Acquire waits until ctx expires or a prepared network is available.
// It binds the namespace to the actor's OCI path before transferring ownership.
func (p *NamespacePool) Acquire(ctx context.Context, actorUID string) (*SandboxNetwork, error) {
	if actorUID == "" {
		return nil, fmt.Errorf("actor UID is required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return nil, err
		}
		if p.closed {
			p.mu.Unlock()
			return nil, net.ErrClosed
		}
		if p.err != nil {
			err := p.err
			p.mu.Unlock()
			return nil, err
		}
		if len(p.ready) > 0 {
			n := p.ready[len(p.ready)-1]
			p.ready = p.ready[:len(p.ready)-1]
			// Keep Close from draining this entry during the ownership transfer.
			err := netns.BindNamed(n.NetNS, nodepath.ActorNetNSName(actorUID))
			if err == nil {
				err = netns.RemoveNamed(nodepath.ActorNetNSName(n.ActorUID))
				if err != nil {
					err = errors.Join(err, netns.RemoveNamed(nodepath.ActorNetNSName(actorUID)))
				}
			}
			if err == nil {
				n.ActorUID = actorUID
				err = ctx.Err()
			}
			p.notifyLocked()
			p.mu.Unlock()
			if err != nil {
				return nil, errors.Join(err, CleanupSandboxNetwork(n))
			}
			return n, nil
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// Close stops creation and destroys unused entries. Acquired networks remain
// owned by their sandbox sessions. Repeated and concurrent calls are safe.
func (p *NamespacePool) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.cancel()
		p.notifyLocked()
		p.mu.Unlock()
		<-p.done
		p.closeErr = errors.Join(p.closeErr, p.err)
		for _, n := range p.ready {
			p.closeErr = errors.Join(p.closeErr, CleanupSandboxNetwork(n))
		}
		p.ready = nil
	})
	return p.closeErr
}
