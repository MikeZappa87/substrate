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
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vishvananda/netlink"
	"go.ate.dev/substrate/internal/netns"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/roottest"
	"golang.org/x/sys/unix"
)

func waitPoolReady(t *testing.T, p *NamespacePool, count int) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		p.mu.Lock()
		names := make([]string, 0, len(p.ready))
		for _, n := range p.ready {
			names = append(names, nodepath.ActorNetNSName(n.ActorUID))
		}
		err := p.err
		changed := p.changed
		p.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if len(names) == count {
			return names
		}
		select {
		case <-ctx.Done():
			t.Fatal("pool did not fill")
		case <-changed:
		}
	}
}

func TestNamespacePoolPausedAndShutdown(t *testing.T) {
	p, err := NewNamespacePool(2, time.Second, testEgressPort, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(ctx, "paused"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty paused pool: %v", err)
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			_, err := p.Acquire(context.Background(), "waiting")
			if !errors.Is(err, net.ErrClosed) {
				t.Errorf("shutdown waiter: %v", err)
			}
		})
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestNamespacePoolSingleUse(t *testing.T) {
	roottest.Require(t, "creates pooled namespaces and nftables rules")
	p, err := NewNamespacePool(4, time.Millisecond, testEgressPort, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	names := waitPoolReady(t, p, 4)
	p.SetPaused(true)
	networks := make([]*SandboxNetwork, 2)
	var wg sync.WaitGroup
	for i := range networks {
		wg.Go(func() {
			n, err := SetupSandboxNetwork(context.Background(), SandboxNetworkConfig{
				ActorUID: fmt.Sprintf("pool-actor-%d", i), Veth: true, EgressPort: testEgressPort, Pool: p,
			})
			if err != nil {
				t.Error(err)
				return
			}
			networks[i] = n
		})
	}
	wg.Wait()
	var inodes []uint64
	for _, n := range networks {
		if n == nil {
			t.Fatal("checkout failed")
		}
		t.Cleanup(func() {
			if err := CleanupSandboxNetwork(n); err != nil {
				t.Error(err)
			}
		})
		var st unix.Stat_t
		if err := unix.Fstat(int(n.NetNS), &st); err != nil {
			t.Fatal(err)
		}
		inodes = append(inodes, st.Ino)
		path := nodepath.ActorNetNSPath(n.ActorUID)
		var bound unix.Stat_t
		if err := unix.Stat(path, &bound); err != nil {
			t.Fatal(err)
		}
		if bound.Ino != st.Ino {
			t.Fatal("actor path names a different namespace")
		}
		if err := netns.Do(context.Background(), n.NetNS, func(context.Context) error {
			for _, name := range []string{"all", "default", "eth0", "atside"} {
				b, err := os.ReadFile("/proc/sys/net/ipv6/conf/" + name + "/disable_ipv6")
				if err != nil {
					return err
				}
				if strings.TrimSpace(string(b)) != "1" {
					return fmt.Errorf("IPv6 enabled on %s", name)
				}
			}
			for _, name := range []string{"eth0", "atside"} {
				l, err := netlink.LinkByName(name)
				if err != nil {
					return err
				}
				addrs, err := netlink.AddrList(l, netlink.FAMILY_V6)
				if err != nil {
					return err
				}
				if len(addrs) != 0 {
					return fmt.Errorf("%s has IPv6 addresses: %v", name, addrs)
				}
			}
			// Simulate runsc importing eth0 before gateway activation.
			l, err := netlink.LinkByName("eth0")
			if err != nil {
				return err
			}
			return netlink.AddrDel(l, ActorVethAddr)
		}); err != nil {
			t.Fatal(err)
		}
		if err := n.ActivateGateway(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if inodes[0] == inodes[1] {
		t.Fatal("actors shared a namespace")
	}
	// A collision must preserve the active actor's namespace.
	if _, err := p.Acquire(context.Background(), networks[0].ActorUID); !errors.Is(err, os.ErrExist) {
		t.Fatalf("duplicate actor name: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, err := os.Stat("/run/netns/" + name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pool name survived shutdown: %s: %v", name, err)
		}
	}
	for _, n := range networks {
		if _, err := os.Stat(nodepath.ActorNetNSPath(n.ActorUID)); err != nil {
			t.Fatalf("shutdown removed an acquired namespace: %v", err)
		}
	}
}

func TestNamespacePoolCreationInterval(t *testing.T) {
	roottest.Require(t, "creates a prepared network")
	p, err := NewNamespacePool(2, time.Hour, testEgressPort, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	p.SetPaused(false)
	waitPoolReady(t, p, 1)
	n, err := p.Acquire(context.Background(), "pool-rate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := CleanupSandboxNetwork(n); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(ctx, "pool-rate-exhausted"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("creation interval bypassed: %v", err)
	}
}
