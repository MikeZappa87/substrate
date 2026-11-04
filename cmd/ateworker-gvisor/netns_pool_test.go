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

package main

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/roottest"
	"google.golang.org/grpc/codes"
)

func TestHostActorWithNamespacePool(t *testing.T) {
	roottest.Require(t, "creates actor network namespaces")
	for _, tc := range []struct {
		name   string
		paused bool
	}{
		{"leftover", false}, {"exhausted", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const uid = "pool-worker-test"
			const port = 15001
			p, err := actornet.NewNamespacePool(1, time.Millisecond, port, tc.paused)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := p.Close(); err != nil {
					t.Error(err)
				}
			})
			proxy, err := egress.New(func(net.Conn) (string, error) { return "", errors.New("unexpected egress") })
			if err != nil {
				t.Fatal(err)
			}
			s := &AteWorkerService{
				actors: map[string]*hostedActor{}, maxActors: 1,
				tunnel: &atunnel.Tunnel{EgressPort: port, Egress: proxy}, namespacePool: p, namespacePoolWait: 5 * time.Second,
			}
			t.Cleanup(func() {
				if err := s.unhostActor(context.Background(), uid); err != nil {
					t.Error(err)
				}
			})
			if tc.paused {
				s.namespacePoolWait = 20 * time.Millisecond
			} else {
				old, err := actornet.SetupSandboxNetwork(context.Background(), actornet.SandboxNetworkConfig{ActorUID: uid, Veth: true, EgressPort: port})
				if err != nil {
					t.Fatal(err)
				}
				// Leave the mount, as if the previous worker lost its in-memory session.
				if err := old.NetNS.Close(); err != nil {
					t.Fatal(err)
				}
			}
			hosted, err := s.hostActor(context.Background(), resources.ActorAttribution{UID: uid}, nil, 0)
			if tc.paused {
				if apierror.Code(err) != codes.ResourceExhausted {
					t.Fatalf("exhaustion: %v", err)
				}
				if len(s.actors) != 0 {
					t.Fatal("failed startup retained actor capacity")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if hosted.network == nil {
				t.Fatal("missing pooled session")
			}
			if err := s.unhostActor(context.Background(), uid); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNamespacePoolSignalsResume(t *testing.T) {
	roottest.Require(t, "creates a prepared network after SIGUSR2")
	p, err := actornet.NewNamespacePool(1, time.Second, 15001, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stop := watchNamespacePoolSignals(ctx, p)
	t.Cleanup(stop)
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR2); err != nil {
		t.Fatal(err)
	}
	n, err := p.Acquire(ctx, "signal-resume-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := actornet.CleanupSandboxNetwork(n); err != nil {
			t.Error(err)
		}
	})
}

func TestGracefulShutdownClosesNamespacePool(t *testing.T) {
	p, err := actornet.NewNamespacePool(1, time.Second, 15001, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	s := &AteWorkerService{inFlight: actorlock.NewInFlight(), namespacePool: p}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	s.gracefulShutdown(ctx)
	if _, err := p.Acquire(ctx, "after-shutdown"); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("checkout after worker shutdown: %v", err)
	}
}
