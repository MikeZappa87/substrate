# Single-use network namespace pool PoC

This gVisor-only experiment prepares actor networking before startup. It is
based on the single-namespace branch. Each gVisor worker prepares up to 32
unused networks by default and starts replenishment immediately.

Override the defaults through WorkerPool:

```yaml
spec:
  networkNamespacePool:
    size: 32
    creationIntervalMilliseconds: 100
    waitTimeoutSeconds: 5
    startPaused: false
```

Omitting `networkNamespacePool` uses the worker defaults shown above. All
fields are optional. Set `size: 0` to disable pooling, or `startPaused: true`
to wait for SIGUSR2 before creating networks.
The controller passes these settings to the existing `--netns-pool-*` worker
flags. The CRD rejects this configuration for microVM pools. Updating pool
settings replaces worker pods, so change them before starting load.

One creator builds namespaces, veth pairs, sysctls, and nftables rules. It
waits at least the configured interval after each creation completes. The
pool retains at most the configured number of ready entries. This bounds
creation concurrency to one; it does not bound the number of active actors.
IPv6 is disabled on existing and future interfaces before creating the pair.
`eth0` has the actor address; `atside` stays up and addressless until runsc
imports `eth0` and the worker activates the gateway.

## Run the scale suite on a fresh kind cluster

Use branch `mzappa/netns-pool-poc`, with the WorkerPool CRD and controller changes.
Run commands from the repository root. This procedure uses a new, single-node
kind cluster named `kind` and its local registry on port 5001. Select the
intended Docker context first. Do not run cluster creation against an existing
cluster you want to preserve.

Follow the [development prerequisites](../../README.md#quickstart-development)
for Docker, kind, kubectl, Go, and the installer dependencies. The benchmark
scripts also need `jq` and `envsubst`. gVisor works on Docker Desktop on a Mac;
this experiment does not require KVM or a separate Lima VM. Give Docker enough
CPU and memory for the chosen load. A laptop run establishes correctness and
local comparisons, rather than production capacity.

Create the cluster and install this checkout. Include the benchmark client's
identity in both API authorization and the ingress allowlist:

```console
hack/create-kind-cluster.sh
kubectl config use-context kind-kind
export ATE_INSTALL_KIND=true
export KO_DOCKER_REPO=localhost:5001
ATE_API_AUTHZ_BOOTSTRAP_OWNERS=system:serviceaccount:ate-system:ate-client,spiffe://cluster.local/ns/benchmarking/sa/default \
  go run ./cmd/ate-setup deploy ate-system \
  --credential-provider='{"name":"k8s.io"}' \
  --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/benchmarking/sa/default,spiffe://cluster.local/ns/ate-system/sa/atenet-client
```

If a corporate proxy intercepts TLS, export its public PEM CA bundle before
running the installer above:

```console
export DOCKER_BUILD_FLAGS="--secret id=host_ca,src=/absolute/path/to/host-ca.pem"
```

The Envoy Cargo build uses this secret for certificate verification. A host
trust-store entry alone does not configure the Docker build's trust store.
See [the Envoy build instructions](../dataplane/envoy/README.md).

On Docker Desktop, if the selected Buildx builder belongs to another Docker
context, add `--builder desktop-linux` to `DOCKER_BUILD_FLAGS` so pushes reach
the kind registry. Keep the CA secret flag when setting both.

Use the real gVisor data plane. `--fake-data-plane` does not create actor
namespaces and cannot test this change.

Check the node architecture, then set the image platforms to match. The
following values are for arm64; use `linux/amd64` for an amd64 node:

```console
kubectl get nodes -o custom-columns=NAME:.metadata.name,ARCH:.status.nodeInfo.architecture
export KO_DEFAULTPLATFORMS=linux/arm64
export LOCUST_IMAGE_PLATFORM=linux/arm64
```

Deploy the scale workloads and Locust. Start with one worker, then repeat with
the worker count and resource limits you intend to compare:

```console
benchmarking/deploy_locust.sh --deploy --sandbox-class gvisor --worker-count 1
```

This creates `benchmark-workloads/benchmark-worker` with pooling enabled by
default. Deploying Locust does not start load. The default `GluttonUser` workload uses the benchmark templates;
it does not require the counter demo.

The normal installation and benchmark deployment build the worker image from
this checkout with pooling enabled. No custom image, entrypoint, or WorkerPool
patch is needed. If you used the earlier image helper, redeploy the benchmark
workloads first to restore the ordinary worker image.

To override pool settings, patch the WorkerPool with the configuration shown
at the top of this guide. Changes replace worker pods. Confirm the generated
Deployment arguments match the override and wait for its rollout before
starting load:

```console
kubectl get deployment benchmark-worker -n benchmark-workloads \
  -o jsonpath='{.spec.template.spec.containers[0].args}'
kubectl rollout status deployment/benchmark-worker -n benchmark-workloads --timeout=300s
```

When no override is configured, the worker's defaults apply and no
`--netns-pool-*` arguments are required. Each worker has its own pool, so two
workers prepare up to 64 entries by default. The same defaults apply to other
gVisor WorkerPools, including the counter demo.

### Verify and control the pool

List the workers and select a pod. In the commands below, replace `POD_NAME`
with its actual name. For this single-node cluster the node container is
`kind-control-plane`:

```console
kubectl get pods -n benchmark-workloads -o wide
POD=POD_NAME
CONTAINER_ID=$(kubectl get pod "$POD" -n benchmark-workloads -o jsonpath='{.status.containerStatuses[0].containerID}')
WORKER_PID=$(docker exec kind-control-plane crictl inspect "${CONTAINER_ID#*://}" | jq -r '.info.pid')
docker exec kind-control-plane ls "/proc/$WORKER_PID/root/run/netns"
kubectl logs -n benchmark-workloads "$POD" --all-containers --tail=100
```

Wait until there are 32 `worker-actor:pool-*` entries, or your configured size.
Count ready entries rather than assuming that pod readiness means pool
readiness. Actor namespaces have actor UUIDs instead of `pool-` names. Repeat
inspection for each worker. After a rollout, resolve the new PID again.

Pause creation after filling the pool to test a burst without concurrent
replenishment. Resume it for sustained load:

```console
docker exec kind-control-plane kill -USR1 "$WORKER_PID"
docker exec kind-control-plane kill -USR2 "$WORKER_PID"
```

Check the worker logs for the pause/resume acknowledgement. A paused pool can
serve only its remaining entries; they are never returned after use.

### Start load and compare

```console
kubectl port-forward svc/locust -n benchmarking 8089:8089
```

Open <http://localhost:8089>, select `GluttonUser`, and set users, spawn rate,
and duration. Begin with a small run, such as one user at one user per second
for one minute. Download Locust's statistics and failures CSVs after each run.
See the [benchmarking guide](../../docs/guides/benchmarking.md) for workload
settings, monitoring, and larger runs.

For a baseline with pooling disabled, set the size to zero:

```console
kubectl patch workerpool benchmark-worker -n benchmark-workloads --type=merge \
  -p '{"spec":{"networkNamespacePool":{"size":0}}}'
```

Remove the override with `networkNamespacePool: null` to restore the defaults.
Use the same worker image for both comparisons. Stop load and remove the
previous run's actors before rolling workers. Keep node resources, worker
count, actor resources, workload settings, tracing, and test duration the same. Use multiple runs and
record startup/restore latency, throughput, failures, and CPU. Locust request
latency includes more than namespace setup; collect the same kernel profiles
in both runs if measuring lock contention.

Report warm bursts and sustained load separately. Precreation must finish
outside the timed window for a warm-burst comparison. At `100ms`, each worker
creates fewer than ten entries per second because setup time is additional.
Sustained demand above replenishment eventually exhausts the pool. The default
five-second wait may then fail actor startup with `ResourceExhausted`; record
those failures rather than treating them as a performance improvement. Actor
placement can exhaust one worker while another still has ready entries.

When finished, stop load before removing the benchmark deployment:

```console
benchmarking/deploy_locust.sh --delete
```

## Control replenishment

On the node, find the worker process PID using the existing namespace
inspection procedure, then signal that PID:

```console
kill -USR2 <worker-pid>
kill -USR1 <worker-pid>
```

`SIGUSR2` resumes replenishment. `SIGUSR1` pauses it. The worker logs each
change. The PID 1 supervisor also forwards these signals to the worker.
One creation already in progress may finish after a pause. Pausing retains
ready entries and allows actors to consume them; it only stops new creation.
Workers create immediately at boot unless `startPaused: true` is configured.

Inspect the prepared entries from the node:

```console
ls /proc/<worker-pid>/root/run/netns/worker-actor:pool-*
```

Resume and allow the pool to fill before scheduling a burst. Leave it paused
during a period when creation would compete with important work. There is
no automatic load detector or placement integration in this PoC.

## Checkout and exhaustion

An actor takes one prepared entry exclusively. Checkout creates an exclusive
bind mount at `/run/netns/worker-actor:<actor-uid>` and removes the temporary
pool name. Runsc continues to use the existing actor OCI path. DNS, egress
listeners, and identity remain actor-specific and are attached after checkout.
The worker clears a leftover actor name at its serialized lifecycle boundary.
The pool itself never replaces an existing actor name.

If the pool is empty, startup waits up to `--netns-pool-wait`, or the RPC's
earlier cancellation. Expiration returns `ResourceExhausted` and releases the
worker's actor reservation. There is no synchronous creation fallback. The
wait budget covers network checkout and serving setup, rather than the whole
actor boot. Kernel mount operations are not interruptible by the context.

An acquired entry never returns to the pool. Failed startup and actor teardown
destroy it through the existing sandbox session cleanup. New creations have
new names and namespaces, so no actor inherits another actor's network state.

## Shutdown and limits

Graceful shutdown stops replenishment, wakes waiting checkouts, joins the
creator, and destroys unused entries. An in-flight creation finishes and is
then destroyed. Acquired entries remain owned by their actor sessions and are
removed by the normal actor cleanup. Pool close returns creation and cleanup
errors; the worker logs them.

A creation failure stops replenishment and makes subsequent checkouts fail.
Restart the worker after resolving the cause. This PoC does not add automatic
retry, crash recovery for unused pool entries, metrics, memory budgeting, or
pool-aware placement. SIGKILL cannot execute graceful cleanup. Ready entries
consume kernel resources. Precreation shifts setup work ahead of demand; it
does not eliminate creation or teardown locking.

The microVM worker does not use the pool. Run network e2e and restore tests
before broader deployment. No production latency or density benefit has been
established by this change.

## Local tests

On a disposable Linux host with root privileges and the repository's build
dependencies:

```console
sudo go test -race ./internal/netns ./internal/ateworker/actornet ./cmd/ateworker-gvisor -count=3 -shuffle=on
```

The pool tests exercise exclusive checkout, pause/resume, creation pacing,
exhaustion, IPv6 configuration, gateway activation, actor-name collision,
leftover-name recovery, and cleanup. Existing actor networking tests cover
packet filtering and ingress/egress separately.

Validated locally on Linux arm64 with unmodified `runsc release-20261026.0`:
all tests in `internal/netns`, `internal/ateworker/actornet`, and
`cmd/ateworker-gvisor` passed three shuffled runs. A separate gVisor smoke
probe passed TCP ingress and egress after acquiring a pooled namespace and
closing the pool. Scoped vet and lint passed. The race detector has not been
run for this PoC.

The networking e2e suite passed against the pooled counter and egress workers.
The WebSocket ingress fixture was rerun with `E2E_GVISOR_WORKER_IMAGE` pointing
to the pool-enabled image, since it creates its own WorkerPool. The actor
lifecycle e2e passed pause/resume, suspend/resume, revert/resume, and deletion.
The microVM-only clock test was skipped in the gVisor run.

Those e2e results used the earlier image-entrypoint configuration. Current
gVisor workers enable pooling by default, including workers created by e2e
fixtures. Use worker images built from this checkout for both installed demos
and rendered fixtures. `E2E_GVISOR_WORKER_IMAGE` overrides fixture images only.

```console
GOMAXPROCS=4 hack/run-e2e-kind.sh ./internal/e2e/suites/networking -p 1 -parallel 1 -count=1 -shuffle=on
GOMAXPROCS=4 hack/run-e2e-kind.sh ./internal/e2e/suites/demo -run '^TestActorLifecycle$' -p 1 -parallel 1 -count=1 -shuffle=on
```
