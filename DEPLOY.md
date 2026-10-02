# Kubernetes deployment guide (autoconfig routing stack + test model + ModelRoute)

English | [简体中文](DEPLOY.zh-CN.md)

This guide deploys the whole "ModelRoute-driven LLM routing stack" on a clean Kubernetes cluster and brings up
one test model end to end (**qwen** as the example; opt and other models are added the same way).
Component images are published to Docker Hub (`4pdosc/*`) when each repository is tagged, and the Helm charts
are published from [modelsphere/helm-charts](https://github.com/modelsphere/helm-charts); this guide only runs
`helm install` and `kubectl apply`.

## 0. Architecture and dependency order

```
                    ┌── autoconfig (operator) ──┐  watches ModelRoute + EndpointSlice
   ModelRoute (CR) ▶│  renders 3 ConfigMaps:    │  →  openresty-conf / cart-<m>-config / monitor-conf
                    └──────────┬────────────────┘
        ┌──────────────┬───────┴───────┬──────────────┐
   openresty        cart-<model>      monitor        (each mounts its ConfigMap; the reload sidecar hot-reloads)
   (entry, 8080)    (cache-affinity   (dashboard)
                     routing)
        │                │
        └── 3 peer tiers: cart (preferred) → backend pod IPs → backend-svc VIP fallback ──▶ model backend (vllm/sglang)
```

**Deployment order (there are dependencies; do not reorder)**:
1. Prerequisites: Helm repository + namespaces
2. **autoconfig** (first — it brings the ModelRoute CRD and the controller; everything after consumes the
   ConfigMaps it produces)
3. Test model backend (qwen)
4. **cart** (one per model; with `waitForWorkers` it waits in Init until autoconfig writes the workers)
5. **openresty** (the entry point)
6. **monitor** (dashboard + MySQL)
7. **ModelRoute CR** (qwen) → autoconfig fills the three ConfigMaps → cart becomes ready, openresty gets the
   route, monitor starts monitoring it
8. Verify

Images come from Docker Hub (`4pdosc/`) by default; charts come from the Helm repository
`https://modelsphere.github.io/helm-charts`. If the cluster has no internet access, mirror the images into your
own registry and override each chart's `image` values.

monitor has no public chart or image yet: in step 6, substitute your own monitor chart, or skip it (see step 6).

---

## 1. Prerequisites

```bash
# 1.1 Add the Helm repository (public, read-only, no credentials)
helm repo add modelsphere https://modelsphere.github.io/helm-charts
helm repo update modelsphere

# 1.2 Check that the charts are visible (without --version, the latest is used)
helm search repo modelsphere/autoconfig
helm search repo modelsphere/openresty
helm search repo modelsphere/cart

# 1.3 Namespaces (the qwen namespace comes with the backend sample; no need to create it)
kubectl create ns llm-route   2>/dev/null || true
kubectl create ns monitoring  2>/dev/null || true
```

> **No `helm install`/`upgrade` in this guide passes `--version`** — Helm takes the latest version in the
> repository, so a new release is picked up the next time you run the command without editing this guide. To
> pin or roll back to an earlier version, add `--version <x.y.z>` (available versions:
> `helm search repo modelsphere/<chart> --versions`).

---

## 2. autoconfig (operator + CRD)

The chart ships `crds/` (the ModelRoute CRD), the controller Deployment (2 replicas with leader election) and
RBAC.

```bash
helm -n llm-route install autoconfig modelsphere/autoconfig \
  --set fullnameOverride=autoconfig-controller

# Check: CRD installed + controller running
kubectl get crd modelroutes.routing.modelsphere.dev
kubectl -n llm-route rollout status deploy/autoconfig-controller
```

### 2.1 Health probes and metrics (since 0.3.32)

The container exposes two ports, **for Kubernetes and Prometheus only**; they carry no application traffic:

| Port | Path | Purpose |
|---|---|---|
| 8081 | `/healthz` `/readyz` | liveness / readiness probes |
| 8080 | `/metrics` | controller-runtime's built-in metrics (scraped by Prometheus) |

```bash
# Check the probes
kubectl -n llm-route get deploy autoconfig-controller \
  -o jsonpath='{.spec.template.spec.containers[0].livenessProbe.httpGet}{"\n"}'

# Check that Prometheus picks up the ServiceMonitor (note the label release=kube-prometheus-stack)
kubectl -n llm-route get servicemonitor autoconfig-controller -o jsonpath='{.metadata.labels}{"\n"}'
# Is it scraped? Prometheus should have a target with job=autoconfig-controller-metrics
```

**⚠️ The two replicas are master-standby, but both are in the Service.** A Kubernetes Service selects pods by
label only and **knows nothing about the leader**; autoconfig has no hagate sidecar either (it takes no traffic,
so there is no need to take the standby out of the endpoints). The metrics Service is therefore **headless
(`clusterIP: None`)**, so Prometheus scrapes each pod and the metrics carry a `pod` label.
**A plain curl to the Service lands on a random replica, possibly the standby, whose queue is always empty and
whose reconcile count is near zero — do not mistake that for the controller doing nothing.**

To identify the leader in Prometheus, use `leader_election_master_status` (leader=1 / standby=0, built into
controller-runtime):

```promql
# The leader's queue backlog only
workqueue_depth{job=~"autoconfig.*"} and on(pod) (leader_election_master_status == 1)
```

**To diagnose a stuck reconcile (such as a wedged cart ConfigMap), look at this one** — the health probes cannot
catch it; they only prove the process answers HTTP:

```promql
# How long the current reconcile has been running; rising without returning to zero = stuck
workqueue_unfinished_work_seconds{job=~"autoconfig.*"} and on(pod) (leader_election_master_status == 1)
```

Other useful ones: `workqueue_depth` (ModelRoutes waiting in the queue; with a handful of objects and the 10s
resync, 0–1 in steady state), `workqueue_retries_total` (reconcile retries; jumps on DiscoverError or an empty
base config), `controller_runtime_reconcile_errors_total`, `rest_client_requests_total` (429s = throttled by the
API server), `go_goroutines` (leaks).

The leader is a Kubernetes Lease; to see the current holder:

```bash
kubectl -n llm-route get lease autoconfig-controller.routing.modelsphere.dev -o jsonpath='{.spec.holderIdentity}{"\n"}'
```

To turn metrics off (if you do not want them scraped): `--set metrics.enabled=false` or
`--set metrics.serviceMonitor.enabled=false`.

---

## 3. Test model backend (qwen as the example)

The samples are in `config/samples/` and include the Namespace, Deployment and Service.

- `qwen-backend.yaml`: sglang `Qwen3.5-4B` (namespace `qwen`, `qwen-svc` NodePort 30055; **`--enable-metrics`**,
  without which the monitor cannot collect KV/running/waiting; `terminationGracePeriodSeconds: 3600` to drain long
  requests). The model is read from a `hostPath`; set `nodeName` (commented out in the sample) to the node that
  holds it.

```bash
kubectl apply -f config/samples/qwen-backend.yaml
kubectl -n qwen rollout status deploy/qwen
# sglang /metrics returns 200 only with --enable-metrics (404 by default):
kubectl -n qwen exec deploy/qwen -- sh -c "curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8000/metrics"
```

> **opt works the same way**: `config/samples/opt-backend.yaml` (vLLM `opt-125m`, namespace `opt`, `opt-svc`
> ClusterIP 8000; `--shutdown-timeout=3540` for a graceful shutdown) + `modelroute-opt.yaml`; in the following
> steps, replace `qwen` with `opt`.
> Production models (for example multi-node deployments with LeaderWorkerSet) are deployed differently (see the
> `sglang` / `vllm` charts in modelsphere/helm-charts), but they join the routing the same way: create a Service
> and write a ModelRoute.

---

## 4. cart (cache_aware_router, one per model)

**There is one cart per model** (the radix prefix cache only helps within one model). `fullnameOverride` decides
the Service name and the config ConfigMap name (`<name>-config`); the ModelRoute's `cart.service` /
`cart.outputConfigMap` must match them.
With `waitForWorkers=true`, the cart pod waits in Init for autoconfig to write the workers (ready after the
ModelRoute is applied in step 7) instead of crash-looping.

```bash
# The reload/hagate sidecar images are already the chart's defaults (bumped in each chart when autoconfig
# releases), so do not override them with --set — a pinned version stays pinned after the chart default moves,
# and drifts silently if you forget to update it.
helm -n llm-route install cart-qwen modelsphere/cart \
  --set fullnameOverride=cart-qwen

# The cart pod now waits in Init (for workers); that is expected. It turns Running after step 7
kubectl -n llm-route get pods | grep cart-
```

> opt works the same way: `--set fullnameOverride=cart-opt` (must match `cart.service`/`cart.outputConfigMap`
> in `modelroute-opt.yaml`).

---

## 5. openresty (the entry point)

The chart mounts the `openresty-conf` ConfigMap autoconfig produces (one `session_route_<model>.conf` per model),
and the reload sidecar hot-reloads the routes on SIGHUP.
`image.tag` uses the chart default (= the chart's `appVersion`, following the latest release); likewise leave
`reload.image`/`ha.image` at the chart defaults. `bodylog.host` points at the bodylog listener (in Kubernetes it
must be an FQDN or another resolvable address).

```bash
helm -n llm-route install openresty modelsphere/openresty \
  --set fullnameOverride=openresty \
  --set bodylog.host=192.0.2.31

kubectl -n llm-route rollout status deploy/openresty
```

> The openresty Service is **ClusterIP 8080** (in-cluster access); to test from outside, use
> `kubectl -n llm-route port-forward svc/openresty 18080:8080`.

---

## 6. monitor (dashboard + MySQL)

The chart includes MySQL (persistent state + time series) and reads the `monitor-conf` autoconfig produces
(service/nginx/router lines, hot-reloaded every 60s).

> monitor has no public chart: replace `<monitor-chart>` below with your own chart source. Without a monitor,
> skip this step and remove the `spec.monitor` block from the ModelRoute in step 7 (it is optional).

**Recommended for production: keep secrets in an `existingSecret` (created out of band, not managed by Helm)** —
then `helm upgrade` never touches the secrets, with or without `--set`, which avoids the trap of "`--set` without
`--reuse-values` → secrets reset to placeholders" (both the app and the MySQL secrets can be external).

```bash
# ① Create the two secrets out of band (template: k8s/secret.example.yaml in the monitor repository; fill in real values) — the MySQL password must match in both
kubectl -n monitoring apply -f secret.example.yaml    # llm-monitor-secret + llm-monitor-mysql-secret

# ② Install: disable the chart's own secret and reference the external ones
#    (override nginxHost / bodylogSummaryURL with --set for your cluster)
helm -n monitoring install monitor <monitor-chart> \
  --set secret.create=false \
  --set secret.existingSecret=llm-monitor-secret \
  --set mysql.auth.existingSecret=llm-monitor-mysql-secret
kubectl -n monitoring rollout status deploy/monitor
```

> **Quick start (for testing, chart-managed secrets)**: if you do not want external secrets, put the keys and
> passwords in a values file (`secret.data.*` + `mysql.auth.*`, replacing the placeholders; **never commit it**)
> and install with `create: true`. In this mode always upgrade with `--reuse-values`, and take particular care when
> passing `--set` (see §9).

> The dashboard is **NodePort 30080** → `http://<any node IP>:30080` (for example `http://192.0.2.20:30080`), Basic
> Auth `admin/<WEB_PASS>`; the `/tpm` page has its own auth `tpm/<TPM_PASS>`.

---

## 7. Install the ModelRoute (triggers configuration of the whole stack)

The ModelRoute is the **central configuration**: from it, autoconfig writes openresty-conf /
cart-<model>-config / monitor-conf at the same time.
The sample is `config/samples/modelroute-qwen.yaml`: three-tier routing for qwen (cart-qwen → backend pod IPs →
backend-svc VIP fallback); discovery `qwen/qwen-svc`; monitor model `qwen`.

```bash
kubectl apply -f config/samples/modelroute-qwen.yaml

# autoconfig reconciles within seconds; ConfigMap → pod mount propagation lags by ~1 min (kubelet sync)
kubectl -n llm-route get modelroute qwen        # READY should be true, BACKENDS ≥ 1, CART = 1
```

After applying, you should see: the cart-qwen pod go from Init to **Running** (autoconfig wrote the workers);
`session_route_qwen.conf` appear in openresty; and qwen's service line appear on the monitor dashboard.

> **Which namespace for the ModelRoute?** This example uses `llm-route` (the same namespace as cart/openresty, so
> the sample's `cart.service: cart-qwen` and `nginx.service: openresty` can be bare names). The controller watches
> the whole cluster (ClusterRole), so the ModelRoute can also live in the model's namespace (such as `qwen`), but
> then **bare references resolve to the ModelRoute's own namespace** → prefix them explicitly:
> `llm-route/cart-qwen`, `llm-route/openresty` (each `outputConfigMap` already includes its namespace).
> **opt works the same way**: `kubectl apply -f config/samples/modelroute-opt.yaml` (discovery `opt/opt-svc`,
> cart-opt, monitor model `opt-125m`).

---

## 8. Verify (end to end)

```bash
# 8.1 ModelRoute ready
kubectl -n llm-route get modelroute

# 8.2 cart ready (3/3: cart + reload + hagate sidecars)
kubectl -n llm-route get pods | grep -E 'cart-|openresty'

# 8.3 End to end through openresty → cart → backend (call local port 8080 inside the openresty pod)
AUTH_KEY='<openresty entry auth key>'   # never commit the real key
ORP=$(kubectl -n llm-route get pod -l app.kubernetes.io/name=openresty -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
[ -z "$ORP" ] && ORP=$(kubectl -n llm-route get pods -o name | grep openresty | head -1 | cut -d/ -f2)
kubectl -n llm-route exec $ORP -c openresty -- sh -c \
  "curl -s -o /dev/null -w 'qwen /v1/models=%{http_code}\n' http://127.0.0.1:8080/qwen/v1/models -H 'Authorization: Bearer $AUTH_KEY'"
# A chat stream (should be 200, with an X-Routed-Peer header):
kubectl -n llm-route exec $ORP -c openresty -- sh -c \
  "curl -s -D - -o /dev/null http://127.0.0.1:8080/qwen/v1/chat/completions -H 'Content-Type: application/json' \
   -H 'Authorization: Bearer $AUTH_KEY' \
   -d '{\"model\":\"qwen\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":8}' | grep -iE 'HTTP/|x-routed-peer'"

# 8.4 The monitor collects the model (dashboard or /api/status)
kubectl -n monitoring exec deploy/monitor -- python3 -c \
 'import urllib.request,base64,json;r=urllib.request.Request("http://127.0.0.1:8080/api/status");r.add_header("Authorization","Basic "+base64.b64encode(b"admin:<WEB_PASS>").decode());print("api/status",json.load(urllib.request.urlopen(r)) and "OK")'
```

An `X-Routed-Peer` header means the request really was routed by cart to a real backend (cart's
`proxy.add_routed_peer_header=true`).

---

## 9. Upgrade / roll back

Releasing a new version: change the code → tag each repository (CI publishes the images to Docker Hub) → the
charts are released in modelsphere/helm-charts → `helm upgrade` in the cluster.

```bash
helm repo update modelsphere
# Without --version = the latest release; --reuse-values keeps fullnameOverride / secrets / bodylog.host etc. from install time
helm -n llm-route  upgrade autoconfig modelsphere/autoconfig --reuse-values
helm -n llm-route  upgrade openresty  modelsphere/openresty  --reuse-values
helm -n llm-route  upgrade cart-qwen  modelsphere/cart       --reuse-values   # once for each cart-<model>
helm -n monitoring upgrade monitor    <monitor-chart>        --reuse-values

helm -n <ns> history <release>                          # list revisions
helm -n <ns> rollback <release> <REV>                    # roll back to a revision
helm -n <ns> upgrade <release> <chart> --version <x.y.z> --reuse-values   # add --version only to pin or go back to an earlier version
```

> When the chart content is unchanged, `helm upgrade` only updates the release metadata and **does not restart
> pods** (the rendered spec is identical): no interruption.
> A chart version is not necessarily the component version (for example the `cart` chart 0.2.x deploys CART
> 0.6.x); to pin a version, look up the chart versions with `helm search repo modelsphere/<chart> --versions`.

---

## 10. Uninstall / clean up

```bash
kubectl delete -f config/samples/modelroute-qwen.yaml
helm -n llm-route  uninstall openresty cart-qwen autoconfig      # opt: also uninstall cart-opt
helm -n monitoring uninstall monitor
kubectl delete -f config/samples/qwen-backend.yaml               # also deletes the qwen namespace (opt: opt-backend.yaml)
kubectl delete crd modelroutes.routing.modelsphere.dev            # to remove the CRD completely
```
