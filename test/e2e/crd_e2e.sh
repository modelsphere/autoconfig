#!/usr/bin/env bash
# autoconfig CRD controller 端到端(在能 kubectl 的机器上跑,如 k8s-cpu-20)。
# 装 CRD + controller → mock 后端(Service→EndpointSlice)+ mock CART → ModelRoute
# → 验 cart-config workers + openresty CART优先/后端兜底 peers + status → scale 跟随 → 删除清理。
#
# 依赖:autoconfig helm chart(默认 $HERE/charts/autoconfig,含 CRD+RBAC+controller)。
# 用法:NS=ac-e2e IMG=harbor.4pd.io/hardcore-tech/autoconfig:0.3.19 bash crd_e2e.sh [--keep]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=${NS:-ac-e2e}
CTRL_NS=${CTRL_NS:-autoconfig}
AC_CHART=${AC_CHART:-$HERE/charts/autoconfig}
IMG=${IMG:-harbor.4pd.io/hardcore-tech/autoconfig:0.3.19}
MOCK=${MOCK:-harbor.4pd.io/hardcore-tech/python:3.12-alpine}
KEEP=0; [ "${1:-}" = "--keep" ] && KEEP=1
FAIL=0
say(){ echo -e "\n=== $* ==="; }
ok(){ echo "  PASS: $*"; }
bad(){ echo "  FAIL: $*"; FAIL=1; }
# 轮询直到命令输出等于期望值(超时 fail)
waiteq(){ local want="$1" desc="$2"; shift 2; local i got; for i in $(seq 1 40); do got="$("$@" 2>/dev/null)"; [ "$got" = "$want" ] && { ok "$desc = $want"; return 0; }; sleep 3; done; bad "$desc: got '$got' want '$want'"; return 1; }

cleanup(){ [ "$KEEP" = 1 ] && { echo "(--keep,保留资源)"; return; }
  say "cleanup"
  kubectl delete ns "$NS" --wait=false 2>/dev/null
  helm -n "$CTRL_NS" uninstall autoconfig 2>/dev/null
}
trap cleanup EXIT

# ---------- 1) CRD + controller(helm chart:CRD+RBAC+controller 一把装)----------
say "helm install autoconfig controller ($IMG)"
kubectl apply -f "$AC_CHART/crds/"                      # 确保 CRD 是最新 schema(helm crds/ 不做升级)
helm -n "$CTRL_NS" upgrade --install autoconfig "$AC_CHART" --create-namespace \
  --set fullnameOverride=autoconfig-controller --set image.repository="${IMG%:*}" --set image.tag="${IMG##*:}" >/dev/null
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=150s || { bad "controller 未就绪"; kubectl -n "$CTRL_NS" get pods; exit 1; }
ok "controller 就绪"

# ---------- 2) mock 后端 + CART + ModelRoute ----------
say "mock 后端(2) + CART(1) + ModelRoute"
kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: { name: glm-backend, labels: { app: glm, role: leader } }
spec:
  replicas: 2
  selector: { matchLabels: { app: glm, role: leader } }
  template:
    metadata: { labels: { app: glm, role: leader } }
    spec: { containers: [{ name: c, image: $MOCK, command: ["sleep","infinity"], ports: [{ containerPort: 8050 }] }] }
---
apiVersion: v1
kind: Service
metadata: { name: glm-leader }
spec: { selector: { app: glm, role: leader }, ports: [{ port: 8050, targetPort: 8050 }] }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: cart-glm, labels: { app: cart } }
spec:
  replicas: 1
  selector: { matchLabels: { app: cart } }
  template:
    metadata: { labels: { app: cart } }
    spec: { containers: [{ name: c, image: $MOCK, command: ["sleep","infinity"], ports: [{ containerPort: 8071 }] }] }
---
apiVersion: v1
kind: Service
metadata: { name: cart-glm }
spec: { selector: { app: cart }, ports: [{ port: 8071, targetPort: 8071 }] }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: openresty-mock, labels: { app: openresty } }
spec:
  replicas: 1
  selector: { matchLabels: { app: openresty } }
  template:
    metadata: { labels: { app: openresty } }
    spec: { containers: [{ name: c, image: $MOCK, command: ["sleep","infinity"], ports: [{ containerPort: 18083 }] }] }
---
apiVersion: v1
kind: Service
metadata: { name: openresty-svc }
spec: { selector: { app: openresty }, ports: [{ port: 18083, targetPort: 18083 }] }
---
# 三个目标 ConfigMap:生产由 helm chart 建;本 mock 测试没装 chart,预建空的(autoconfig 只更新不创建)
apiVersion: v1
kind: ConfigMap
metadata: { name: cart-config }
---
apiVersion: v1
kind: ConfigMap
metadata: { name: openresty-conf }
---
apiVersion: v1
kind: ConfigMap
metadata: { name: monitor-conf }
---
apiVersion: routing.gpucluster.io/v1alpha1
kind: ModelRoute
metadata: { name: glm }
spec:
  discovery: { service: glm-leader, port: 8050 }
  cart: { service: cart-glm, port: 8071, outputConfigMap: $NS/cart-config, maxLoad: 20 }
  nginx:
    route: glm
    listen: 18083
    outputConfigMap: $NS/openresty-conf
    service: openresty-svc                 # nginx 入口 Service → monitor nginx 行 + 事件驱动
    values: { ttft_limit_ms: "60000" }
    peers: [{ use: cart, priority: 1, maxConcurrency: 180 }, { use: backend, priority: 0 }]
  monitor:
    outputConfigMap: $NS/monitor-conf
    model: glm
    gpuType: H100
    # nginx/router 默认开(配了 spec.nginx.service 和 cart);router 自动用 cartPeers
YAML
kubectl -n "$NS" rollout status deploy/glm-backend --timeout=120s
kubectl -n "$NS" rollout status deploy/cart-glm --timeout=120s
kubectl -n "$NS" rollout status deploy/openresty-mock --timeout=120s

# ---------- 3) 断言:status + cart workers + openresty peers ----------
say "验证:status / cart-config / openresty-conf"
rb_backends(){ kubectl -n "$NS" get mr glm -o jsonpath='{.status.backends}'; }
rb_cart(){ kubectl -n "$NS" get mr glm -o jsonpath='{.status.cartPeers}'; }
rb_ready(){ kubectl -n "$NS" get mr glm -o jsonpath='{.status.ready}'; }
waiteq 2 "status.backends" rb_backends
waiteq 1 "status.cartPeers" rb_cart
waiteq true "status.ready" rb_ready

CART=$(kubectl -n "$NS" get cm cart-config -o jsonpath='{.data.config\.yaml}' 2>/dev/null)
echo "--- cart-config config.yaml ---"; echo "$CART"
[ "$(echo "$CART" | grep -c 'url:')" = 2 ] && ok "cart workers = 2" || bad "cart workers != 2"
echo "$CART" | grep -q 'max_load: 20' && ok "max_load 20" || bad "无 max_load 20"

OR=$(kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_glm\.conf}' 2>/dev/null)
echo "--- openresty session_route_glm.conf ---"; echo "$OR"
echo "$OR" | grep -qE '8071, "cart-0", 1, 180' && ok "CART peer priority-1 maxConc-180" || bad "无 CART 优先 peer"
[ "$(echo "$OR" | grep -c '8050, "backend-')" = 2 ] && ok "后端兜底 peer = 2" || bad "后端 peer != 2"
echo "$OR" | grep -q 'listen 18083' && ok "listen 18083" || bad "无 listen 18083"
# 顺序:CART 在后端之前
cl=$(echo "$OR" | grep -n 'cart-0' | head -1 | cut -d: -f1); bl=$(echo "$OR" | grep -n 'backend-0' | head -1 | cut -d: -f1)
[ -n "$cl" ] && [ -n "$bl" ] && [ "$cl" -lt "$bl" ] && ok "CART 排在后端之前" || bad "CART/后端顺序不对"

MON=$(kubectl -n "$NS" get cm monitor-conf -o jsonpath='{.data.glm\.monitor\.conf}' 2>/dev/null)
echo "--- monitor glm.monitor.conf ---"; echo "$MON"
[ "$(echo "$MON" | grep -c '^service: glm-')" = 2 ] && ok "monitor service 行 = 2(每后端一行)" || bad "monitor service 行 != 2"
echo "$MON" | grep -q '| glm | H100' && ok "monitor model/gpu_type 正确" || bad "monitor model/gpu_type 不对"
echo "$MON" | grep -qE '^nginx: glm-0 \| http://.+:18083$' && ok "monitor nginx 行(name=model,port=listen)" || bad "无 monitor nginx 行"
echo "$MON" | grep -qE '^router: glm-router-0 \| http://.+:8071/workers$' && ok "monitor router 行(CART /workers)" || bad "无 monitor router 行"

# ---------- 4) scale 跟随 ----------
say "scale 后端 2→3,验证跟随"
kubectl -n "$NS" scale deploy/glm-backend --replicas=3
kubectl -n "$NS" rollout status deploy/glm-backend --timeout=120s
waiteq 3 "status.backends(scale后)" rb_backends
for i in $(seq 1 20); do n=$(kubectl -n "$NS" get cm cart-config -o jsonpath='{.data.config\.yaml}' | grep -c 'url:'); [ "$n" = 3 ] && break; sleep 3; done
[ "$n" = 3 ] && ok "cart workers 跟随到 3" || bad "cart workers 未跟随($n)"

# ---------- 5) 删除清理:finalizer 从各共享 ConfigMap 摘 key(CM 本体归 chart/手工,不删)----------
say "删除 ModelRoute,验证清理"
kubectl -n "$NS" delete mr glm --timeout=60s
for i in $(seq 1 20); do kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_glm\.conf}' 2>/dev/null | grep -q . || break; sleep 3; done
kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_glm\.conf}' 2>/dev/null | grep -q . && bad "openresty key 未被 finalizer 摘除" || ok "openresty key 已摘除(finalizer)"
for i in $(seq 1 20); do kubectl -n "$NS" get cm cart-config -o jsonpath='{.data.config\.yaml}' 2>/dev/null | grep -q . || break; sleep 3; done
kubectl -n "$NS" get cm cart-config -o jsonpath='{.data.config\.yaml}' 2>/dev/null | grep -q . && bad "cart-config config.yaml 未被摘除" || ok "cart-config config.yaml 已摘除(finalizer,CM 本体保留)"

say "结果"
[ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"
exit $FAIL
