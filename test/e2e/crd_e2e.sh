#!/usr/bin/env bash
# autoconfig CRD controller 端到端(在能 kubectl 的机器上跑,如 k8s-cpu-20)。
# 装 CRD + controller → mock 后端(Service→EndpointSlice)+ mock CART → RouterBinding
# → 验 cart-config workers + openresty CART优先/后端兜底 peers + status → scale 跟随 → 删除清理。
#
# 依赖同目录文件:routerbindings.yaml(CRD)、controller.yaml(controller 部署)。
# 用法:NS=ac-e2e IMG=harbor.4pd.io/hardcore-tech/autoconfig:0.2.0 bash crd_e2e.sh [--keep]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=${NS:-ac-e2e}
CTRL_NS=${CTRL_NS:-autoconfig}
IMG=${IMG:-harbor.4pd.io/hardcore-tech/autoconfig:0.2.0}
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
  kubectl -n "$CTRL_NS" delete -f "$HERE/controller.yaml" --wait=false 2>/dev/null
}
trap cleanup EXIT

# ---------- 1) CRD + controller ----------
say "install CRD + controller ($IMG)"
kubectl apply -f "$HERE/routerbindings.yaml"
kubectl create ns "$CTRL_NS" --dry-run=client -o yaml | kubectl apply -f -
# 用传入 IMG 覆盖 controller.yaml 里的 image
sed "s#image: harbor.4pd.io/hardcore-tech/autoconfig:.*#image: $IMG#" "$HERE/controller.yaml" | kubectl apply -f -
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=150s || { bad "controller 未就绪"; kubectl -n "$CTRL_NS" get pods; exit 1; }
ok "controller 就绪"

# ---------- 2) mock 后端 + CART + RouterBinding ----------
say "mock 后端(2) + CART(1) + RouterBinding"
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
apiVersion: v1
kind: ConfigMap
metadata: { name: base-cart }
data: { config.base.yaml: "server: { host: \"0.0.0.0\", port: 6700 }" }
---
apiVersion: routing.4pd.io/v1alpha1
kind: RouterBinding
metadata: { name: glm }
spec:
  discovery: { service: glm-leader, port: 8050 }
  cart: { service: cart-glm, port: 8071, outputConfigMap: $NS/cart-config, baseConfigRef: { name: base-cart, key: config.base.yaml }, maxLoad: 20 }
  openresty:
    route: glm
    listen: 18083
    outputConfigMap: $NS/openresty-conf
    values: { ttft_limit_ms: "60000" }
    sources: [{ use: cart, priority: 1, maxConcurrency: 180 }, { use: backend, priority: 0 }]
YAML
kubectl -n "$NS" rollout status deploy/glm-backend --timeout=120s
kubectl -n "$NS" rollout status deploy/cart-glm --timeout=120s

# ---------- 3) 断言:status + cart workers + openresty peers ----------
say "验证:status / cart-config / openresty-conf"
rb_backends(){ kubectl -n "$NS" get rb glm -o jsonpath='{.status.backends}'; }
rb_cart(){ kubectl -n "$NS" get rb glm -o jsonpath='{.status.cartPeers}'; }
rb_ready(){ kubectl -n "$NS" get rb glm -o jsonpath='{.status.ready}'; }
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

# ---------- 4) scale 跟随 ----------
say "scale 后端 2→3,验证跟随"
kubectl -n "$NS" scale deploy/glm-backend --replicas=3
kubectl -n "$NS" rollout status deploy/glm-backend --timeout=120s
waiteq 3 "status.backends(scale后)" rb_backends
for i in $(seq 1 20); do n=$(kubectl -n "$NS" get cm cart-config -o jsonpath='{.data.config\.yaml}' | grep -c 'url:'); [ "$n" = 3 ] && break; sleep 3; done
[ "$n" = 3 ] && ok "cart workers 跟随到 3" || bad "cart workers 未跟随($n)"

# ---------- 5) 删除清理:finalizer 摘 openresty key + ownerRef GC cart-config ----------
say "删除 RouterBinding,验证清理"
kubectl -n "$NS" delete rb glm --timeout=60s
for i in $(seq 1 20); do kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_glm\.conf}' 2>/dev/null | grep -q . || break; sleep 3; done
kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_glm\.conf}' 2>/dev/null | grep -q . && bad "openresty key 未被 finalizer 摘除" || ok "openresty key 已摘除(finalizer)"
for i in $(seq 1 20); do kubectl -n "$NS" get cm cart-config >/dev/null 2>&1 || break; sleep 3; done
kubectl -n "$NS" get cm cart-config >/dev/null 2>&1 && bad "cart-config 未被 GC" || ok "cart-config 已 GC(ownerRef)"

say "结果"
[ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"
exit $FAIL
