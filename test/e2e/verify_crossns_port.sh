#!/usr/bin/env bash
# 验证两个新特性(0.3.8 端口自动推导 + 0.3.20 service ns/name 跨 ns 发现):
# 在【别的 ns(xns)】建 ModelRoute,discovery.service=kimi/kimi-k26-leader(跨 ns)、【不写 port】,
# 验证仍发现 kimi leader、端口自动 = 8050。前提:kimi LWS 在跑、autoconfig controller 已升到目标 tag。
# 依赖:autoconfig helm chart($HERE/charts/autoconfig)。用法:IMG_TAG=0.3.20 bash verify_crossns_port.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
TAG=${IMG_TAG:-0.3.20}
CTRL_NS=${CTRL_NS:-autoconfig}
AC=harbor.4pd.io/hardcore-tech/autoconfig
AC_CHART=${AC_CHART:-$HERE/charts/autoconfig}
NS=xns
FAIL=0; ok(){ echo "  PASS: $*"; }; bad(){ echo "  FAIL: $*"; FAIL=1; }
cleanup(){ kubectl delete ns "$NS" --wait=false 2>/dev/null; }
trap cleanup EXIT

echo "=== 升级 controller 到 $TAG ==="
kubectl apply -f "$AC_CHART/crds/" >/dev/null
helm -n "$CTRL_NS" upgrade --install autoconfig "$AC_CHART" --create-namespace \
  --set fullnameOverride=autoconfig-controller --set image.repository="$AC" --set image.tag="$TAG" >/dev/null
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=150s | tail -1

echo "=== 在 xns ns 建 ModelRoute:跨 ns 发现 kimi leader + 不写 port ==="
kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" create configmap openresty-conf >/dev/null 2>&1  # 目标 CM 预建(无 chart;autoconfig 只更新不创建)
kubectl -n "$NS" apply -f - <<YAML
apiVersion: routing.gpucluster.io/v1alpha1
kind: ModelRoute
metadata: { name: xns-kimi }
spec:
  discovery: { service: kimi/kimi-k26-leader }     # ← ns/name 跨 ns;无 port(自动推导)
  nginx:
    route: xnskimi
    listen: 18080
    outputConfigMap: $NS/openresty-conf
    peers: [{ use: backend, priority: 0 }]
YAML

echo "=== 断言 ==="
LEADER_IP=$(kubectl -n kimi get pod -l app=kimi-k26,role=leader -o jsonpath='{.items[0].status.podIP}')
echo "  kimi leader podIP=$LEADER_IP(在 kimi ns);ModelRoute 在 $NS ns"
for i in $(seq 1 30); do [ "$(kubectl -n "$NS" get mr xns-kimi -o jsonpath='{.status.backends}' 2>/dev/null)" = 1 ] && break; sleep 3; done
b=$(kubectl -n "$NS" get mr xns-kimi -o jsonpath='{.status.backends}' 2>/dev/null)
[ "$b" = 1 ] && ok "跨 ns 发现到 kimi leader(status.backends=1)" || bad "跨 ns 未发现(backends=$b)"
OR=$(kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_xnskimi\.conf}' 2>/dev/null)
echo "$OR" | grep -q "\"$LEADER_IP\", 8050, \"backend-0\"" && ok "端口自动推导 = 8050(EndpointSlice 取)且 IP=kimi leader" || { bad "端口/IP 不对"; echo "$OR" | grep -A5 'peers'; }

echo "=== 结果 ==="; [ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"; exit $FAIL
