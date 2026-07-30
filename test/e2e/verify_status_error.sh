#!/usr/bin/env bash
# 验证:发现失败(多端口 Service 未显式配 port)时,原因写进 ModelRoute status(DiscoverError),
# kubectl describe/get 看得到——而非只进 controller 日志。前提:controller 已升到目标 tag。
# 依赖:autoconfig helm chart($HERE/charts/autoconfig)。用法:IMG_TAG=0.3.19 bash verify_status_error.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
TAG=${IMG_TAG:-0.3.19}
CTRL_NS=${CTRL_NS:-autoconfig}
AC=harbor.4pd.io/hardcore-tech/autoconfig
AC_CHART=${AC_CHART:-$HERE/charts/autoconfig}
NS=sterr
MOCK=${MOCK:-harbor.4pd.io/hardcore-tech/python:3.12-alpine}
FAIL=0; ok(){ echo "  PASS: $*"; }; bad(){ echo "  FAIL: $*"; FAIL=1; }
cleanup(){ kubectl delete ns "$NS" --wait=false 2>/dev/null; }
trap cleanup EXIT

kubectl apply -f "$AC_CHART/crds/" >/dev/null
helm -n "$CTRL_NS" upgrade --install autoconfig "$AC_CHART" --create-namespace \
  --set fullnameOverride=autoconfig-controller --set image.repository="$AC" --set image.tag="$TAG" >/dev/null
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=150s | tail -1

echo "=== 多端口 mock 后端 + 不写 port 的 ModelRoute ==="
kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: { name: be, labels: { app: be } }
spec: { replicas: 1, selector: { matchLabels: { app: be } }, template: { metadata: { labels: { app: be } }, spec: { containers: [{ name: c, image: $MOCK, command: ["sleep","infinity"], ports: [{ containerPort: 8050 },{ containerPort: 8060 }] }] } } }
---
apiVersion: v1
kind: Service
metadata: { name: be-multi }
spec: { selector: { app: be }, ports: [{ name: a, port: 8050, targetPort: 8050 },{ name: b, port: 8060, targetPort: 8060 }] }   # 两个端口
---
apiVersion: routing.gpucluster.io/v1alpha1
kind: ModelRoute
metadata: { name: mp }
spec:
  discovery: { service: be-multi }            # ← 多端口且不写 port → 应报 DiscoverError
  nginx: { route: mp, listen: 18080, outputConfigMap: $NS/openresty-conf, peers: [{ use: backend }] }
YAML
kubectl -n "$NS" rollout status deploy/be --timeout=60s | tail -1

echo "=== 断言:status 出现 DiscoverError + 多端口原因(apply 本身成功,报在 reconcile)==="
kubectl -n "$NS" get mr mp >/dev/null 2>&1 && ok "apply 成功(不在 apply 时报错)" || bad "apply 失败"
reason=""; msg=""
for i in $(seq 1 20); do
  reason=$(kubectl -n "$NS" get mr mp -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null)
  [ "$reason" = "DiscoverError" ] && break; sleep 3
done
msg=$(kubectl -n "$NS" get mr mp -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null)
ready=$(kubectl -n "$NS" get mr mp -o jsonpath='{.status.ready}' 2>/dev/null)
echo "  status.ready=$ready reason=$reason"
echo "  message=$msg"
[ "$reason" = "DiscoverError" ] && ok "status 有 DiscoverError" || bad "status 无 DiscoverError(reason=$reason)"
echo "$msg" | grep -q '多个端口' && ok "message 说明多端口原因" || bad "message 未说明原因"
[ "$ready" != "true" ] && ok "ready != true(未就绪)" || bad "ready 竟为 true"

echo "=== 结果 ==="; [ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"; exit $FAIL
