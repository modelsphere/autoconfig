#!/usr/bin/env bash
# 用 Helm chart 部署 autoconfig controller 并端到端验证(在能 helm/kubectl 的机器上跑)。
# lint + template + helm install → controller 起来 → 一个 ModelRoute 调谐 → uninstall(CRD 保留)。
# 依赖:同目录 autoconfig/ chart 目录。用法:CHART=./autoconfig bash helm_e2e.sh [--keep]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
CHART=${CHART:-$HERE/autoconfig}
NS=${NS:-autoconfig}
TNS=${TNS:-ac-helm-e2e}
IMG_TAG=${IMG_TAG:-0.3.4}
MOCK=${MOCK:-harbor.4pd.io/hardcore-tech/python:3.12-alpine}
KEEP=0; [ "${1:-}" = "--keep" ] && KEEP=1
FAIL=0
say(){ echo -e "\n=== $* ==="; }
ok(){ echo "  PASS: $*"; }
bad(){ echo "  FAIL: $*"; FAIL=1; }
waiteq(){ local want="$1" desc="$2"; shift 2; local i got; for i in $(seq 1 40); do got="$("$@" 2>/dev/null)"; [ "$got" = "$want" ] && { ok "$desc = $want"; return 0; }; sleep 3; done; bad "$desc: got '$got' want '$want'"; }

cleanup(){ [ "$KEEP" = 1 ] && { echo "(--keep)"; return; }
  say cleanup
  kubectl delete ns "$TNS" --wait=false 2>/dev/null
  helm uninstall autoconfig -n "$NS" 2>/dev/null
  kubectl delete ns "$NS" --wait=false 2>/dev/null
  kubectl delete crd modelroutes.routing.gpucluster.io --wait=false 2>/dev/null
}
trap cleanup EXIT

say "helm lint + template"
helm lint "$CHART" && ok "lint" || bad "lint"
helm template ac "$CHART" >/dev/null 2>/tmp/helm_tpl_err && ok "template 渲染" || { bad "template"; cat /tmp/helm_tpl_err; }

say "helm install(image.tag=$IMG_TAG)"
helm upgrade --install autoconfig "$CHART" -n "$NS" --create-namespace \
  --set image.tag="$IMG_TAG" --wait --timeout 200s && ok "helm install" || { bad "helm install"; kubectl -n "$NS" get pods; }
kubectl -n "$NS" rollout status deploy -l app.kubernetes.io/instance=autoconfig --timeout=60s && ok "controller 就绪" || bad "controller 未就绪"
kubectl get crd modelroutes.routing.gpucluster.io >/dev/null 2>&1 && ok "CRD 已安装" || bad "CRD 未安装"

say "mock 后端 + CART + ModelRoute(helm 装的 controller 调谐)"
kubectl create ns "$TNS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$TNS" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: { name: be, labels: { app: be } }
spec: { replicas: 2, selector: { matchLabels: { app: be } }, template: { metadata: { labels: { app: be } }, spec: { containers: [{ name: c, image: $MOCK, command: ["sleep","infinity"] }] } } }
---
apiVersion: v1
kind: Service
metadata: { name: be-svc }
spec: { selector: { app: be }, ports: [{ port: 8050 }] }
---
apiVersion: routing.gpucluster.io/v1alpha1
kind: ModelRoute
metadata: { name: demo }
spec:
  discovery: { service: be-svc, port: 8050 }
  openresty:
    route: demo
    listen: 18099
    outputConfigMap: $TNS/openresty-conf
    sources: [{ use: backend }]
YAML
kubectl -n "$TNS" rollout status deploy/be --timeout=120s
waiteq 2 "rb.status.backends" kubectl -n "$TNS" get mr demo -o jsonpath='{.status.backends}'
waiteq true "rb.status.ready" kubectl -n "$TNS" get mr demo -o jsonpath='{.status.ready}'
OR=$(kubectl -n "$TNS" get cm openresty-conf -o jsonpath='{.data.session_route_demo\.conf}' 2>/dev/null)
[ "$(echo "$OR" | grep -c '8050, "backend-')" = 2 ] && ok "openresty peers = 2 后端" || bad "openresty peers != 2"
echo "$OR" | grep -q 'listen 18099' && ok "listen 18099" || bad "无 listen 18099"

say "helm uninstall(验证 CRD 因 resource-policy:keep 保留)"
helm uninstall autoconfig -n "$NS" && ok "uninstall" || bad "uninstall"
kubectl get crd modelroutes.routing.gpucluster.io >/dev/null 2>&1 && ok "CRD 卸载后仍保留(keep)" || bad "CRD 被误删"

say "结果"
[ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"
exit $FAIL
