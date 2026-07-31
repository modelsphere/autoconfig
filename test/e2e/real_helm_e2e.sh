#!/usr/bin/env bash
# 真组件端到端(经 Helm chart):autoconfig controller 驱动 **真 openresty + 真 CART + 真 monitor**,
# 三个组件都用本仓 deploy/helm/{openresty,cart,monitor} chart 部署(chart 建 ConfigMap 初值,autoconfig 更新)。
# 验证:CART 读 workers + /workers 端点、openresty reload 生效 peers、monitor 消费 service+nginx+router 行、scale 跟随。
# 在能 kubectl+helm 的机器上跑(如 k8s-cpu-20)。依赖同目录:charts/{autoconfig,openresty,cart,monitor}(autoconfig chart 含 CRD+RBAC+controller)。
#   openresty chart 已迁到 llm-openresty 仓 helm/openresty/ —— 跑前把它拷进 charts/openresty(cart/monitor 仍在本仓 deploy/helm/)。
#   IMG_TAG=0.3.22 bash real_helm_e2e.sh [--keep]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=${NS:-ac-helm}
CTRL_NS=${CTRL_NS:-autoconfig}
TAG=${IMG_TAG:-0.3.22}
CHARTS=${CHARTS:-$HERE/charts}
AC=harbor.4pd.io/hardcore-tech/autoconfig
ACR=harbor.4pd.io/hardcore-tech/autoconfig-reload:$TAG
OR_IMG=${OR_IMG:-harbor.4pd.io/hardcore-tech/llm-openresty:0.2.0-routes}
CART_IMG=${CART_IMG:-harbor.4pd.io/hardcore-tech/cache_aware_router:v0.6.0}
MON_IMG=${MON_IMG:-harbor.4pd.io/hardcore-tech/llm-monitor:0.1.0}
MOCK=${MOCK:-harbor.4pd.io/hardcore-tech/python:3.12-alpine}
KEEP=0; [ "${1:-}" = "--keep" ] && KEEP=1
FAIL=0
say(){ echo -e "\n=== $* ==="; }
ok(){ echo "  PASS: $*"; }
bad(){ echo "  FAIL: $*"; FAIL=1; }
waiteq(){ local want="$1" desc="$2"; shift 2; local i got; for i in $(seq 1 60); do got="$("$@" 2>/dev/null)"; [ "$got" = "$want" ] && { ok "$desc = $want"; return 0; }; sleep 3; done; bad "$desc: got '$got' want '$want'"; return 1; }

cleanup(){ [ "$KEEP" = 1 ] && { echo "(--keep)"; return; }
  say cleanup
  helm -n "$NS" uninstall openresty cart monitor 2>/dev/null
  kubectl -n "$NS" delete mr --all --timeout=40s 2>/dev/null
  kubectl delete ns "$NS" --wait=false 2>/dev/null
  helm -n "$CTRL_NS" uninstall autoconfig 2>/dev/null
}
trap cleanup EXIT

# ---------- 1) CRD + autoconfig controller(helm chart)----------
say "helm install autoconfig controller ($AC:$TAG)"
kubectl apply -f "$CHARTS/autoconfig/crds/"            # CRD 最新 schema(helm crds/ 不做升级)
helm -n "$CTRL_NS" upgrade --install autoconfig "$CHARTS/autoconfig" --create-namespace \
  --set fullnameOverride=autoconfig-controller --set image.repository="$AC" --set image.tag="$TAG" >/dev/null
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=150s || { bad "controller 未就绪"; kubectl -n "$CTRL_NS" get pods; exit 1; }
ok "controller 就绪"

# ---------- 2) ns + mock 后端(2)(cart 底稿由 cart chart 建,不需 base-cart)----------
say "mock 后端(2)"
kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: { name: be, labels: { app: be } }
spec: { replicas: 2, selector: { matchLabels: { app: be } }, template: { metadata: { labels: { app: be } }, spec: { containers: [{ name: c, image: $MOCK, command: ["sleep","infinity"], ports: [{ containerPort: 8050 }] }] } } }
---
apiVersion: v1
kind: Service
metadata: { name: be-svc }
spec: { selector: { app: be }, ports: [{ port: 8050, targetPort: 8050 }] }
YAML
kubectl -n "$NS" rollout status deploy/be --timeout=120s

# ---------- 3) Helm 装 3 组件(chart 建 openresty-conf / cart-config / monitor-conf 初值)----------
say "helm install openresty / cart / monitor"
helm -n "$NS" upgrade --install openresty "$CHARTS/openresty" \
  --set fullnameOverride=openresty --set image.repository="${OR_IMG%:*}" --set image.tag="${OR_IMG##*:}" \
  --set reload.image="$ACR" >/dev/null && ok "openresty chart installed" || bad "openresty chart 装失败"
helm -n "$NS" upgrade --install cart "$CHARTS/cart" \
  --set fullnameOverride=cart --set image.repository="${CART_IMG%:*}" --set image.tag="${CART_IMG##*:}" \
  --set reload.image="$ACR" >/dev/null && ok "cart chart installed" || bad "cart chart 装失败"
helm -n "$NS" upgrade --install monitor "$CHARTS/monitor" \
  --set fullnameOverride=monitor --set image.repository="${MON_IMG%:*}" --set image.tag="${MON_IMG##*:}" \
  >/dev/null && ok "monitor chart installed" || bad "monitor chart 装失败"
# chart 建的 ConfigMap 就位
waiteq openresty-conf "openresty-conf 已建" kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.metadata.name}'
waiteq cart-config    "cart-config 已建"    kubectl -n "$NS" get cm cart-config    -o jsonpath='{.metadata.name}'
waiteq monitor-conf   "monitor-conf 已建"   kubectl -n "$NS" get cm monitor-conf   -o jsonpath='{.metadata.name}'

# ---------- 4) ModelRoute:驱动三组件(cart svc/openresty svc 名 = fullnameOverride)----------
say "apply ModelRoute(discovery + cart + openresty + monitor.nginx)"
kubectl -n "$NS" apply -f - <<YAML
apiVersion: routing.gpucluster.io/v1alpha1
kind: ModelRoute
metadata: { name: glm }
spec:
  discovery: { service: be-svc, port: 8050 }
  cart: { service: cart, port: 8071, outputConfigMap: $NS/cart-config, maxLoad: 20 }
  nginx:
    route: glm
    outputConfigMap: $NS/openresty-conf
    service: openresty                     # nginx 入口(openresty chart Service)→ monitor nginx 行
    peers: [{ use: cart, priority: 1, maxConcurrency: 180 }, { use: backend, priority: 0 }]
  monitor:
    outputConfigMap: $NS/monitor-conf
    model: glm-5.1-fp8
    gpuType: H100
    # nginx/router 默认开(配了 spec.nginx.service 和 cart)
YAML

# controller 写出内容(权威 ConfigMap)
waiteq true "status.ready" kubectl -n "$NS" get mr glm -o jsonpath='{.status.ready}'
# 等三组件 Ready(cart 初值 workers:[],可能先崩→autoconfig 写真 workers 传播后恢复,给足时间)
kubectl -n "$NS" rollout status deploy/openresty --timeout=180s || { bad "openresty 未就绪"; kubectl -n "$NS" logs deploy/openresty -c openresty --tail=20; }
kubectl -n "$NS" rollout status deploy/cart      --timeout=240s || { bad "cart 未就绪";      kubectl -n "$NS" logs deploy/cart -c cart --tail=20; }
kubectl -n "$NS" rollout status deploy/monitor   --timeout=150s || { bad "monitor 未就绪";   kubectl -n "$NS" logs deploy/monitor --tail=20; }
# cart Ready 后 autoconfig 才发现 CART pod(cartPeers=1),router/openresty-cart-peer 才有
waiteq 1 "status.cartPeers(cart Ready 后)" kubectl -n "$NS" get mr glm -o jsonpath='{.status.cartPeers}'

# ---------- 5) 验证真消费 ----------
say "验证真消费:CART workers + /workers、openresty peers + reload、monitor service/nginx/router"
# CART:mount 的 config.yaml 有 2 worker + /workers 端点返回后端
for i in $(seq 1 40); do CN=$(kubectl -n "$NS" exec deploy/cart -c cart -- sh -c 'cat /workspace/configs/config.yaml 2>/dev/null' | grep -c 'url:'); [ "$CN" = 2 ] && break; sleep 4; done
[ "$CN" = 2 ] && ok "CART config.yaml 有 2 worker(后端)" || bad "CART workers 不对($CN)"
# CART 镜像无 wget/curl,从 alpine be pod 跨 pod 打 cart Service(busybox wget,python3 兜底)
WK=$(kubectl -n "$NS" exec deploy/be -- sh -c 'wget -qO- http://cart:8071/workers 2>/dev/null || python3 -c "import urllib.request;print(urllib.request.urlopen(\"http://cart:8071/workers\",timeout=5).read().decode())" 2>/dev/null')
echo "--- CART /workers(从 be pod 跨 pod 打)---"; echo "$WK" | head -c 400; echo
[ "$(echo "$WK" | grep -oE '8050' | wc -l)" -ge 2 ] && ok "CART /workers 端点返回 2 后端(真消费)" || bad "CART /workers 不含 2 后端"

# openresty:权威 conf 有 CART优先+后端兜底 peers;pod nginx master 在;openresty -t OK;mount 的 route conf 到位
ORCONF=$(kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_glm\.conf}' 2>/dev/null)
echo "$ORCONF" | grep -qE '8071, "cart-0", 1, 180' && ok "openresty-conf 含 CART 优先 peer" || bad "openresty-conf 无 CART peer"
[ "$(echo "$ORCONF" | grep -c '8050, "backend-')" -ge 2 ] && ok "openresty-conf 含后端兜底 peer" || bad "openresty-conf 后端 peer 不对"
kubectl -n "$NS" exec deploy/openresty -c openresty -- pgrep -f 'nginx: master' >/dev/null 2>&1 && ok "openresty nginx master 运行中" || bad "openresty master 未运行"
# 等挂载传播,route conf 出现在 conf.d/routes 且 openresty -t 通过(真加载)
for i in $(seq 1 30); do kubectl -n "$NS" exec deploy/openresty -c openresty -- sh -c 'cat /usr/local/openresty/nginx/conf/conf.d/routes/session_route_glm.conf 2>/dev/null' | grep -q 'listen unix:/usr/local/openresty/nginx/sock/glm.sock' && break; sleep 4; done
kubectl -n "$NS" exec deploy/openresty -c openresty -- sh -c 'cat /usr/local/openresty/nginx/conf/conf.d/routes/session_route_glm.conf 2>/dev/null' | grep -q 'listen unix:/usr/local/openresty/nginx/sock/glm.sock' && ok "openresty pod 挂到 route conf(listen unix:/usr/local/openresty/nginx/sock/glm.sock)" || bad "openresty pod 未挂到 route conf"
kubectl -n "$NS" exec deploy/openresty -c openresty -- /usr/local/openresty/bin/openresty -t >/tmp/ortest 2>&1 && ok "openresty -t 通过(reload 生效的配置合法)" || { bad "openresty -t 失败"; cat /tmp/ortest; }

# monitor:mount 的 glm.monitor.conf 有 service+nginx+router;monitor 加载无解析错误
for i in $(seq 1 30); do kubectl -n "$NS" exec deploy/monitor -- sh -c 'cat /etc/monitor/conf.d/glm.monitor.conf 2>/dev/null' | grep -q '^router:' && break; sleep 4; done
MM=$(kubectl -n "$NS" exec deploy/monitor -- sh -c 'cat /etc/monitor/conf.d/glm.monitor.conf 2>/dev/null')
echo "--- monitor pod 内 glm.monitor.conf ---"; echo "$MM"
[ "$(echo "$MM" | grep -c '^service: glm-')" -ge 2 ] && ok "monitor 消费 service 行(每后端一行)" || bad "monitor service 行不对"
echo "$MM" | grep -qE '^nginx: glm-5.1-fp8-0 \| http://.+:18083$' && ok "monitor 消费 nginx 行(name=model,port=listen)" || bad "monitor 无 nginx 行"
echo "$MM" | grep -qE '^router: glm-router-0 \| http://.+:8071/workers$' && ok "monitor 消费 router 行(CART /workers)" || bad "monitor 无 router 行"
kubectl -n "$NS" logs deploy/monitor --tail=200 2>/dev/null | grep -qiE 'Traceback|ValueError|line [0-9]+:' && bad "monitor 日志有配置解析错误" || ok "monitor 加载 conf.d 无解析错误"

# ---------- 6) 动态:scale 后端 2→3 → CART reload 跟随 ----------
say "scale 后端 2→3,验证 CART reload 跟随"
kubectl -n "$NS" scale deploy/be --replicas=3
kubectl -n "$NS" rollout status deploy/be --timeout=120s
for i in $(seq 1 40); do n=$(kubectl -n "$NS" exec deploy/cart -c cart -- sh -c 'cat /workspace/configs/config.yaml 2>/dev/null' | grep -c 'url:'); [ "$n" = 3 ] && break; sleep 4; done
[ "$n" = 3 ] && ok "CART workers 跟随到 3(sidecar reload 生效)" || bad "CART workers 未跟随($n)"

say "结果"
[ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"
exit $FAIL
