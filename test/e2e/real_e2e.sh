#!/usr/bin/env bash
# 真实组件端到端:autoconfig controller(真 CRD)驱动 **真 openresty + 真 CART + 真 monitor**。
# 验证不止「写对 ConfigMap」,而是真 router 挂载 + reload sidecar SIGHUP + 真消费。
# 在能 kubectl 的机器上跑(如 k8s-cpu-20)。依赖同目录:modelroutes.yaml(CRD)、controller.yaml。
#   IMG_TAG=0.3.13 bash real_e2e.sh [--keep]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=${NS:-ac-real}
CTRL_NS=${CTRL_NS:-autoconfig}
TAG=${IMG_TAG:-0.3.13}
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
waitcm(){ local ns=$1 cm=$2 key=$3 i; for i in $(seq 1 40); do kubectl -n "$ns" get cm "$cm" -o jsonpath="{.data.$key}" 2>/dev/null | grep -q . && return 0; sleep 3; done; return 1; }

cleanup(){ [ "$KEEP" = 1 ] && { echo "(--keep)"; return; }
  say cleanup
  kubectl -n "$NS" delete mr --all --timeout=40s 2>/dev/null
  kubectl delete ns "$NS" --wait=false 2>/dev/null
  kubectl -n "$CTRL_NS" delete -f "$HERE/controller.yaml" --wait=false 2>/dev/null
}
trap cleanup EXIT

# ---------- 1) CRD + autoconfig controller ----------
say "install CRD + autoconfig controller ($AC:$TAG)"
kubectl apply -f "$HERE/modelroutes.yaml"
kubectl create ns "$CTRL_NS" --dry-run=client -o yaml | kubectl apply -f -
sed "s#image: $AC:.*#image: $AC:$TAG#" "$HERE/controller.yaml" | kubectl apply -f -
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=150s || { bad "controller 未就绪"; kubectl -n "$CTRL_NS" get pods; exit 1; }
ok "controller 就绪"

# ---------- 2) mock 后端 + base ConfigMap(cart base / monitor base)----------
say "mock 后端(2)+ base 配置"
kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" apply -f - <<YAML
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
apiVersion: v1
kind: ConfigMap
metadata: { name: monitor-conf }       # 预置 base(阈值),autoconfig 再往里 merge 每模型 service key
data:
  00-base.conf: |
    alert_ttft: 80
    gpu_temp_warn: 75
YAML
kubectl -n "$NS" rollout status deploy/be --timeout=120s

# ---------- 3) ModelRoute:驱动 openresty + CART + monitor ----------
say "ModelRoute(discovery + cart + openresty + monitor)"
kubectl -n "$NS" apply -f - <<YAML
apiVersion: routing.gpucluster.io/v1alpha1
kind: ModelRoute
metadata: { name: glm }
spec:
  discovery: { service: be-svc, port: 8050 }
  cart: { service: cart-svc, port: 8071, outputConfigMap: $NS/cart-config, maxLoad: 20 }
  openresty:
    route: glm
    listen: 18083
    outputConfigMap: $NS/openresty-conf
    peers: [{ use: cart, priority: 1, maxConcurrency: 180 }, { use: backend, priority: 0 }]
  monitor: { outputConfigMap: $NS/monitor-conf, model: glm-5.1-fp8, gpuType: H100 }
YAML
waitcm "$NS" cart-config 'config\.yaml' && ok "autoconfig 写出 cart-config" || bad "cart-config 未生成"
waitcm "$NS" openresty-conf 'session_route_glm\.conf' && ok "autoconfig 写出 openresty-conf" || bad "openresty-conf 未生成"
waitcm "$NS" monitor-conf 'glm\.monitor\.conf' && ok "autoconfig 写出 monitor service" || bad "monitor service 未生成"

# ---------- 4) 部署真 CART + 真 openresty + 真 monitor(挂 autoconfig 的 ConfigMap + reload sidecar)----------
say "部署真 CART / openresty / monitor"
kubectl -n "$NS" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: { name: cart, labels: { app: cart } }
spec:
  replicas: 1
  selector: { matchLabels: { app: cart } }
  template:
    metadata: { labels: { app: cart } }
    spec:
      shareProcessNamespace: true
      containers:
      - name: cart
        image: $CART_IMG
        command: ["sh","-c","ulimit -n 65535; exec ./launch_service"]   # launch_service 要 ulimit≥65535
        ports: [{ containerPort: 8071 }]
        volumeMounts: [{ name: cfg, mountPath: /workspace/configs }]
      - name: reload
        image: $ACR
        args: ["--watch","/workspace/configs","--process","cache-aware-router"]
        volumeMounts: [{ name: cfg, mountPath: /workspace/configs }]
      volumes: [{ name: cfg, configMap: { name: cart-config } }]   # 整卷挂,autoconfig 写的 config.yaml
---
apiVersion: v1
kind: Service
metadata: { name: cart-svc }
spec: { selector: { app: cart }, ports: [{ port: 8071, targetPort: 8071 }] }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: openresty, labels: { app: openresty } }
spec:
  replicas: 1
  selector: { matchLabels: { app: openresty } }
  template:
    metadata: { labels: { app: openresty } }
    spec:
      shareProcessNamespace: true
      containers:
      - name: openresty
        image: $OR_IMG
        ports: [{ containerPort: 18083 }]
        volumeMounts: [{ name: routes, mountPath: /usr/local/openresty/nginx/conf/conf.d/routes }]
      - name: reload
        image: $ACR
        args: ["--watch","/watch","--process","nginx: master"]
        volumeMounts: [{ name: routes, mountPath: /watch }]
      volumes: [{ name: routes, configMap: { name: openresty-conf } }]
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: monitor, labels: { app: monitor } }
spec:
  replicas: 1
  selector: { matchLabels: { app: monitor } }
  template:
    metadata: { labels: { app: monitor } }
    spec:
      containers:
      - name: monitor
        image: $MON_IMG
        ports: [{ containerPort: 8080 }]
        volumeMounts: [{ name: mconf, mountPath: /etc/monitor/conf.d }]   # base + autoconfig service,monitor 60s 自热加载(无 sidecar)
      volumes: [{ name: mconf, configMap: { name: monitor-conf } }]
YAML
kubectl -n "$NS" rollout status deploy/cart --timeout=150s || { bad "CART 未就绪"; kubectl -n "$NS" logs deploy/cart -c cart --tail=15; }
kubectl -n "$NS" rollout status deploy/openresty --timeout=120s || { bad "openresty 未就绪"; kubectl -n "$NS" logs deploy/openresty -c openresty --tail=15; }
kubectl -n "$NS" rollout status deploy/monitor --timeout=120s || { bad "monitor 未就绪"; kubectl -n "$NS" logs deploy/monitor --tail=15; }

# ---------- 5) 验证真消费 ----------
say "验证:CART workers / openresty peers / monitor services"
# CART:配置里有 backends 作 workers
CARTCFG=$(kubectl -n "$NS" exec deploy/cart -c cart -- cat /workspace/configs/config.yaml 2>/dev/null)
[ "$(echo "$CARTCFG" | grep -c 'url:')" = 2 ] && ok "CART config.yaml 有 2 个 worker(后端)" || bad "CART workers 不对"
# openresty peers:读【权威 ConfigMap】(挂载文件有 ~1min 传播延迟),等 autoconfig 发现 CART 进 peers
for i in $(seq 1 30); do
  ORCONF=$(kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_glm\.conf}' 2>/dev/null)
  echo "$ORCONF" | grep -qE '8071, "cart-0", 1, 180' && break; sleep 4
done
echo "--- openresty-conf peers(权威)---"; echo "$ORCONF" | grep -A5 'peers ='
echo "$ORCONF" | grep -qE '8071, "cart-0", 1, 180' && ok "openresty peers 含 CART(优先)" || bad "openresty 无 CART peer"
[ "$(echo "$ORCONF" | grep -c '8050, "backend-')" -ge 2 ] && ok "openresty peers 含后端(兜底)" || bad "openresty 后端 peer 不对"
# openresty 真加载了:pod Running(rollout 已过)+ nginx master 在
kubectl -n "$NS" exec deploy/openresty -c openresty -- pgrep -f 'nginx: master' >/dev/null 2>&1 && ok "openresty nginx master 运行中(config 已加载)" || bad "openresty 未运行"
# monitor:真加载了 autoconfig 写的 service(svc_attrs 带 model/gpu_type)
kubectl -n "$NS" logs deploy/monitor --tail=80 2>/dev/null | grep -qE 'svc_attrs: glm-.*model=glm-5.1-fp8, gpu_type=H100' && ok "monitor 加载了 autoconfig 写的 service(model/gpu_type 正确)" || { bad "monitor 未加载 service"; kubectl -n "$NS" logs deploy/monitor --tail=15; }

# ---------- 6) 动态:scale 后端 → reload 跟随 ----------
say "scale 后端 2→3,验证真 reload 跟随"
kubectl -n "$NS" scale deploy/be --replicas=3
kubectl -n "$NS" rollout status deploy/be --timeout=120s
for i in $(seq 1 20); do n=$(kubectl -n "$NS" exec deploy/cart -c cart -- cat /workspace/configs/config.yaml 2>/dev/null | grep -c 'url:'); [ "$n" = 3 ] && break; sleep 4; done
[ "$n" = 3 ] && ok "CART workers 跟随到 3(sidecar reload 生效)" || bad "CART workers 未跟随($n)"

say "结果"
[ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"
exit $FAIL
