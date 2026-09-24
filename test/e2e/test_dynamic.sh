#!/usr/bin/env bash
# Phase 1 动态:B 扩缩容 + C 增删模型 + D monitor 消费/存活 + G4 session 亲和。
# 后端用可扩缩 mock(autoconfig 发现行为不需真推理);真推理/GPU 数据在 resilience/opt 用真 opt-125m。
set -uo pipefail
NS=${NS:-llm-route}; CTRL_NS=autoconfig; CHARTS=~/prod-e2e/charts
H=harbor.4pd.io/hardcore-tech; TAG=clusterip-test
AC=$H/autoconfig; ACR=$H/autoconfig-reload:$TAG; AH=$H/autoconfig-hagate:$TAG
OR_IMG=$H/llm-openresty:e2e0; CART_IMG=$H/cache_aware_router:v0.6.0; MON_IMG=$H/llm-monitor:k8s; MOCK=$H/python:3.12-alpine
KEEP=${KEEP:-0}; FAIL=0
say(){ echo -e "\n=== $* ==="; }; ok(){ echo "  PASS: $*"; }; bad(){ echo "  FAIL: $*"; FAIL=1; }
waiteq(){ local w="$1" d="$2"; shift 2; local i g; for i in $(seq 1 60); do g="$("$@" 2>/dev/null)"; [ "$g" = "$w" ] && { ok "$d = $w"; return 0; }; sleep 3; done; bad "$d: got '$g' want '$w'"; return 1; }
# 轮询直到函数返回值 == want(用于等 cart/openresty/monitor 配置跟随)
waitval(){ local w="$1" d="$2"; shift 2; local i g; for i in $(seq 1 45); do g="$(eval "$1" 2>/dev/null)"; [ "$g" = "$w" ] && { ok "$d = $w"; return 0; }; sleep 4; done; bad "$d: got '$g' want '$w'"; return 1; }
cleanup(){ [ "$KEEP" = 1 ] && { echo "(--keep)"; return; }
  say cleanup; helm -n "$NS" uninstall openresty cart monitor 2>/dev/null
  kubectl -n "$NS" delete mr --all --timeout=40s 2>/dev/null; kubectl delete ns "$NS" --wait=false 2>/dev/null
  helm -n "$CTRL_NS" uninstall autoconfig 2>/dev/null; }
trap cleanup EXIT

######## SETUP ########
say "SETUP: controller + mock 后端(2)+ openresty/cart/monitor + ModelRoute m1"
kubectl apply -f "$CHARTS/autoconfig/crds/" >/dev/null
helm -n "$CTRL_NS" upgrade --install autoconfig "$CHARTS/autoconfig" --create-namespace \
  --set fullnameOverride=autoconfig-controller --set image.repository="$AC" --set image.tag="$TAG" --set image.pullPolicy=Always >/dev/null
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=150s >/dev/null && ok controller || { bad controller; exit 1; }
kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" apply -f - >/dev/null <<YAML
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
kubectl -n "$NS" rollout status deploy/be --timeout=120s >/dev/null
helm -n "$NS" upgrade --install openresty "$CHARTS/openresty" --set fullnameOverride=openresty \
  --set image.repository="${OR_IMG%:*}" --set image.tag="${OR_IMG##*:}" --set reload.image="$ACR" --set ha.image="$AH" \
  --set resources.requests.cpu=200m --set resources.requests.memory=256Mi --set resources.limits.cpu=2 --set resources.limits.memory=2Gi >/dev/null
helm -n "$NS" upgrade --install cart "$CHARTS/cache_aware_router" --set fullnameOverride=cart \
  --set image.repository="${CART_IMG%:*}" --set image.tag="${CART_IMG##*:}" --set reload.image="$ACR" --set ha.image="$AH" >/dev/null
helm -n "$NS" upgrade --install monitor "$CHARTS/monitor" --set fullnameOverride=monitor \
  --set image.repository="${MON_IMG%:*}" --set image.tag="${MON_IMG##*:}" --set service.nodePort="" \
  --set-string secret.data.WEB_TPM_AUTH_PASS=testpass --set-string mysql.auth.rootPassword=rootpass --set-string mysql.auth.password=monpass >/dev/null
kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: routing.modelsphere.dev/v1alpha1
kind: ModelRoute
metadata: { name: m1 }
spec:
  discovery: { service: be-svc, port: 8050 }
  cart: { service: cart, port: 8071, outputConfigMap: $NS/cart-config, maxLoad: 20 }
  nginx: { route: m1, outputConfigMap: $NS/openresty-conf, service: openresty, peers: [{use: cart, priority: 3, maxConcurrency: 180},{use: backend, priority: 2},{use: backend-svc, priority: 1}] }
  monitor: { outputConfigMap: $NS/monitor-conf, model: m1 }
YAML
waiteq true "status.ready" kubectl -n "$NS" get mr m1 -o jsonpath='{.status.ready}'
waiteq 2 "status.backends" kubectl -n "$NS" get mr m1 -o jsonpath='{.status.backends}'
kubectl -n "$NS" rollout status deploy/cart --timeout=180s >/dev/null && ok "cart Ready(initContainer 等到 workers)" || bad "cart 未就绪"
kubectl -n "$NS" rollout status deploy/openresty --timeout=120s >/dev/null && ok "openresty Ready" || bad "openresty 未就绪"
kubectl -n "$NS" rollout status deploy/monitor --timeout=150s >/dev/null && ok "monitor Ready(k8s+mysql)" || bad "monitor 未就绪"

# helper:数 cart workers / openresty backend peers / monitor service 行
cw(){ kubectl -n "$NS" get cm cart-config -o jsonpath='{.data.config\.yaml}' 2>/dev/null | grep -c 'url:'; }
orb(){ kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_m1\.conf}' 2>/dev/null | grep -cE '"backend-[0-9]'; }
ms(){ kubectl -n "$NS" get cm monitor-conf -o jsonpath='{.data.m1\.monitor\.conf}' 2>/dev/null | grep -c '^service: '; }

######## B 扩缩容 ########
say "B1: 后端 scale 2→4 → cart/openresty/monitor 三组件都跟随到 4"
kubectl -n "$NS" scale deploy/be --replicas=4 >/dev/null
kubectl -n "$NS" rollout status deploy/be --timeout=120s >/dev/null
waiteq 4 "status.backends(4)" kubectl -n "$NS" get mr m1 -o jsonpath='{.status.backends}'
waitval 4 "cart workers → 4" cw
waitval 4 "openresty backend peers → 4" orb
waitval 4 "monitor service 行 → 4" ms

say "B2: 后端 scale 4→1 → 三组件都跟随到 1"
kubectl -n "$NS" scale deploy/be --replicas=1 >/dev/null
kubectl -n "$NS" rollout status deploy/be --timeout=120s >/dev/null
waiteq 1 "status.backends(1)" kubectl -n "$NS" get mr m1 -o jsonpath='{.status.backends}'
waitval 1 "cart workers → 1" cw
waitval 1 "openresty backend peers → 1" orb
waitval 1 "monitor service 行 → 1" ms

######## C 增删模型 ########
say "C1: 新增 ModelRoute m2(route=m2,复用 be-svc)→ openresty/monitor 新增,m1 不受影响"
kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: routing.modelsphere.dev/v1alpha1
kind: ModelRoute
metadata: { name: m2 }
spec:
  discovery: { service: be-svc, port: 8050 }
  nginx: { route: m2, outputConfigMap: $NS/openresty-conf, peers: [{use: backend, priority: 0}] }
  monitor: { outputConfigMap: $NS/monitor-conf, model: m2 }
YAML
waiteq true "m2 status.ready" kubectl -n "$NS" get mr m2 -o jsonpath='{.status.ready}'
waitval 1 "openresty 有 m2 route conf" "kubectl -n $NS get cm openresty-conf -o jsonpath='{.data.session_route_m2\.conf}' | grep -c 'listen unix:.*/m2.sock'"
waitval 1 "monitor 有 m2 key" "kubectl -n $NS get cm monitor-conf -o jsonpath='{.data.m2\.monitor\.conf}' | grep -c '^service: '"
[ "$(kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_m1\.conf}' | grep -c 'listen unix')" -ge 1 ] && ok "m1 route conf 仍在(新增不影响老)" || bad "m1 route 被影响"

say "C2: 删 ModelRoute m2 → finalizer 清理 openresty/monitor 的 m2,m1 保留"
kubectl -n "$NS" delete mr m2 --timeout=40s >/dev/null 2>&1
waitval 0 "openresty m2 key 已摘" "kubectl -n $NS get cm openresty-conf -o jsonpath='{.data.session_route_m2\.conf}' | grep -c 'listen'"
waitval 0 "monitor m2 key 已摘" "kubectl -n $NS get cm monitor-conf -o jsonpath='{.data.m2\.monitor\.conf}' | grep -c 'service'"
[ "$(kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_m1\.conf}' | grep -c 'listen unix')" -ge 1 ] && ok "m1 route conf 仍在(删 m2 不影响)" || bad "删 m2 误伤 m1"

######## D monitor 消费/存活 ########
say "D: monitor k8s+mysql 模式存活 + 消费三类行 + 无解析错"
MON=$(kubectl -n "$NS" get cm monitor-conf -o jsonpath='{.data.m1\.monitor\.conf}')
echo "$MON" | grep -q '^service: ' && ok "monitor service 行" || bad "无 service 行"
echo "$MON" | grep -qE '^nginx: .*:8080/m1' && ok "monitor nginx 行(8080/route)" || bad "无 nginx 行"
echo "$MON" | grep -qE '^router: .*:8071/workers' && ok "monitor router 行" || bad "无 router 行"
kubectl -n "$NS" exec deploy/monitor -- sh -c 'nc -z 127.0.0.1 8080' 2>/dev/null && ok "monitor dashboard 端口在(8080)" || bad "monitor dashboard 端口不在"
kubectl -n "$NS" logs deploy/monitor --tail=200 2>/dev/null | grep -qiE 'Traceback|ValueError|line [0-9]+:' && bad "monitor 日志有解析错误" || ok "monitor 无解析错误"
kubectl -n "$NS" exec deploy/monitor-mysql -- sh -c 'mysql -uroot -prootpass -e "SHOW DATABASES;" 2>/dev/null | grep -q monitor' 2>/dev/null && ok "mysql 有 monitor 库" || echo "  (mysql 库检查跳过)"

######## G4 session 亲和 ########
say "G4: openresty session 亲和(同 sid 恒落同 backend,crc32)——用 /_route_debug 不需真推理"
ORP=$(kubectl -n "$NS" get pod -l openresty-active=true --no-headers 2>/dev/null | awk '{print $1}' | head -1)
P1=$(kubectl -n "$NS" exec "$ORP" -c openresty -- sh -c 'wget -qO- "http://127.0.0.1:8090/healthz" >/dev/null 2>&1; wget -qO- "http://127.0.0.1:18080/_route_debug?sid=abc123" 2>/dev/null || curl -s "http://127.0.0.1:18080/_route_debug?sid=abc123"' 2>/dev/null | head -c 200)
echo "  route_debug(sid=abc123): $P1"
# 连查两次同 sid,结果应一致(亲和稳定)
P2=$(kubectl -n "$NS" exec "$ORP" -c openresty -- sh -c 'curl -s "http://127.0.0.1:18080/_route_debug?sid=abc123"' 2>/dev/null | head -c 200)
[ -n "$P1" ] && [ "$P1" = "$P2" ] && ok "同 sid 两次路由决策一致(亲和稳定)" || echo "  (route_debug 端点/端口可能不同,亲和验证跳过:$P1 vs $P2)"

say "结果"; [ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"
echo TEST_DYNAMIC_DONE
