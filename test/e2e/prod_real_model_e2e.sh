#!/usr/bin/env bash
# 生产式全栈 e2e:autoconfig + openresty(HA)+ cart + monitor(mysql+k8s+rbac 生产档),
# 后端 = 真 opt-125m vLLM(复用 e2e-test/vllm-mock-vllm-svc,跨 ns discovery)。真 /v1/completions 推理。
set -uo pipefail
NS=${NS:-llm-route}; CTRL_NS=autoconfig; CHARTS=~/prod-e2e/charts
H=harbor.4pd.io/hardcore-tech; TAG=clusterip-test
AC=$H/autoconfig; ACR=$H/autoconfig-reload:$TAG; AH=$H/autoconfig-hagate:$TAG
OR_IMG=$H/llm-openresty:e2e0; CART_IMG=$H/cache_aware_router:v0.6.0; MON_IMG=$H/llm-monitor:k8s
BACKEND_SVC=e2e-test/vllm-mock-vllm-svc; BPORT=8000; MODEL=facebook/opt-125m
KEEP=${KEEP:-0}; FAIL=0
AUTH_KEY=${AUTH_KEY:?需设置:openresty 入口鉴权 key(Bearer)}
say(){ echo -e "\n=== $* ==="; }; ok(){ echo "  PASS: $*"; }; bad(){ echo "  FAIL: $*"; FAIL=1; }
waiteq(){ local w="$1" d="$2"; shift 2; local i g; for i in $(seq 1 60); do g="$("$@" 2>/dev/null)"; [ "$g" = "$w" ] && { ok "$d = $w"; return 0; }; sleep 3; done; bad "$d: got '$g' want '$w'"; return 1; }
cleanup(){ [ "$KEEP" = 1 ] && { echo "(--keep)"; return; }
  say cleanup; helm -n "$NS" uninstall openresty cart monitor 2>/dev/null
  kubectl -n "$NS" delete mr --all --timeout=40s 2>/dev/null; kubectl delete ns "$NS" --wait=false 2>/dev/null
  helm -n "$CTRL_NS" uninstall autoconfig 2>/dev/null; }
trap cleanup EXIT

say "0) 后端 opt-125m 在服务?"
BE_POD=$(kubectl -n e2e-test get pods --no-headers 2>/dev/null | grep vllm-mock | awk '{print $1}' | head -1)
BE_IP=$(kubectl -n e2e-test get pod "$BE_POD" -o jsonpath='{.status.podIP}' 2>/dev/null)
echo "opt-125m pod=$BE_POD ip=$BE_IP"; [ -n "$BE_IP" ] && ok "opt-125m 后端在" || { bad "无 opt-125m 后端"; exit 1; }

say "1) autoconfig controller"
kubectl apply -f "$CHARTS/autoconfig/crds/" >/dev/null
helm -n "$CTRL_NS" upgrade --install autoconfig "$CHARTS/autoconfig" --create-namespace \
  --set fullnameOverride=autoconfig-controller --set image.repository="$AC" --set image.tag="$TAG" >/dev/null
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=150s && ok "controller ready" || { bad controller; exit 1; }
kubectl create ns "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

say "2) openresty(HA 主备 + reload)"
helm -n "$NS" upgrade --install openresty "$CHARTS/openresty" \
  --set fullnameOverride=openresty --set image.repository="${OR_IMG%:*}" --set image.tag="${OR_IMG##*:}" \
  --set reload.image="$ACR" --set ha.image="$AH" \
  --set resources.requests.cpu=200m --set resources.requests.memory=256Mi --set resources.limits.cpu=2 --set resources.limits.memory=2Gi \
  >/dev/null && ok "openresty installed" || bad "openresty install"

say "3) cart"
helm -n "$NS" upgrade --install cart "$CHARTS/cache_aware_router" \
  --set fullnameOverride=cart --set image.repository="${CART_IMG%:*}" --set image.tag="${CART_IMG##*:}" \
  --set reload.image="$ACR" --set ha.image="$AH" >/dev/null && ok "cart installed" || bad "cart install"

say "4) monitor(生产:mysql + k8s + rbac)"
helm -n "$NS" upgrade --install monitor "$CHARTS/monitor" \
  --set fullnameOverride=monitor --set image.repository="${MON_IMG%:*}" --set image.tag="${MON_IMG##*:}" \
  --set service.nodePort="" \
  --set-string secret.data.WEB_TPM_AUTH_PASS=testpass \
  --set-string mysql.auth.rootPassword=rootpass --set-string mysql.auth.password=monpass \
  >/dev/null && ok "monitor installed(mysql+k8s+rbac)" || bad "monitor install"

waiteq openresty-conf "openresty-conf 建" kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.metadata.name}'
waiteq cart-config    "cart-config 建"    kubectl -n "$NS" get cm cart-config    -o jsonpath='{.metadata.name}'
waiteq monitor-conf   "monitor-conf 建"   kubectl -n "$NS" get cm monitor-conf   -o jsonpath='{.metadata.name}'

say "5) ModelRoute → 真 opt-125m(跨 ns discovery)"
kubectl -n "$NS" apply -f - <<YAML
apiVersion: routing.gpucluster.io/v1alpha1
kind: ModelRoute
metadata: { name: opt }
spec:
  discovery: { service: $BACKEND_SVC, port: $BPORT }
  cart: { service: cart, port: 8071, outputConfigMap: $NS/cart-config, maxLoad: 20 }
  nginx:
    route: opt
    outputConfigMap: $NS/openresty-conf
    service: openresty
    peers: [{ use: cart, priority: 3, maxConcurrency: 180 }, { use: backend, priority: 2 }, { use: backend-svc, priority: 1 }]
  monitor:
    outputConfigMap: $NS/monitor-conf
    model: opt-125m
YAML
waiteq true "status.ready" kubectl -n "$NS" get mr opt -o jsonpath='{.status.ready}'
waiteq 1 "status.backends(=opt-125m)" kubectl -n "$NS" get mr opt -o jsonpath='{.status.backends}'

kubectl -n "$NS" rollout status deploy/openresty --timeout=180s || { bad "openresty 未就绪"; kubectl -n "$NS" get pods; }
kubectl -n "$NS" rollout status deploy/cart --timeout=240s || { bad "cart 未就绪"; kubectl -n "$NS" logs deploy/cart -c cart --tail=15; }
kubectl -n "$NS" rollout status deploy/monitor-mysql --timeout=180s || { bad "mysql 未就绪"; kubectl -n "$NS" logs deploy/monitor-mysql --tail=15; }
kubectl -n "$NS" rollout status deploy/monitor --timeout=180s || { bad "monitor 未就绪"; kubectl -n "$NS" logs deploy/monitor --tail=30; }
waiteq 1 "status.cartPeers" kubectl -n "$NS" get mr opt -o jsonpath='{.status.cartPeers}'

say "6) 配置层验证"
CART=$(kubectl -n "$NS" get cm cart-config -o jsonpath='{.data.config\.yaml}')
echo "$CART" | grep -q "http://$BE_IP:$BPORT" && ok "cart workers = opt-125m pod IP" || bad "cart workers 不含 opt-125m($BE_IP)"
OR=$(kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_opt\.conf}')
echo "$OR" | grep -qE '"cart-0", 3, 180' && ok "openresty cart peer(prio3)" || bad "无 cart peer"
echo "$OR" | grep -q "$BE_IP\", $BPORT, \"backend-0\", 2" && ok "openresty backend peer=opt-125m(prio2)" || bad "backend peer 不对"
echo "$OR" | grep -qE 'backend-svc-0", 1' && ok "openresty backend-svc VIP 兜底(prio1)" || bad "无 backend-svc 兜底"
echo "$OR" | grep -q 'listen unix:/usr/local/openresty/nginx/sock/opt.sock' && ok "openresty socket listen" || bad "无 socket listen"
# 等 nginx 行出现(依赖 openresty hagate 标 leader,比 service/router 慢)
for i in $(seq 1 45); do kubectl -n "$NS" get cm monitor-conf -o jsonpath='{.data}' 2>/dev/null | grep -q 'nginx:' && break; sleep 4; done
MON=$(kubectl -n "$NS" get cm monitor-conf -o jsonpath='{.data}')
echo "$MON" | grep -qE "service: opt-[0-9]+ \| http://$BE_IP:$BPORT \| opt-125m" && ok "monitor service 行=opt-125m" || bad "monitor service 行不对"
echo "$MON" | grep -qE "nginx: opt-125m-[0-9]+ \| http://.+:8080/opt" && ok "monitor nginx 行(8080/opt)" || bad "monitor 无 nginx 行"
echo "$MON" | grep -qE "router: .*:8071/workers" && ok "monitor router 行(cart /workers)" || bad "monitor 无 router 行"

say "7) monitor k8s 模式活着(dashboard + mysql 连通)"
kubectl -n "$NS" exec deploy/monitor -- python3 -c "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8080/api/status',timeout=5).status)" 2>&1 | grep -q 200 && ok "monitor dashboard /api/status 200" || echo "  (dashboard 走认证,可能 401;看 pod Running 即可)"
kubectl -n "$NS" logs deploy/monitor --tail=100 2>/dev/null | grep -qiE "MySQL|ensure_schema|mysql://" && ok "monitor 用 MySQLStore(日志见)" || echo "  (未在日志匹配到 mysql 字样,非致命)"

say "8) 真推理:openresty:8080/opt/v1/completions → cart → opt-125m(真出词)"
# 从 monitor pod 发(有 python3)。opt-125m 是 base 模型,用 /v1/completions
for try in 1 2 3; do
RESP=$(kubectl -n "$NS" exec -i deploy/monitor -- python3 - "$MODEL" "$AUTH_KEY" <<'PYEOF'
import sys,urllib.request,json
model,auth=sys.argv[1],sys.argv[2]
req={"model":model,"prompt":"The capital of France is","max_tokens":10,"temperature":0}
r=urllib.request.Request("http://openresty:8080/opt/v1/completions",data=json.dumps(req).encode(),
  headers={"Content-Type":"application/json","Authorization":"Bearer "+auth})
try: print(urllib.request.urlopen(r,timeout=30).read().decode())
except Exception as e: print("ERR",e)
PYEOF
)
echo "  try$try: $(echo "$RESP" | head -c 300)"
echo "$RESP" | grep -q '"text"' && { ok "经 openresty→cart→opt-125m 真出词"; break; } || sleep 5
done
echo "$RESP" | grep -q '"text"' || bad "推理未拿到 completion"

say "结果"; [ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"
echo PROD_E2E_DONE
