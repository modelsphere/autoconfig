#!/usr/bin/env bash
# 真 kimi LWS 端到端:autoconfig 整套(openresty+cart+monitor chart)接【已部署的真 kimi-k26 LWS】做后端。
# 前置:kimi ns 里 kimi-k26 LWS + kimi-k26-leader Service 已就绪(leader /v1/models=200)。
# 验证:autoconfig 发现 kimi leader→CART workers/openresty peers/monitor service+nginx+router→
#       真发一条 /v1/chat/completions 经 openresty→CART→kimi 拿真实回答。
# 依赖同目录:modelroutes.yaml(CRD)、controller.yaml、charts/{openresty,cart,monitor}。
#   IMG_TAG=0.3.7 bash real_kimi_e2e.sh [--keep]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=${NS:-kimi}                      # 与 kimi LWS 同 ns(discovery 同 ns)
CTRL_NS=${CTRL_NS:-autoconfig}
TAG=${IMG_TAG:-0.3.7}
CHARTS=${CHARTS:-$HERE/charts}
KIMI_SVC=${KIMI_SVC:-kimi-k26-leader}
MODEL=${MODEL:-kimi-k2.6}
LISTEN=${LISTEN:-18080}
AUTH_KEY=${AUTH_KEY:-REDACTED-SEE-DEPLOY-DOCS}   # openresty 入口鉴权 key(Bearer);kimi 后端无鉴权
AC=harbor.4pd.io/hardcore-tech/autoconfig
ACR=harbor.4pd.io/hardcore-tech/autoconfig-reload:$TAG
OR_IMG=${OR_IMG:-harbor.4pd.io/hardcore-tech/llm-openresty:0.2.0-routes}
CART_IMG=${CART_IMG:-harbor.4pd.io/hardcore-tech/cache_aware_router:v0.6.0}
MON_IMG=${MON_IMG:-harbor.4pd.io/hardcore-tech/llm-monitor:0.1.0}
ALP=${ALP:-harbor.4pd.io/hardcore-tech/python:3.12-alpine}
KEEP=0; [ "${1:-}" = "--keep" ] && KEEP=1
FAIL=0
say(){ echo -e "\n=== $* ==="; }
ok(){ echo "  PASS: $*"; }
bad(){ echo "  FAIL: $*"; FAIL=1; }
waiteq(){ local want="$1" desc="$2"; shift 2; local i got; for i in $(seq 1 60); do got="$("$@" 2>/dev/null)"; [ "$got" = "$want" ] && { ok "$desc = $want"; return 0; }; sleep 3; done; bad "$desc: got '$got' want '$want'"; return 1; }

cleanup(){ [ "$KEEP" = 1 ] && { echo "(--keep:保留 autoconfig 栈 + kimi)"; return; }
  say cleanup
  helm -n "$NS" uninstall openresty cart monitor 2>/dev/null
  kubectl -n "$NS" delete mr glm-kimi 2>/dev/null
  kubectl -n "$NS" delete cm base-cart openresty-conf cart-config monitor-conf 2>/dev/null
  kubectl -n "$CTRL_NS" delete -f "$HERE/controller.yaml" --wait=false 2>/dev/null
  echo "(kimi LWS 保留,不动)"
}
trap cleanup EXIT

# ---------- 0) 前置:kimi leader Ready ----------
say "等 kimi-k26 leader Ready(/v1/models=200)"
kubectl -n "$NS" wait --for=condition=Ready pod -l app=kimi-k26,role=leader --timeout=900s || { bad "kimi leader 未就绪"; kubectl -n "$NS" get pods; exit 1; }
ok "kimi leader Ready"

# ---------- 1) CRD + autoconfig controller ----------
say "install CRD + autoconfig controller ($AC:$TAG)"
kubectl apply -f "$HERE/modelroutes.yaml"
kubectl create ns "$CTRL_NS" --dry-run=client -o yaml | kubectl apply -f -
sed "s#image: $AC:.*#image: $AC:$TAG#" "$HERE/controller.yaml" | kubectl apply -f -
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=150s || { bad "controller 未就绪"; exit 1; }
ok "controller 就绪"

# ---------- 2) base-cart + helm 装 3 组件 ----------
say "base-cart + helm install openresty/cart/monitor(kimi ns)"
kubectl -n "$NS" apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata: { name: base-cart }
data:
  config.base.yaml: |
    server: { host: "0.0.0.0", port: 8071 }
    cache: { threshold: 0.3 }
    health: { endpoint: "/health", interval_secs: 10 }
YAML
helm -n "$NS" upgrade --install openresty "$CHARTS/openresty" \
  --set fullnameOverride=openresty --set image.repository="${OR_IMG%:*}" --set image.tag="${OR_IMG##*:}" \
  --set reload.image="$ACR" >/dev/null && ok "openresty chart" || bad "openresty chart 装失败"
helm -n "$NS" upgrade --install cart "$CHARTS/cart" \
  --set fullnameOverride=cart --set image.repository="${CART_IMG%:*}" --set image.tag="${CART_IMG##*:}" \
  --set reload.image="$ACR" >/dev/null && ok "cart chart" || bad "cart chart 装失败"
helm -n "$NS" upgrade --install monitor "$CHARTS/monitor" \
  --set fullnameOverride=monitor --set image.repository="${MON_IMG%:*}" --set image.tag="${MON_IMG##*:}" \
  >/dev/null && ok "monitor chart" || bad "monitor chart 装失败"

# ---------- 3) ModelRoute:discovery = 真 kimi leader ----------
say "apply ModelRoute(discovery=$KIMI_SVC:8050,真 kimi 后端)"
kubectl -n "$NS" apply -f - <<YAML
apiVersion: routing.gpucluster.io/v1alpha1
kind: ModelRoute
metadata: { name: glm-kimi }
spec:
  discovery: { service: $KIMI_SVC, port: 8050 }
  cart: { service: cart, port: 8071, outputConfigMap: $NS/cart-config, baseConfigRef: { name: base-cart, key: config.base.yaml }, maxLoad: 20 }
  openresty:
    route: kimi
    listen: $LISTEN
    outputConfigMap: $NS/openresty-conf
    sources: [{ use: cart, priority: 1, maxConcurrency: 180 }, { use: backend, priority: 0 }]
  monitor:
    outputConfigMap: $NS/monitor-conf
    model: $MODEL
    gpuType: A100
    nginx: { service: openresty, port: $LISTEN }
YAML
waiteq true "status.ready" kubectl -n "$NS" get mr glm-kimi -o jsonpath='{.status.ready}'
waiteq 1 "status.backends(=kimi leader)" kubectl -n "$NS" get mr glm-kimi -o jsonpath='{.status.backends}'
kubectl -n "$NS" rollout status deploy/cart --timeout=180s || { bad "cart 未就绪"; kubectl -n "$NS" logs deploy/cart -c cart --tail=20; }
kubectl -n "$NS" rollout status deploy/openresty --timeout=150s || bad "openresty 未就绪"
kubectl -n "$NS" rollout status deploy/monitor --timeout=150s || bad "monitor 未就绪"
waiteq 1 "status.cartPeers" kubectl -n "$NS" get mr glm-kimi -o jsonpath='{.status.cartPeers}'

# ---------- 4) 验证真消费(配置层)----------
say "验证配置:CART workers=kimi leader / openresty peers / monitor service+nginx+router"
LEADER_IP=$(kubectl -n "$NS" get pod -l app=kimi-k26,role=leader -o jsonpath='{.items[0].status.podIP}')
echo "kimi leader podIP=$LEADER_IP"
CART=$(kubectl -n "$NS" get cm cart-config -o jsonpath='{.data.config\.yaml}')
echo "$CART" | grep -q "http://$LEADER_IP:8050" && ok "CART workers = kimi leader" || bad "CART workers 不含 kimi leader"
OR=$(kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_kimi\.conf}')
echo "$OR" | grep -qE '"cart-0", 1, 180' && ok "openresty CART 优先 peer" || bad "openresty 无 CART peer"
echo "$OR" | grep -q "$LEADER_IP\", 8050, \"backend-0\"" && ok "openresty 后端兜底=kimi leader" || bad "openresty 后端 peer 不对"
MON=$(kubectl -n "$NS" get cm monitor-conf -o jsonpath='{.data.glm-kimi\.monitor\.conf}')
echo "--- monitor glm-kimi.monitor.conf ---"; echo "$MON"
echo "$MON" | grep -qE "^service: glm-kimi-0 \| http://$LEADER_IP:8050 \| $MODEL \| A100$" && ok "monitor service=kimi leader" || bad "monitor service 不对"
echo "$MON" | grep -qE '^nginx: openresty-0 \|' && ok "monitor nginx 行" || bad "monitor 无 nginx 行"
echo "$MON" | grep -qE '^router: glm-kimi-router-0 \|.+/workers$' && ok "monitor router 行" || bad "monitor 无 router 行"

# ---------- 5) 真消费:发一条 /v1/chat/completions 经 openresty→CART→kimi ----------
say "真推理:openresty:$LISTEN → CART → kimi(拿真实回答)"
# 直连 kimi leader 先确认后端本身能出词(隔离 openresty/CART 问题)
DIRECT=$(kubectl -n "$NS" exec -i deploy/monitor -- python3 - "$MODEL" "http://$LEADER_IP:8050" <<'PYEOF'
import sys,urllib.request,json
model,base=sys.argv[1],sys.argv[2]
req={"model":model,"messages":[{"role":"user","content":"用一句话说明你是谁"}],"max_tokens":64,"temperature":0,"chat_template_kwargs":{"thinking":False}}
r=urllib.request.Request(base+"/v1/chat/completions",data=json.dumps(req).encode(),headers={"Content-Type":"application/json"})
try: print(urllib.request.urlopen(r,timeout=120).read().decode())
except Exception as e: print("ERR",e)
PYEOF
)
echo "--- 直连 kimi leader 回答(截断)---"; echo "$DIRECT" | head -c 600; echo
echo "$DIRECT" | grep -q '"content"' && ok "kimi 后端直连出词" || bad "kimi 后端直连未出词"
# 经 openresty 入口(→CART→kimi),带 Bearer 鉴权
RESP=$(kubectl -n "$NS" exec -i deploy/monitor -- python3 - "$MODEL" "http://openresty:$LISTEN" "$AUTH_KEY" <<'PYEOF'
import sys,urllib.request,json
model,base,key=sys.argv[1],sys.argv[2],sys.argv[3]
req={"model":model,"messages":[{"role":"user","content":"用一句话说明你是谁"}],"max_tokens":64,"temperature":0,"chat_template_kwargs":{"thinking":False}}
r=urllib.request.Request(base+"/v1/chat/completions",data=json.dumps(req).encode(),headers={"Content-Type":"application/json","Authorization":"Bearer "+key})
try: print(urllib.request.urlopen(r,timeout=120).read().decode())
except Exception as e: print("ERR",e)
PYEOF
)
echo "--- openresty 入口回答(截断)---"; echo "$RESP" | head -c 800; echo
echo "$RESP" | grep -q '"content"' && ok "经 openresty→CART→kimi 拿到真实 completion" || bad "openresty 入口未拿到回答"

say "结果"
[ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"
exit $FAIL
