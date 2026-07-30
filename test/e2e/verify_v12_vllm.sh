#!/usr/bin/env bash
# 验证 0.3.14 三改动,用现成的真 vllm(e2e-test/vllm-mock-vllm-svc,opt-125m,跨 ns 发现,不动 GPU):
#   Q5 sources→peers、Q4 values 任意 key、Q1 monitor nginx 每端口(port=openresty.listen、name=model)
#   + backend maxConcurrency、+ 真推理 openresty→CART→vllm。
# 复用 kimi ns 里已装的 openresty/cart/monitor(controller 已 0.3.14)。用法:bash verify_v12_vllm.sh
set -uo pipefail
NS=${NS:-kimi}
BACKEND_SVC=${BACKEND_SVC:-e2e-test/vllm-mock-vllm-svc}
MODEL=${MODEL:-facebook/opt-125m}   # 真实服务模型名(推理用)
MODEL_LABEL=${MODEL_LABEL:-opt-125m}  # monitor 显示名
LISTEN=${LISTEN:-18080}
AUTH_KEY=${AUTH_KEY:-REDACTED-SEE-DEPLOY-DOCS}
FAIL=0; ok(){ echo "  PASS: $*"; }; bad(){ echo "  FAIL: $*"; FAIL=1; }
waiteq(){ local want="$1" d="$2"; shift 2; local i g; for i in $(seq 1 40); do g="$("$@" 2>/dev/null)"; [ "$g" = "$want" ] && { ok "$d=$want"; return; }; sleep 3; done; bad "$d: got '$g' want '$want'"; }

echo "=== 换 ModelRoute:opt(跨 ns 发现 vllm-mock + 新语法)==="
kubectl -n "$NS" delete mr glm-kimi opt 2>/dev/null; sleep 3
kubectl -n "$NS" apply -f - <<YAML
apiVersion: routing.gpucluster.io/v1alpha1
kind: ModelRoute
metadata: { name: opt }
spec:
  discovery: { service: $BACKEND_SVC }        # 跨 ns,port 自动=8000
  cart: { service: cart, outputConfigMap: $NS/cart-config, maxLoad: 20 }
  openresty:
    route: opt
    listen: $LISTEN
    outputConfigMap: $NS/openresty-conf
    values: { ttft_limit_ms: "60000", zz_custom_tunable: "7" }   # 任意 key:自定义的也渲染
    peers:
      - { use: cart,    priority: 1, maxConcurrencyFromBackend: true }   # 动态=后端并发 × 后端数
      - { use: backend, priority: 0, maxConcurrency: 120 }               # 单实例 120(vllm-mock 1 后端 → cart=120)
  monitor:
    outputConfigMap: $NS/monitor-conf
    model: $MODEL_LABEL
    gpuType: A100
    nginx: { service: openresty }              # 无 port → 用 openresty.listen($LISTEN)
YAML
waiteq true "status.ready" kubectl -n "$NS" get mr opt -o jsonpath='{.status.ready}'
waiteq 1 "status.backends(vllm-mock,跨 ns)" kubectl -n "$NS" get mr opt -o jsonpath='{.status.backends}'
waiteq 1 "status.cartPeers" kubectl -n "$NS" get mr opt -o jsonpath='{.status.cartPeers}'

echo "=== Q5 peers + Q4 任意 values(openresty-conf)==="
OR=$(kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_opt\.conf}')
echo "--- session_route_opt.conf ---"; echo "$OR" | grep -E 'peers|cart-0|backend-0|ttft|zz_custom'
echo "$OR" | grep -qE '"cart-0", 1, 120' && ok "peers:CART 动态并发=后端120 × 1 后端=120" || bad "CART 动态并发不对"
echo "$OR" | grep -qE '"backend-0", 0, 120' && ok "peers:backend maxConcurrency=120(第5元素)" || bad "backend maxConc 不对"
echo "$OR" | grep -q 'ttft_limit_ms = 60000' && ok "values:ttft_limit_ms 渲染" || bad "ttft 未渲染"
echo "$OR" | grep -q 'zz_custom_tunable = 7' && ok "values:自定义 key 也原样渲染(无需改代码)" || bad "自定义 value 未渲染"

echo "=== Q1 monitor nginx 每端口(monitor-conf)==="
# 换路由后 openresty active 标签需重新稳定(路由 conf 传播+reload)→ 等 nginx 行出现
for i in $(seq 1 30); do kubectl -n "$NS" get cm monitor-conf -o jsonpath='{.data.opt\.monitor\.conf}' 2>/dev/null | grep -q '^nginx:' && break; sleep 4; done
MON=$(kubectl -n "$NS" get cm monitor-conf -o jsonpath='{.data.opt\.monitor\.conf}')
echo "--- opt.monitor.conf ---"; echo "$MON"
echo "$MON" | grep -qE "^nginx: $MODEL_LABEL-0 \| http://.+:$LISTEN\$" && ok "nginx:name=model、port=openresty.listen($LISTEN)" || bad "nginx 行不对"
echo "$MON" | grep -qE "^service: opt-0 \| http://.+:8000 \| $MODEL_LABEL \| A100\$" && ok "service:后端 port 自动=8000(EndpointSlice 取)" || bad "service 行不对"

echo "=== 真推理 openresty:$LISTEN → CART → vllm-mock(opt-125m)==="
# 等挂载传播 + reload
sleep 8
RESP=$(kubectl -n "$NS" exec -i deploy/monitor -- python3 - "$MODEL" "http://openresty:$LISTEN" "$AUTH_KEY" <<'PYEOF'
import sys,urllib.request,json
model,base,key=sys.argv[1],sys.argv[2],sys.argv[3]
req={"model":model,"prompt":"Hello, world","max_tokens":8,"temperature":0}
r=urllib.request.Request(base+"/v1/completions",data=json.dumps(req).encode(),headers={"Content-Type":"application/json","Authorization":"Bearer "+key})
for i in range(12):
    try: print(urllib.request.urlopen(r,timeout=20).read().decode()); break
    except Exception as e:
        import time; last=e; time.sleep(5)
else: print("ERR",last)
PYEOF
)
echo "--- openresty 入口回答(截断)---"; echo "$RESP" | head -c 500; echo
echo "$RESP" | grep -qE '"text"|"choices"' && ok "经 openresty→CART→vllm 拿到真实 completion" || bad "真推理未拿到回答"

echo "=== 结果 ==="; [ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"; exit $FAIL
