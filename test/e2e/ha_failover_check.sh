#!/usr/bin/env bash
# master-standby(ha-gate)验证:openresty/cart 各 2 副本但只 1 个进 Service;删 leader → standby ~接管。
# 前提:real_kimi_e2e.sh --keep 已把栈跑起来(kimi ns)。用法:bash ha_failover_check.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=${NS:-kimi}
CHARTS=${CHARTS:-$HERE/charts}
OR_IMG=${OR_IMG:-harbor.4pd.io/hardcore-tech/llm-openresty:0.2.0-routes}
CART_IMG=${CART_IMG:-harbor.4pd.io/hardcore-tech/cache_aware_router:v0.6.0}
ACR=${ACR:-harbor.4pd.io/hardcore-tech/autoconfig-reload:0.3.11}
AUTH_KEY=${AUTH_KEY:-REDACTED-SEE-DEPLOY-DOCS}
FAIL=0; say(){ echo -e "\n=== $* ==="; }; ok(){ echo "  PASS: $*"; }; bad(){ echo "  FAIL: $*"; FAIL=1; }

# ready endpoint 数(Service 背后 Ready 端点)
eps(){ kubectl -n "$NS" get endpointslice -l kubernetes.io/service-name="$1" -o jsonpath='{range .items[*].endpoints[*]}{.conditions.ready}{"\n"}{end}' 2>/dev/null | grep -c true; }
leaderpod(){ kubectl -n "$NS" get lease "$1" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null; }

say "helm upgrade openresty/cart → 0.3.11(HA:replicas2 + ha-gate)"
helm -n "$NS" upgrade --install openresty "$CHARTS/openresty" --set fullnameOverride=openresty \
  --set image.repository="${OR_IMG%:*}" --set image.tag="${OR_IMG##*:}" --set reload.image="$ACR" >/dev/null && ok "openresty upgraded" || bad "openresty upgrade 失败"
helm -n "$NS" upgrade --install cart "$CHARTS/cart" --set fullnameOverride=cart \
  --set image.repository="${CART_IMG%:*}" --set image.tag="${CART_IMG##*:}" --set reload.image="$ACR" >/dev/null && ok "cart upgraded" || bad "cart upgrade 失败"

say "等 rollout(2 副本)"
kubectl -n "$NS" rollout status deploy/openresty --timeout=180s || bad "openresty rollout"
kubectl -n "$NS" rollout status deploy/cart --timeout=180s || bad "cart rollout"

say "等收敛(ha-gate 打 active 标签需 app 端口可服务;openresty 要等路由 conf 传播 ~60s)"
# 等每个 svc 恰好 1 个 active 端点(最多 ~150s)
for svc in openresty cart; do
  for i in $(seq 1 50); do [ "$(eps $svc)" = 1 ] && break; sleep 3; done
done
runpods(){ kubectl -n "$NS" get pods -l app.kubernetes.io/name=$1 --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' '; }

say "稳态:2 副本(都 Ready/健康)但 Service 只 1 个 active 端点(主备)"
for svc in openresty cart; do
  n=$(runpods $svc); e=$(eps $svc)
  echo "  $svc: running-pods=$n, service-endpoints=$e, lease-holder=$(leaderpod ${svc}-ha)"
  [ "$n" = 2 ] && ok "$svc 2 副本 Running" || bad "$svc Running 副本=$n"
  [ "$e" = 1 ] && ok "$svc Service 只 1 个 active 端点(单 active)" || bad "$svc active 端点=$e(期望 1)"
done

say "推理正常(经 openresty 入口)"
infer(){ kubectl -n "$NS" exec -i deploy/monitor -- python3 - "$AUTH_KEY" <<'PYEOF'
import sys,urllib.request,json
key=sys.argv[1]
req={"model":"kimi-k2.6","messages":[{"role":"user","content":"1+1=?只答数字"}],"max_tokens":8,"temperature":0,"chat_template_kwargs":{"thinking":False}}
r=urllib.request.Request("http://openresty:18080/v1/chat/completions",data=json.dumps(req).encode(),headers={"Content-Type":"application/json","Authorization":"Bearer "+key})
try: print(urllib.request.urlopen(r,timeout=60).read().decode())
except Exception as e: print("ERR",e)
PYEOF
}
infer | grep -q '"content"' && ok "failover 前推理 OK" || bad "failover 前推理失败"

say "failover:删 openresty leader pod,测切换耗时(计划内 SIGTERM 路径)"
LEADER=$(leaderpod openresty-ha)
echo "  当前 leader pod=$LEADER"
t0=$(date +%s%3N)
kubectl -n "$NS" delete pod "$LEADER" --wait=false 2>/dev/null
# 紧密轮询 Service active 端点:记录跌到 0 与回到 1 的时刻
drop=""; rec=""
for i in $(seq 1 120); do
  e=$(eps openresty); now=$(date +%s%3N)
  [ -z "$drop" ] && [ "$e" = 0 ] && drop=$now
  [ "$e" = 1 ] && { [ -n "$drop" ] && { rec=$now; break; }; [ $((now-t0)) -gt 4000 ] && { rec=$now; break; }; }
done
NEW=$(leaderpod openresty-ha)
[ -n "$NEW" ] && [ "$NEW" != "$LEADER" ] && ok "standby 接管 leader:$LEADER → $NEW" || bad "leader 未切换(仍 $NEW)"
if [ -n "$rec" ]; then
  if [ -n "$drop" ]; then echo "  ⏱ 切换:delete→恢复 = $((rec-t0))ms,其中 Service 0 端点窗口 = $((rec-drop))ms"
  else echo "  ⏱ 切换:全程未跌到 0 端点(无缝)"; fi
fi
[ "$(eps openresty)" = 1 ] && ok "切换后 Service 恰好 1 个 active 端点" || bad "切换后端点=$(eps openresty)"

say "failover 后推理仍正常"
for i in $(seq 1 20); do infer | grep -q '"content"' && { ok "failover 后推理 OK"; break; }; sleep 3; [ "$i" = 20 ] && bad "failover 后推理失败"; done

say "结果"; [ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"; exit $FAIL
