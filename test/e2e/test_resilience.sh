#!/usr/bin/env bash
# Phase 2 韧性/失败:复用 test_dynamic(KEEP=1)留下的栈(llm-route:m1 + mock be + openresty/cart/monitor)。破坏性。
# F8 fail-safe、F4/F5 主备 failover、F6/F7 组件/mysql fail、F1/F2/F3 operator fail + backend-svc 兜底、E1 restart 权限。
set -uo pipefail
NS=llm-route; CTRL_NS=autoconfig; FAIL=0
say(){ echo -e "\n=== $* ==="; }; ok(){ echo "  PASS: $*"; }; bad(){ echo "  FAIL: $*"; FAIL=1; }
waitcond(){ local d="$1"; shift; local i; for i in $(seq 1 45); do eval "$1" >/dev/null 2>&1 && { ok "$d"; return 0; }; sleep 3; done; bad "$d(超时)"; return 1; }
cw(){ kubectl -n "$NS" get cm cart-config -o jsonpath='{.data.config\.yaml}' 2>/dev/null | grep -c 'url:'; }
active(){ kubectl -n "$NS" get pod -l "$1-active=true" --no-headers 2>/dev/null | awk '{print $1}' | head -1; }
eps(){ kubectl -n "$NS" get endpoints "$1" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null; }

kubectl -n "$NS" get mr m1 >/dev/null 2>&1 || { echo "需先跑 test_dynamic KEEP=1"; exit 1; }

######## F8 fail-safe:后端全下不写空 ########
say "F8: 后端 scale 1→0 → autoconfig fail-safe 绝不写空(cart workers 保留上次,不清)"
before=$(cw); echo "  scale 前 cart workers=$before"
kubectl -n "$NS" scale deploy/be --replicas=0 >/dev/null; kubectl -n "$NS" rollout status deploy/be --timeout=60s >/dev/null 2>&1
sleep 20   # 给 autoconfig 反应时间(它应「不写」)
after=$(cw); echo "  scale 0 后 cart workers=$after"
[ "$after" -ge 1 ] && ok "cart workers 未被清空(fail-safe,保留 $after)" || bad "cart workers 被清空($after)——fail-safe 失效"
kubectl -n "$NS" get mr m1 -o jsonpath='{.status.conditions}' 2>/dev/null | grep -qiE "NoBackends|no ready" && ok "status 标记 NoBackends" || echo "  (status reason 未显式,非致命)"
kubectl -n "$NS" scale deploy/be --replicas=1 >/dev/null; kubectl -n "$NS" rollout status deploy/be --timeout=120s >/dev/null
waitcond "后端恢复 1,status.backends=1" "[ \"\$(kubectl -n $NS get mr m1 -o jsonpath='{.status.backends}')\" = 1 ]"

######## F4 openresty 主备 failover ########
say "F4: 杀 openresty leader → hagate standby 接管(active 标签漂移 + Service 换端点)"
old=$(active openresty); echo "  当前 openresty leader=$old, Service ep=$(eps openresty)"
kubectl -n "$NS" delete pod "$old" --wait=false >/dev/null 2>&1
waitcond "新 openresty leader 接管(≠旧)" "n=\$(kubectl -n $NS get pod -l openresty-active=true --no-headers 2>/dev/null|awk '{print \$1}'|head -1); [ -n \"\$n\" ] && [ \"\$n\" != \"$old\" ]"
waitcond "openresty Service 有端点(failover 后可路由)" "[ -n \"\$(kubectl -n $NS get endpoints openresty -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null)\" ]"

######## F5 cart 主备 failover ########
say "F5: 杀 cart leader → hagate standby 接管"
oldc=$(active cart); echo "  当前 cart leader=$oldc"
kubectl -n "$NS" delete pod "$oldc" --wait=false >/dev/null 2>&1
waitcond "新 cart leader 接管(≠旧)" "n=\$(kubectl -n $NS get pod -l cart-active=true --no-headers 2>/dev/null|awk '{print \$1}'|head -1); [ -n \"\$n\" ] && [ \"\$n\" != \"$oldc\" ]"
waitcond "cart Service 有端点" "[ -n \"\$(kubectl -n $NS get endpoints cart -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null)\" ]"

######## F6 monitor fail ########
say "F6: 杀 monitor pod → 单副本 Recreate 恢复"
kubectl -n "$NS" delete pod -l app.kubernetes.io/name=monitor --wait=false >/dev/null 2>&1
waitcond "monitor 恢复 Running" "kubectl -n $NS rollout status deploy/monitor --timeout=8s"

######## F7 mysql fail + 数据持久 ########
say "F7: 杀 mysql pod → 恢复 + PVC 数据在(monitor 库仍在)"
kubectl -n "$NS" delete pod -l app=monitor-mysql --wait=false >/dev/null 2>&1
waitcond "mysql 恢复 Running" "kubectl -n $NS rollout status deploy/monitor-mysql --timeout=8s"
waitcond "mysql monitor 库仍在(PVC 未丢)" "kubectl -n $NS exec deploy/monitor-mysql -- sh -c 'mysql -uroot -prootpass -e \"SHOW DATABASES;\"' 2>/dev/null | grep -q monitor"

######## F1 operator fail → 现有不受影响 ########
say "F1: operator(autoconfig controller)挂 → 现有配置/组件继续服务(不依赖 operator 存活)"
cw_before=$(cw)
kubectl -n "$CTRL_NS" scale deploy/autoconfig-controller --replicas=0 >/dev/null
waitcond "controller 已停" "[ \"\$(kubectl -n $CTRL_NS get deploy autoconfig-controller -o jsonpath='{.status.readyReplicas}')\" != 1 ]"
sleep 8
for d in openresty cart monitor; do kubectl -n "$NS" get deploy $d -o jsonpath='{.status.readyReplicas}' 2>/dev/null | grep -qE '[1-9]' && ok "$d operator 挂时仍在跑" || bad "$d 挂了"; done
[ "$(cw)" = "$cw_before" ] && ok "cart config operator 挂时不变" || bad "config 变了"

######## F2 operator down + 后端 rollout → backend-svc VIP 兜底在位 ########
say "F2: operator 挂时后端 rollout(换 IP)→ pod-IP 层不更新,backend-svc VIP 兜底 peer 在(最后防线)"
kubectl -n "$NS" rollout restart deploy/be >/dev/null; kubectl -n "$NS" rollout status deploy/be --timeout=120s >/dev/null
kubectl -n "$NS" get cm openresty-conf -o jsonpath='{.data.session_route_m1\.conf}' 2>/dev/null | grep -qE '"backend-svc-0"' && ok "backend-svc VIP 兜底 peer 在位(operator 宕+rollout 时 openresty 可级联到它,降级不全断)" || bad "无 backend-svc 兜底"

######## F3 operator 恢复 → 重新收敛 ########
say "F3: operator 恢复 → 重新发现、写回新 pod IP"
kubectl -n "$CTRL_NS" scale deploy/autoconfig-controller --replicas=1 >/dev/null
kubectl -n "$CTRL_NS" rollout status deploy/autoconfig-controller --timeout=120s >/dev/null
NEWIP=$(kubectl -n "$NS" get pod -l app=be -o jsonpath='{.items[0].status.podIP}' 2>/dev/null)
echo "  rollout 后 be 新 pod IP=$NEWIP"
waitcond "operator 恢复后 cart workers 写回新 IP($NEWIP)" "kubectl -n $NS get cm cart-config -o jsonpath='{.data.config\.yaml}' | grep -q \"$NEWIP\""

######## E1 monitor restart 能力(RBAC) ########
say "E1: monitor restart() = 删故障 pod → SA 有 delete pod 权限"
kubectl auth can-i delete pods --as=system:serviceaccount:$NS:monitor -n e2e-test 2>/dev/null | grep -q yes && ok "monitor SA 可 delete pod(restart 能力就绪)" || bad "monitor SA 无 delete pod 权限"
kubectl auth can-i list pods --as=system:serviceaccount:$NS:monitor -A 2>/dev/null | grep -q yes && ok "monitor SA 可 list pods(按 pod-IP 反查)" || bad "monitor SA 无 list pods"

say "结果"; [ "$FAIL" = 0 ] && echo "ALL PASS ✅" || echo "SOME FAILED ❌"
echo TEST_RESILIENCE_DONE
