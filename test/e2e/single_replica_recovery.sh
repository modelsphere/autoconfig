#!/usr/bin/env bash
# 对照基线:单副本(纯 Deployment,无热备)openresty 挂掉后靠 Deployment 重建恢复的耗时,
# 与主备(ha_failover_check.sh,~2.4s)对比。
# 用【隔离的 ortest Deployment】测,不碰线上 openresty release(toggle ha 会误删 SA,危险)。
# 前提:kimi ns 有 openresty-conf ConfigMap(autoconfig 已写)。用法:bash single_replica_recovery.sh
set -uo pipefail
NS=${NS:-kimi}
OR_IMG=${OR_IMG:-harbor.4pd.io/hardcore-tech/llm-openresty:0.2.0-routes}
avail(){ local a=$(kubectl -n "$NS" get deploy ortest -o jsonpath='{.status.availableReplicas}' 2>/dev/null); echo "${a:-0}"; }

echo "=== 起隔离单副本 ortest(openresty 镜像 + 同款 readinessProbe,挂共享路由 conf)==="
kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: { name: ortest, labels: { app: ortest } }
spec:
  replicas: 1
  selector: { matchLabels: { app: ortest } }
  template:
    metadata: { labels: { app: ortest } }
    spec:
      containers:
      - name: openresty
        image: ${OR_IMG}
        volumeMounts: [{ name: routes, mountPath: /usr/local/openresty/nginx/conf/conf.d/routes }]
        readinessProbe:
          exec: { command: ["sh","-c","pgrep -f 'nginx: master' >/dev/null"] }
          initialDelaySeconds: 10
          periodSeconds: 10
      volumes: [{ name: routes, configMap: { name: openresty-conf } }]
YAML
for i in $(seq 1 40); do [ "$(avail)" = 1 ] && break; sleep 2; done
echo "  就绪 available=$(avail)"

echo "=== 删 pod,测 delete → 新 pod Ready(availableReplicas 1→0→1)==="
POD=$(kubectl -n "$NS" get pod -l app=ortest -o jsonpath='{.items[0].metadata.name}')
t0=$(date +%s%3N)
kubectl -n "$NS" delete pod "$POD" --wait=false >/dev/null 2>&1
tdrop=""; for i in $(seq 1 100); do [ "$(avail)" = 0 ] && { tdrop=$(date +%s%3N); break; }; sleep 0.2; done
trec="";  for i in $(seq 1 120); do [ "$(avail)" = 1 ] && { trec=$(date +%s%3N); break; }; sleep 1; done
echo "  ⏱ 单副本 Deployment 重建恢复:"
[ -n "$tdrop" ] && echo "     delete → 端点掉 0        = $((tdrop-t0))ms"
[ -n "$trec" ]  && echo "     delete → 新 pod Ready     = $((trec-t0))ms(主要 = readiness initialDelay 10s + 起容器)" || echo "     120s 未恢复"

kubectl -n "$NS" delete deploy ortest --wait=false >/dev/null 2>&1
echo "(ortest 已清理;不碰线上 openresty)"
