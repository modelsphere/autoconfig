# autoconfig RUN BOOK

autoconfig 运维手册。当前聚焦:**验证后端扩缩容时,autoconfig 是否把 openresty / cart / monitor 的配置自动同步跟随**。

## 组件与版本(k8s-cpu-20 / `llm-route`+`monitoring` ns,2026-08-04)

| 组件 | helm release | 运行镜像 |
|---|---|---|
| autoconfig controller | `autoconfig-0.3.30` | `autoconfig:0.3.30` |
| openresty | `openresty-0.1.1` | `llm-openresty:0.1.1` + sidecar `autoconfig-reload/hagate:0.3.30` |
| cart | `cache_aware_router-0.1.1` (app `v0.6.2-k8s`) | `cache_aware_router:v0.6.2-k8s` + sidecar `:0.3.30` |
| monitor | `monitor-0.1.0` | `llm-monitor:0.1.0`(在 `monitoring` ns) |

**链路**:后端 pod 变化 → k8s EndpointSlice → **autoconfig watch → 重渲染 → 写 ConfigMap**(秒级)→ 消费方 pod 挂载卷更新(kubelet 传播 **~1min lag**)→ reload sidecar `SIGHUP` → 生效。

autoconfig 写的 3 个 ConfigMap:
- openresty:`llm-route/openresty-conf` key `session_route_<route>.conf`(peers 块)
- cart:`llm-route/cart-config` key `config.yaml`(workers)
- monitor:`monitoring/monitor-conf` key `<route>.monitor.conf`(service/nginx/router 行)

---

## 验证:扩缩容 → 配置自动同步

以 `opt` 路由(后端 = `opt` ns 的 vLLM `opt-125m`,route 名 `opt`)为例。**全部命令在 k8s-cpu-20 上跑**(kubectl 在那)。建议开 2-3 个终端:一个执行 scale,其余 `watch` 观察。

### 0. 辅助变量(每个新终端先跑一次)
```bash
NS=llm-route
AUTH="Authorization: Bearer REDACTED-SEE-DEPLOY-DOCS"
# openresty active leader pod(HA hagate 只有它对外服务 + 有 curl,用它做 exec 探测)
orexec(){ kubectl -n $NS exec "$(kubectl -n $NS get pod -l openresty-active=true -o jsonpath='{.items[0].metadata.name}')" -c openresty -- "$@"; }
```

### 1. opt 后端 replica 2 → 1(制造变化)
```bash
kubectl -n opt get pod -l app=opt -o wide          # 当前

kubectl -n opt scale deploy opt --replicas=2        # 扩到 2 → 触发"新增 peer"
kubectl -n opt rollout status deploy opt

# 观察一会儿(见下面 2-5)后,缩回 1 → 触发"摘除 peer"
kubectl -n opt scale deploy opt --replicas=1
kubectl -n opt get pod -l app=opt -o wide
```

### 2. autoconfig status 变化(controller 感知)
```bash
# ModelRoute status:backends 应 1→2→1,observedGeneration / lastSyncTime 跟着跳
watch -n1 "kubectl -n $NS get mr opt -o custom-columns='BACKENDS:.status.backends,CARTPEERS:.status.cartPeers,GEN:.status.observedGeneration,READY:.status.ready,SYNC:.status.lastSyncTime'"

# (可选)controller 日志看 reconcile 触发
kubectl -n $NS logs -l app.kubernetes.io/name=autoconfig --tail=30 -f
```

### 3. openresty 配置自动更新(**关键对比:ConfigMap 快、pod 生效慢 ~1min**)
```bash
# a) autoconfig 写的 ConfigMap —— 秒级跟随(backend peer 行)
watch -n1 "kubectl -n $NS get cm openresty-conf -o jsonpath='{.data.session_route_opt\.conf}' | grep -E '8000, \"backend-'"

# b) openresty POD 实际路由用的 peer(_health_status)—— 有 ~1min 挂载卷传播 lag
watch -n2 "kubectl -n $NS exec \$(kubectl -n $NS get pod -l openresty-active=true -o jsonpath='{.items[0].metadata.name}') -c openresty -- curl -s http://127.0.0.1:8080/opt/_health_status"
```
对比 a / b:scale 后 **a 立刻变(0-1s),b 要等约一个 kubelet ConfigMap 同步周期(实测 ~57s)才变**。这段窗口内 openresty 仍按旧 peer 列表路由(对已摘除的死 pod 靠 `proxy_next_upstream` 重试兜)。

### 4. cart 配置自动更新(同样 ConfigMap 快、cart 生效有 lag)
```bash
# a) ConfigMap 里的 workers —— 快
watch -n1 "kubectl -n $NS get cm cart-config -o jsonpath='{.data.config\.yaml}' | grep -E 'url:'"

# b) cart 实际加载的 worker —— 有 lag
watch -n2 "kubectl -n $NS exec \$(kubectl -n $NS get pod -l openresty-active=true -o jsonpath='{.items[0].metadata.name}') -c openresty -- curl -s http://cart:8071/workers"
```

### 5. monitor 配置自动更新
```bash
# autoconfig 写的 monitor-conf,opt 的 service 行后端 IP 列表随 replica 变
watch -n1 "kubectl -n monitoring get cm monitor-conf -o jsonpath='{.data.opt\.monitor\.conf}'"
```
关注 `service: opt-0 | http://<IP>:8000 | opt-125m | A100` 行:scale 到 2 出现两条后端(`opt-0`/`opt-1`),缩回 1 变回一条。monitor 进程按自身 reload 周期(~60s)拉取生效。

### 预期总结
| 对象 | 跟随速度 |
|---|---|
| ModelRoute `status.backends` | 秒级 |
| ConfigMap:openresty-conf / cart-config / monitor-conf | 秒级(autoconfig 直接写) |
| openresty pod 实时 peer(`_health_status`) | **滞后 ~1min**(kubelet ConfigMap→挂载卷传播 + reload sidecar SIGHUP) |
| cart pod 实时 workers(`/workers`) | **滞后 ~1min**(同上) |
| monitor 进程 | ConfigMap 传播 + monitor 自身 ~60s reload |

> **传播 lag 根因**:消费方读的是**挂载的 ConfigMap 卷**,kubelet 同步有 ~1 分钟延迟;reload sidecar 用 inotify 监听挂载文件,只能等文件变才触发,追不上 kubelet。想压到秒级需让 autoconfig **直连信号**(直接 SIGHUP / API 通知)绕开 ConfigMap 卷。参见 `plans/`(TODO)。

---

## 附:一键脚本
`tools/k8s-e2e/`(vllm 部署仓)下有自动化脚本:
- `sigterm_peer_removal_latency.sh` —— in-pod 高频轮询,量 delete pod 后 peer 从 openresty 实时状态消失的延迟。
- `fullstack_cart_openresty_rollout.sh` —— operator 挂 + 单 replica 滚动下,cart(connect_timeout)+ openresty(跨层重试)协同验证。
