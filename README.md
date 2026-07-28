# autoconfig

从 k8s 自动发现后端 pod(按 label 分桶成「模型」),渲染并更新 **openresty** 和 **cache-aware-router(CART)**
的 peer/worker 配置 —— 免去手工维护路由器的后端列表。上 k8s 后后端 pod IP 随扩缩容/重启变化,
autoconfig 让路由器配置跟着实时收敛。

## 架构:中心 agent + reload sidecar

```
┌──────── autoconfig agent(中心 Deployment,RBAC: list/watch pods + 管 configmaps)────────┐
│ 按 targets(label selector)watch/poll pods → 按 target 分桶 {target: [ip:port,...]}(只取 Ready)│
│ 可插拔 sink 渲染:cart → config.yaml 的 workers;openresty → 改写 conf 的 peers 块              │
│ diff(变了才写)+ fail-safe(发现为空 → 保留上次不写)→ 写进对应 ConfigMap                       │
└───────────────┬───────────────────────────────────┬──────────────────────────────────────────┘
        ConfigMap│(整卷挂,kubelet 同步 ~1min)   ConfigMap│
        ┌────────▼──────── CART pod ────────┐ ┌───────▼──── openresty pod ──────┐
        │ [cart] 读 config.yaml             │ │ [openresty] include 动态 confs   │
        │ [autoconfig --reload-mode]        │ │ [autoconfig --reload-mode]       │
        │   inotify 文件变 → SIGHUP cart     │ │   inotify → SIGHUP nginx master  │
        │   shareProcessNamespace           │ │   shareProcessNamespace          │
        └───────────────────────────────────┘ └──────────────────────────────────┘
```

- **agent**(`autoconfig --config`):唯一发现逻辑 + RBAC 一处;发现 → 渲染 → 写 ConfigMap。
- **reload sidecar**(`autoconfig --reload-mode`):同一个二进制的另一模式;watch 挂载的 ConfigMap 文件,
  变化就 `kill -HUP` 主进程(靠 `shareProcessNamespace`)。CART/openresty 收 SIGHUP 优雅重载;
  坏配置只 log warning 保留旧配置,不崩。

已在真集群 + 真 `cache_aware_router` + mock 后端端到端验证:多模型分桶、scale 精准跟随、
不变不 reload、空→保留上次(fail-safe)。

## 配置(agent)

```yaml
intervalSeconds: 5
targets:                      # 每个 = 一桶后端(通用 label,不限 LWS)
  - name: kimi-k2.6
    namespace: kimi
    selector: "leaderworkerset.sigs.k8s.io/name=kimi-k26,leaderworkerset.sigs.k8s.io/worker-index=0"
    port: 8050
    # includeNotReady: false  # 默认只取 Ready
    # staticPeers: [{ip: 127.0.0.1, port: 8060, name: router, priority: 1, maxConcurrency: 180}]
sinks:                        # target ↔ 消费方显式绑定
  - kind: cart                # 一个 CART 一个模型
    target: kimi-k2.6
    baseConfig: /base/cart/config.base.yaml   # 非 workers 部分(server/cache/health…)
    outputConfigMap: kimi/cart-config
    maxLoad: 20
  - kind: openresty           # 一个 openresty 多路由
    baseConfigDir: /base/openresty            # 各 session_route_<model>.conf 的 base
    routeByTarget: { kimi-k2.6: session_route_kimi-k2.6.conf, glm-5.1-fp8: session_route_glm.conf }
    outputConfigMap: openresty/openresty-conf
```

- **cart sink**:把 base config.yaml + 发现的 workers 渲染成完整 `config.yaml`(一个 key)。
- **openresty sink**:读 baseConfigDir 里每个 `.conf`,对 `routeByTarget` 指定的 conf **brace 定位并重写
  `peers = {` / `_G.PEERS = {` 块**(位置形式 `{ip, port, name[, priority[, maxConcurrency]]}`),
  其余原样透传,写进输出 ConfigMap(**只含 `.conf`,不含 lua**)。

### openresty 侧:只把 `.conf` 放 ConfigMap + 一行 include 改动
ConfigMap 整卷挂会覆盖整个目录,而 `.conf` 和 `lua/` 同在 `conf.d/`。所以把 **session_route*.conf 移到
子目录 `conf.d/routes/`**,ConfigMap 挂到那里;`lua/` + `router_locations.inc` + `nginx.conf` 仍烤镜像。
openresty `nginx.conf` 把 `include conf.d/*.conf;` 改成 `include conf.d/routes/*.conf;`(lua_package_path 不变)。
—— autoconfig **代码零改**:baseConfigDir 只放 `.conf`、消费方把输出 ConfigMap 挂到 `conf.d/routes/` 即可。

## 构建

二进制在本机交叉编译成静态 linux/amd64(内网无 golang 基础镜像,这样最省事),镜像只打包:

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o autoconfig ./cmd/autoconfig
docker build -t harbor.4pd.io/hardcore-tech/autoconfig:<tag> .
docker push harbor.4pd.io/hardcore-tech/autoconfig:<tag>
```

## 部署

见 `deploy/`:
- `agent.yaml`:SA + Role(pods list/watch、configmaps get/update)+ RoleBinding + agent Deployment + config ConfigMap。
- 消费方 pod 里加 `autoconfig --reload-mode` sidecar + `shareProcessNamespace` + 挂载对应输出 ConfigMap
  (**整卷挂,非 subPath**——subPath 不随 ConfigMap 更新)。片段见 `deploy/agent.yaml` 注释。

## 注意(踩坑)

- **ConfigMap 必须整卷挂**(非 subPath)才会随更新自动同步;kubelet 同步有 **~1min 延迟**——对 peer
  更新可接受(health-timer + proxy_next_upstream 兜过渡),对 CART 反而是天然去抖(reload 会重建 radix tree)。
- **env 名别撞 k8s Service 注入**:若有名为 `cart` 的 Service,k8s 会注入 `CART_PORT=tcp://...`;
  autoconfig 的 env 前缀统一 `PS_`。
- **找 pid 用 `/proc/pid/cmdline` 不用 comm**(comm 截断 15 字符);openresty 匹配 master(`nginx: master`)。
- **fail-safe**:发现结果为空绝不写空(CART 拒绝空 workers;openresty 会丢全部流量)。
