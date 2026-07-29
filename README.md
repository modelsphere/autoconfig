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
targets:                      # 每个 = 一桶后端;发现方式二选一(service 或 selector)
  - name: glm-5.1-fp8         # (推荐)service:走 EndpointSlice——拿 Service 背后端点,原生 Ready/Terminating 语义
    namespace: glm
    service: glm-leader        #   前提:该 Service 只选服务端点(LWS Service 配成只选 leader:selector 带 worker-index=0)
    port: 8050
  - name: kimi-k2.6           # (兜底)selector:pod label 发现——没建 Service 的单机/单卡
    namespace: kimi
    selector: "leaderworkerset.sigs.k8s.io/name=kimi-k26,leaderworkerset.sigs.k8s.io/worker-index=0"
    port: 8050
    # includeNotReady: false  # 默认只取 Ready(排空中端点 Ready=false 自动排除)
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

- **发现(每 target 二选一)**:`service` → **EndpointSlice**(`discovery.k8s.io/v1`,按 `kubernetes.io/service-name`
  聚合全部分片),用 k8s 原生 `Ready/Serving/Terminating` 判就绪——排空中端点(`Ready=false`)默认自动排除,
  天然支撑优雅下线;要求该 Service 只选服务端点(**LWS Service 配成只选 leader**)。`selector` → pod label 发现
  (兜底:没建 Service 的单机/单卡)。两条路都只用 `t.Port` 配 pod IP。
- **cart sink**:把 base config.yaml + 发现的 workers 渲染成完整 `config.yaml`(一个 key)。
- **openresty sink**:读 baseConfigDir 里每个 `.conf`,对 `routeByTarget` 指定的 conf **brace 定位并重写
  `peers = {` / `_G.PEERS = {` 块**(位置形式 `{ip, port, name[, priority[, maxConcurrency]]}`),
  其余原样透传,写进输出 ConfigMap(**只含 `.conf`,不含 lua**)。

### openresty 两种模式:改写现成 conf / 模板生成整个 conf
- **(A) rewrite**(`baseConfigDir` + `routeByTarget`):读现成 conf,只**改写 peers 块**,其余透传。
  适合共享基座 `session_route.conf`(K2.5 + 共享 dicts/init)和手调过的 conf。
- **(B) template**(`template` + `routes`):**agent 用 Go 模板给每条路由生成整个 conf**(dicts+server+register_route+peers)。
  **加一条路由 = 配置里加一个 `routes` 条目 + 一个 target**,不用手写 conf:
  ```yaml
  sinks:
    - kind: openresty
      baseConfigDir: /base/openresty            # (A) 基座 session_route.conf 等
      routeByTarget: { kimi-k2.6-router: session_route.conf }
      template: /tmpl/openresty-route.tmpl       # (B) 每路由生成
      routes:
        - { target: glm-5.1-fp8, file: session_route_glm.conf, values: { route: glm, listen: 18083, ttft_limit_ms: 60000 } }
        - { target: kimi-k2.6,   file: session_route_kimi-k2.6.conf, values: { route: k26, listen: 18082 } }
      outputConfigMap: openresty/openresty-conf
  ```
  模板见 `deploy/openresty-route.tmpl`(骨架对齐真实 per-model conf:6 个 `*_<route>` dict + server + `register_route` + `peers` + `include conf.d/router_locations.inc`)。已实测:模板生成的 conf 用真 lua 正常加载;**动态加一条路由 → reload 后新端口 + 新 dict 上线**。
  ✅ 改 agent 配置(加/删 route/target)**免重启**:agent 每周期重读 config,变了就重建 sinks(读失败保留上次好配置)。config 也走 ConfigMap 挂载,改 ConfigMap 即热更。

### 自动发现 CART 配进 openresty:一条 route 多来源(CART 优先 + 后端兜底)
CART 本身也是 k8s pod,发现用同一套 label target(指 CART pod,port 8071)。一条 route 的 peers 用
`sources`(有序,带 priority)从**多个 target** 组合 —— 复刻生产 m-cpu-11 的 **CART 作 priority-1 优先 +
后端桶作 priority-0 兜底**(CART 挂了 openresty 仍直连后端)。同一份 agent 配置同时驱动
**CART 的 workers(cart sink)** 和 **openresty 的 peers(CART + 后端)**,闭环:
```yaml
targets:
  - { name: glm-backends, namespace: glm, selector: "app=glm,role=leader", port: 8050 }
  - { name: cart-glm,     namespace: routing, selector: "app=cart,model=glm-5.1-fp8", port: 8071 }
sinks:
  - kind: cart                     # CART 的 workers = 后端桶
    target: glm-backends
    baseConfig: /base/cart/config.base.yaml
    outputConfigMap: routing/cart-glm-config
  - kind: openresty
    template: /tmpl/openresty-route.tmpl
    routes:
      - file: session_route_glm.conf
        values: { route: glm, listen: 18083 }
        sources:                   # 有序:CART 优先,后端兜底
          - { target: cart-glm,     priority: 1, maxConcurrency: 180 }
          - { target: glm-backends, priority: 0 }
    outputConfigMap: openresty/openresty-conf
```
route 只给单一 `target`(不给 `sources`)仍是旧行为(直连后端,无 CART)。纯 openresty→CART→后端
拓扑 = 只写一个 `sources: [{target: cart-glm}]`。渲染已单测(`internal/sink/openresty_test.go`)。

### openresty 侧:只把 `.conf` 放 ConfigMap + 一行 include 改动
ConfigMap 整卷挂会覆盖整个目录,而 `.conf` 和 `lua/` 同在 `conf.d/`。所以把 **session_route*.conf 移到
子目录 `conf.d/routes/`**,ConfigMap 挂到那里;`lua/` + `router_locations.inc` + `nginx.conf` 仍烤镜像。
openresty `nginx.conf` 把 `include conf.d/*.conf;` 改成 `include conf.d/routes/*.conf;`(lua_package_path 不变)。
—— autoconfig **代码零改**:baseConfigDir 只放 `.conf`、消费方把输出 ConfigMap 挂到 `conf.d/routes/` 即可。

## CRD 模式(controller)—— 一个模型一个 ModelRoute

除了「一个大 ConfigMap + 轮询 agent」,autoconfig 还有 **CRD controller 模式**(`autoconfig --controller`):
输入从 ConfigMap 换成 **`ModelRoute`(routing.4pd.io/v1alpha1)**,一个模型一个对象;`kubectl apply` 当场校验、
`kubectl get modelroute` 直接看发现了几个后端/CART/ready。**发现(`internal/discovery`)、渲染(`internal/sink`)、
reload sidecar 全部复用**,只是「输入」变 CR、多回写 `status`。设计见 [`docs/crd-design.md`](docs/crd-design.md)。

**Helm 部署(推荐)** —— chart 在 `deploy/helm/autoconfig/`(含 CRD + SA/RBAC + Deployment):
```bash
helm upgrade --install autoconfig deploy/helm/autoconfig -n autoconfig --create-namespace
kubectl apply -f config/samples/modelroute-glm.yaml
kubectl get mr -A                                   # NAME/BACKENDS/CART/READY/AGE
```
常用 values:`image.tag`、`replicas`(>1 leader 选举 HA)、`crd.install`(默认 true;CRD 带
`helm.sh/resource-policy: keep`,卸载不删,保护已有 ModelRoute)、`leaderElection`、
`modelRoutes`(见下,直接在 chart 里声明路由)。

**⚠️ 卸载顺序:先删 ModelRoute,再 `helm uninstall`。** ModelRoute 带 finalizer
(`routing.4pd.io/cleanup`),要 controller 在跑才能摘。若先 uninstall(删了 controller)再删 ModelRoute /
namespace,ModelRoute 会卡住、拖住 namespace/CRD 删除。正确:`kubectl delete mr --all -A` → `helm uninstall`。
(chart 里用 `modelRoutes` 声明的 ModelRoute 由 helm 托管,`helm uninstall` 前会随 release 删除,controller
还在 → 自动摘 finalizer,无此问题。)已卡住的补救:
`kubectl patch mr <n> -n <ns> --type=merge -p '{"metadata":{"finalizers":[]}}'`。

裸 manifest(不想用 helm 时):
```bash
kubectl apply -f config/crd/modelroutes.yaml     # 装 CRD
kubectl apply -f deploy/controller.yaml             # 起 controller
```
一个 `ModelRoute` 同时驱动 **CART 的 workers**(cart-config,专属 → ownerRef 级联 GC)和 **openresty 的 peers**
(openresty-conf,多路由共享 → finalizer 摘 key);`cart` 段可选(省略 = openresty 直连后端)。消费方 pod +
reload sidecar 与 agent 模式**完全一样**。两模式共用一个镜像/二进制,`--controller` 开关切换。

## 构建

**多阶段 build,依赖已 vendor(`vendor/` 入库),全程离线**——不联网、不预置二进制:

```bash
docker build -t harbor.4pd.io/hardcore-tech/autoconfig:<tag> .   # builder=harbor golang:1.23.3-alpine,-mod=vendor 编译 → 打进 python:3.12-alpine
docker push harbor.4pd.io/hardcore-tech/autoconfig:<tag>
```

**CI(`.gitlab-ci.yml`):打 git tag 自动 build+push** `:<tag>`+`:latest`(`public-buildx` runner,docker 已 login harbor)。
该 runner 只 go1.17.6 且够不到外网,故用 vendor 离线 + golang builder 从 harbor 拉。改依赖后 `go mod vendor` 重新入库。

改 Go 代码本地快速验证:`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -mod=vendor ./cmd/autoconfig`。

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
