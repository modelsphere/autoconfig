# autoconfig

**让路由层的后端列表跟着 Kubernetes 自动收敛的 operator。**

在 k8s 上跑推理服务时,后端 pod 的 IP 会随扩缩容、重启、滚动更新不断变化。而前面的路由组件
—— openresty、cache-aware-router(CART)、监控 —— 各自维护着一份 peer / worker / 采集目标列表。
人工同步这几份列表既繁琐又容易漏:扩容了没加进去等于白扩,缩容了没摘掉就是持续打死 IP。

autoconfig 用一个 `ModelRoute` 自定义资源描述「一个模型的路由长什么样」,然后:

1. **发现** —— watch 该模型对应 Service 的 EndpointSlice(或 pod label),得到当前就绪的后端端点;
2. **渲染** —— 生成 openresty 的路由配置、CART 的 workers 列表、监控的采集行;
3. **下发** —— 写进各消费方已有的 ConfigMap,由它们各自的 sidecar 热重载生效。

```bash
kubectl apply -f config/samples/modelroute-glm.yaml
kubectl get mr -A      # NAME  BACKENDS  CART  READY  AGE
```

后端扩缩容时不需要任何人工操作,`BACKENDS` 列会自己变。

## 它不做什么

- **不创建 chart、不创建 ConfigMap** —— 只把内容写进消费方**已有**的 ConfigMap。消费方的部署、
  初始配置、sidecar 挂载由各自的 chart 负责(见「消费方接入」)。
- **不代理流量** —— 它是控制面,数据面仍是 openresty / CART。
- **不管非 LLM 之外的健康语义** —— `modelType: video` 走纯反向代理,不套 token 级限流那一套。

## 快速上手

```bash
# 1. 部署 controller(chart 含 CRD + RBAC + Deployment)
helm upgrade --install autoconfig deploy/helm/autoconfig -n llm-route --create-namespace

# 2. 声明一条路由
kubectl apply -f config/samples/modelroute-glm.yaml

# 3. 看发现结果
kubectl get mr -A
kubectl describe mr <name>     # status 里有 backends / cartPeers / conditions
```

不用 helm 时走 kustomize(同源生成物):`make install`(装 CRD)+ `make deploy`(起 controller)。

**⚠️ 卸载顺序:先删 ModelRoute,再 `helm uninstall`。** ModelRoute 带 finalizer
(`routing.modelsphere.dev/cleanup`),要 controller 在跑才能摘。若先 uninstall(删了 controller)
再删 ModelRoute / namespace,ModelRoute 会卡住、拖住 namespace 与 CRD 的删除。
正确顺序:`kubectl delete mr --all -A` → `helm uninstall`。
(chart 里用 `modelRoutes` 声明的 ModelRoute 由 helm 托管,uninstall 前会随 release 删除、
controller 还在 → 自动摘 finalizer,无此问题。)
已卡住的补救:`kubectl patch mr <n> -n <ns> --type=merge -p '{"metadata":{"finalizers":[]}}'`。

## 工作原理

![autoconfig 架构:controller 按 Service 发现后端端点 → 写 openresty / CART / monitor 三个 ConfigMap;openresty/CART 里 reload sidecar 收 SIGHUP 热重载,monitor 自身每 60s 热加载](docs/architecture.png)

三个独立二进制 / 镜像,各司其职:

| 组件 | 镜像 | 角色 |
|---|---|---|
| **controller** | `autoconfig`(`cmd/`) | 唯一发现逻辑 + RBAC 一处;watch ModelRoute + EndpointSlice + Pod → 发现 → 渲染 → 写 ConfigMap + status。controller 自身 `replicas>1` 时靠 manager 的 leader 选举保证只有一个在干活。 |
| **reload sidecar** | `autoconfig-reload`(`cmd/reload`) | 跑在消费方 pod 里,watch 挂载的 ConfigMap 文件,变化就 `kill -HUP` 主进程(靠 `shareProcessNamespace`)。CART / openresty 收 SIGHUP 优雅重载。 |
| **hagate sidecar** | `autoconfig-hagate`(`cmd/hagate`) | 消费方 **master-standby**:2 副本都保持 Ready,但只有持 Lease 的 leader 给自己 pod 打 `<name>-active=true` 标签;Service selector 带这个标签 → **只有 leader 进 endpoints**。用标签而非 readiness 门控,standby 不会永久 NotReady 卡住滚动。 |

三个镜像的 tag 与 chart 版本同线(chart 的 `appVersion` = 镜像 tag),见「构建」。

## 两个 sidecar 的实现原理

消费方 pod(openresty / CART)里除主容器外各挂两个 autoconfig sidecar:**hagate**(主备门控)+ **reload**(配置热重载)。
两者都靠 `shareProcessNamespace: true` 与主容器同 pod 协作。

### hagate —— master-standby 单活门控

**为什么要单活**:openresty / CART 是**有状态**路由器(openresty 有 session 亲和 + `active_conns` 并发计数,
CART 有 prefix-cache radix tree)。多副本同时进 Service endpoints = 缓存被打散、并发计数分裂,路由质量下降。
所以要 **2 副本主备(master-standby)**:都保持运行,但同一时刻只有一个对外收流量。

**为什么不用 readinessProbe 门控**:若让 standby 的 readiness 恒 NotReady 来挡流量,Deployment 滚动时
`maxUnavailable`/`minReady` 会把「永久 NotReady 的 standby」当成不可用 → 滚动卡死。故改用**标签门控**而非 readiness。

**机制**:每 pod 一个 hagate sidecar 参与 Lease `<name>-ha` 的 leader 选举。
- 只有持 Lease 的 leader 给**自己 pod** 打 `<name>-active=true` 标签;
- Service 的 selector 带这个标签 → **只有 leader 的 pod 进 endpoints**,standby 在池外待命;
- 消费方上游(如 openresty 的 cart 层)走 **Service ClusterIP VIP**,VIP 恒指 active leader → failover/rollout 对上游透明。

**level-triggered 自愈**:每 2s 读 pod 实际标签,与「**该不该 active**(= 是 leader **且**本地 app 端口可连)」比对,
不符就纠正 —— 标签被外部误删也能自愈;本地 app 端口连不上时即便是 leader 也主动摘标签(避免把流量导向坏 pod)。

**failover**:计划内下线(SIGTERM=删 pod/滚动/驱逐)release Lease → standby ~1-2s 接管**新**流量;
本 pod **保留** active 标签作 **terminating endpoint**(deletionTimestamp),靠 CNI 的 graceful-terminating
把新连接导向 standby、老在途连接留在本 pod 排空(配合主容器优雅停 + grace),**不硬摘标签 → 不 reset 在途连接**,
pod 退出即自动出 endpoints。存活丢主(Lease 续约失败但 pod 没死)才摘标签离开 Service(避免 2-active)。
硬崩则等 Lease TTL 过期后接管。

**监控组件不做 HA**:采集/告警是**自主轮询循环**(不接收外部流量),readiness 门控挡不住重复采集,
单活无意义 → 单例(`replicas: 1` + `Recreate`),chart 不带 hagate。
openresty/cart 默认开(`replicas: 2` + `ha.enabled: true`)。

### reload —— 配置热重载

**问题**:autoconfig 改写了 ConfigMap,主进程(nginx / CART)要重读配置才生效,但不能重启(会断在途长流式连接)。

**机制**:每消费方 pod 一个 reload sidecar:
- 把输出 ConfigMap **整卷挂**(非 subPath —— subPath 不随 ConfigMap 更新同步)到 `--watch` 目录,用 fsnotify 监听;
- 文件变 → 找主进程 pid(读 `/proc/*/cmdline` 匹配 `nginx: master` / `cache-aware-router`;
  **用 cmdline 不用 `comm`** —— comm 截断 15 字符、且要避开 nginx worker)→ `kill -HUP`;
- nginx / CART 收 **SIGHUP 都是优雅重载**:坏配置只 log warning + 保留旧配置,绝不中断在途请求。

**传播延迟**:kubelet 同步挂载的 ConfigMap 有 ~1min 延迟(AtomicWriter `..data` 原子软链切换 →
reload 看到的永远是完整文件,不会读到半写)。

**openresty 侧 `--sock-dir`**:per-model server 监听 unix socket,模型删除后 nginx 不会自动 unlink
残留 `.sock`;reload 前先删掉「已无 conf 引用」的孤儿 socket。

## ModelRoute CRD

`ModelRoute`(`routing.modelsphere.dev/v1alpha1`),一个模型一个对象,`kubectl apply` 当场 CEL 校验、
`kubectl get mr` 看发现结果。完整样例见 [`config/samples/modelroute-glm.yaml`](config/samples/modelroute-glm.yaml)。
下表逐字段说明(✅=必填)。

**`spec` 顶层**

| 字段 | 必填 | 含义 |
|---|---|---|
| `modelType` | 可选 | 这条路由服务的是哪类模型,决定渲染方式。默认 `llm`;`video` = 视频生成,见下 |
| `discovery` | ✅ | 本模型的后端桶发现方式(喂 CART workers / nginx backend / monitor services) |
| `cart` | 可选 | 配了 = autoconfig 管这个 CART;省略 = nginx 直连后端(无 CART 层) |
| `nginx` | ✅ | openresty 路由:渲染 peers → `session_route_<route>.conf` |
| `monitor` | 可选 | 把发现的后端/入口/CART 也写进共享的监控配置 |

### modelType:一条路由服务哪类模型

| 取值 | 渲染成什么 | 适用 |
|---|---|---|
| `llm`(默认) | 走 lua 路由引擎:会话亲和、TTFT/TPS 限流、自适应并发、CART 前置 | `/v1/chat/completions` 这类 token 流式接口 |
| `video` | **纯反向代理**:不解析请求体、不限流;放开超时、关响应缓冲、透传 `Range`、补齐 `X-Forwarded-Host/Proto` | 视频生成:异步建任务 + 轮询 + 大文件下载 |

为什么视频不能套 LLM 那套:请求体可能是 64MB 的 base64 图(引擎要解析 body 取 model)、
一条片子要 1~3 分钟才出结果(TTFT/TPS 这类 token 级指标无从谈起)、响应是几十 MB 的视频流
(响应缓冲会把它憋在内存或磁盘上),而下载接口还要支持断点续传(`Range` 必须原样透传)。

`video` 的可用调优项如下(其余会被 CEL 拒,避免「配了以为生效」):

| `nginx.values` 键 | 默认 | 含义 |
|---|---|---|
| `max_body_size` | `64m` | 请求体上限(I2V 允许 base64 传图) |
| `proxy_timeout` | `3600s` | 读/写超时(生成 + 大文件下载) |
| `connect_timeout` | `10s` | 连后端超时 |
| `rate_limit` | **不配 = 不限速** | **单连接**下载限速,配了才渲染 `limit_rate`。接受 `200Mbps`/`1.5Gbps`(比特口径,自动换算成 nginx 要的字节/秒)或 nginx 原生写法(`25m`/`512k`) |
| `rate_limit_after` | `1m`(仅当配了 `rate_limit`) | 前 N 字节全速。建任务/查询/删除都是几百字节的 JSON,不该被下载限速拖慢 |
| `upload_conn_limit` | 不配 = 不限 | 每 IP 同时在传的连接数(`limit_conn`),超出直接 503 |
| `upload_req_limit` | 不配 = 不限 | 每 IP 请求速率(`limit_req`,nginx 原生写法如 `10r/s`) |
| `upload_req_burst` | 不配 = 无突发 | 配合 `upload_req_limit` 的突发额度 |
| `api_keys` | 不配 = **不鉴权** | 逗号分隔的 Bearer token,与 LLM 路由同一套约定;不匹配返回 401 |
| `auth_public_paths` | `~^/v2/video_generation/[^/]+/content$` | 免鉴权的路径(nginx map 左值)。默认放行下载:`content.url` 交给最终用户,浏览器不带 Authorization 头,而任务 id 是 UUID、相当于一次性能力 URL。置空 = 连下载也要 key |
| `upload_limit_key` | `$http_x_real_ip` | 上面两个 zone 按什么分组。**不能用 `$binary_remote_addr`**,原因见文末 |

**下载限速必须配合开缓冲**:`proxy_buffering off` 时 `limit_rate` 会被 nginx 完全忽略
(50MB 实测:静态文件 4.99s / 开缓冲 4.61s / 关缓冲 0.089s,`proxy_limit_rate` 同理)。
所以配了 `rate_limit` 时模板渲染成 `proxy_buffering on` + `proxy_max_temp_file_size 0`
—— 开缓冲但不落临时文件,缓冲区满即对上游反压;不配限速时仍是 `proxy_buffering off` 边收边发。

**上传方向没有字节级限速**:`limit_rate`/`proxy_limit_rate` 都只作用于响应,nginx 没有
限制请求体读取速率的指令(真要做只能在 lua 里自己读 `ngx.req.socket` 加 sleep,会丢掉
`proxy_request_buffering` 的现成反压)。所以上传靠三道闸:`max_body_size` 卡单条体积、
`upload_conn_limit` 卡并发、`upload_req_limit` 卡频率 —— 单个来源的入向带宽 ≈ 并发数 × 单条速率。

限速只限**速度不限大小** —— `client_max_body_size` 管的是请求体,和响应无关;
`proxy_buffering off` 也让响应不落临时文件,所以下载的视频多大都行(1GB 按 200Mbps 约 40 秒)。
`proxy_read_timeout` 限的是两次数据之间的间隔,不是总时长。

CEL 还会拒掉 `video` + `cart` / `slo` / `monitor`:前两个是 LLM 专用;监控的探活与告警
按 LLM 端点设计,指向视频服务只会产生假告警(用 Prometheus 抓服务自己的指标)。

peers 的优先级在 `video` 下映射成 nginx 的主用/`backup` 两档:优先级最高的一组主用,
更低的(如 `backend-svc` 这种 VIP 静态兜底)标 `backup`,pod-IP 那层全挂了才顶上。

**下发通道与 llm 完全一致**:同样写进 `nginx.outputConfigMap` 的 `session_route_<route>.conf` 键,
同样由 reload sidecar 监听挂载目录 → `SIGHUP` 生效,没有第二条通道。
(sidecar 还靠 conf 里的 `listen unix:.../<route>.sock;` 判断哪些 socket 仍在用,
video 模板保持同样的 listen 行格式,有用例守着。)

样例见 [`config/samples/modelroute-minimax-h3.yaml`](config/samples/modelroute-minimax-h3.yaml)。

**`spec.discovery`** —— 一桶后端怎么发现

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `service` | string | 与 `selector` **二选一** | EndpointSlice 发现(推荐);支持 `ns/name` 跨 ns(裸名默认同 ModelRoute 的 ns)→ ModelRoute 可放中心 ns |
| `selector` | string | 与 `service` **二选一** | pod label 发现(没建 Service 的单机/单卡兜底) |
| `port` | int | 可选,省略自动推导 | 后端端口;service 路径从 EndpointSlice 取、selector 路径从 containerPort 取(仅单端口可推) |
| `includeNotReady` | bool | `false` | 默认只取 Ready 端点(排空中端点自动排除);`true` = 含未 Ready |

**`spec.cart`** —— 省略整段 = 无 CART

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `service` / `selector` | string | **二选一** | CART pod 发现(供 openresty 的 cart source);`service` 支持 `ns/name` |
| `port` | int | 省略推导 | CART 端口(EndpointSlice/containerPort 单端口自动推) |
| `outputConfigMap` | string | ✅ | 写 CART `config.yaml` 的目标 `ns/name`;底稿(server/cache/health)由 CART chart 的 `values.baseConfig` 建在此 CM,autoconfig 只重填 `workers` 段 |
| `maxLoad` | int | `20` | 每 worker 的 `max_load` |

**`spec.nginx`** —— openresty 路由

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `route` | string | 省略 = `metadata.name` | 路由短名 = conf 文件名 + openresty dict 名 + `<route>.sock` + 外部路径 key `/<route>/`。**字符集必须 ⊆ `[a-z0-9._-]`**(做 dispatch 路径捕获正则;含大写会派生不到 socket → 8080 打不通) |
| `peers` | list | ✅(≥1) | 有序 peer 组(见 `peers[]` 表) |
| `outputConfigMap` | string | ✅ | 输出 ConfigMap `ns/name`(多路由共享,每路由一个 key,finalizer 摘各自 key) |
| `values` | map | 可选 | 任意调优项原样渲染进 lua `register_route` 返回表(key=value)→ 加新调优项无需改代码 |
| `service` | string | 可选 | nginx 入口自身的 Service `ns/name` → 供监控的 `nginx:` 行 + 入口 pod 扩缩事件驱动;端口取 Service 的 dispatch 命名端口 8080 |
| `selector` | string | 可选 | nginx 入口 pod label 发现(没建 Service 兜底;与 `service` 二选一,都配则 `service` 优先) |

**`spec.nginx.values` 示例 —— 开启动态限流(自适应并发 AIMD)**

配了 `tps_limit_tps` 即对本路由 opt-in;未显式 `adaptive_cc: "false"` 时,`adaptive_cc` 按全局默认自动开。
删掉 `values` 即退回不限流。

```yaml
spec:
  nginx:
    route: qwen
    service: llm-route/openresty
    outputConfigMap: llm-route/openresty-conf
    values:                       # 任意 key 原样渲染进 lua register_route opts(值必须字符串)
      tps_limit_tps: "30"         # 解码速率下限(tok/s):EWMA 低于它→AIMD 缩并发,高于它→涨(= opt-in 闸门)
      # adaptive_cc_min: "10"     # 可选:并发下限(不配 = 静态 max × 全局 min_frac 派生)
      # ttft_limit_ms: "60000"    # 可选:TTFT 软控阈值
      # adaptive_cc: "false"      # 可选:显式关自适应,走静态硬熔断
    peers:
    - { use: cart, priority: 3, maxConcurrencyFromBackend: true }
    - { use: backend, priority: 2, maxConcurrency: 100 }
    - { use: backend-svc, priority: 1 }
```

生效后 openresty 侧 `GET /<route>/_tps_status` 应见 `opt_in=true, adaptive_cc_on=true`。

**`spec.nginx.peers[]`** —— 有序分层(数字大=优先,高优层全 banned 才级联到低层)

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `use` | enum | ✅ `cart`\|`backend`\|`backend-svc` | 见下「三档 `use`」 |
| `priority` | int | — | openresty peer 优先级(建议 cart=3、backend=2、backend-svc=1) |
| `maxConcurrency` | int | 省略用 `values.default_max` | 该组所有 peer 的并发上限 |
| `maxConcurrencyFromBackend` | bool | 仅 `use:cart` 有意义 | `true` = cart 并发上限动态 = 后端单实例并发 × 后端数(CART 扇出到 N 后端,容量随扩缩自动变);设了则忽略静态 `maxConcurrency`,且**要求 `backend` 组 `maxConcurrency>0`** 作乘数 |
| `probePath` | string | 省略见右 | openresty 健康探测路径覆盖(GET,状态行含 200=健康否则 ban)。`use:cart` 默认 `/health`(CART 的 `/v1/models` 是缓存端点、worker 全挂也返 200,不能当信号);其余层默认空 = 用 route 的 `health_probe_path` |

三档 `use`:
- **`cart`**(priority 3)—— CART 上游,走 **CART Service 的 ClusterIP(VIP,非 pod IP)**:CART 是 master-standby,
  VIP 恒指 active leader → CART failover/rollout 对 openresty 透明,autoconfig 无需重写。
- **`backend`**(priority 2)—— 后端 **pod IP**(session 亲和 / least_conn / per-peer 健康的主力层)。
- **`backend-svc`**(priority 1,可选兜底)—— 后端 **Service 的 ClusterIP(VIP)静态兜底**:
  **autoconfig 本身宕 + 后端 rollout** 时 pod-IP 层是死 IP 又没人重写 → 若无兜底会全断;
  VIP 由 kube-proxy 维护、不依赖 autoconfig 存活,pod-IP 层全 banned 后级联到它 →
  **降级(走 kube-proxy、无亲和)但不全断**。需 `discovery.service`(selector 模式无 VIP,自动跳过)。

**`spec.monitor`** —— 可选;监控组件自身每 60s 热加载,**无 reload sidecar**(区别于 nginx/CART)。每模型一个 key,三类行:

| 字段 | 类型 | 默认/约束 | 含义与配置 |
|---|---|---|---|
| `outputConfigMap` | string | ✅ | 写监控配置的 ConfigMap `ns/name`(多模型共享,每模型一个 key) |
| `model` | string | 省略 = `metadata.name` | `service:` 行的 model 字段(served-model-name) |
| `gpuType` | string | 省略自动推导 | `service:` 行的 gpu_type;省略 = 从后端节点 GPU label `nvidia.com/gpu.product`(GFD)推短名,推不出留空 |
| `nginx` | bool | 默认 `true`(当 `spec.nginx` 配了 service/selector) | 复用 nginx 入口发现写 `nginx:` 行;`false` 关 |
| `router` | bool | 默认 `true`(当配了 `spec.cart`) | 复用 `spec.cart` 发现的 CART pod 写 `router:` 表(`.../workers`);`false` 关 |

三类输出行格式:`service: <name> \| <url> \| <model> \| <gpu_type>`(每后端实例一行)、
`nginx: <svc>-<i> \| http://ip:8080/<route>`、`router: <name>-router-<i> \| http://ip:port/workers`。

**CEL 校验(apply 时即报错)**:① `discovery`/`cart` 的 `service` 与 `selector` 必须**恰好一个**;
② `nginx.peers` 用了 `cart` 必须配 `spec.cart`;③ `cart` 用 `maxConcurrencyFromBackend` 必须给
`backend` 组配 `maxConcurrency>0`。

## 消费方接入

autoconfig 只负责把配置**写进已有的 ConfigMap**(不创建 chart、不创建 ConfigMap);消费方各自 chart 建初始
ConfigMap + reload/hagate sidecar + Service 门控,并把对应 ConfigMap 挂进自己的 pod。
`autoconfig-reload` / `autoconfig-hagate` 两个 sidecar 镜像由本仓构建,消费方跨仓引用。三个消费方一览:

| 消费方 | autoconfig 写的 ConfigMap → 挂载文件 | reload sidecar |
|---|---|---|
| **openresty** | `openresty-conf` → `conf.d/routes/session_route_<route>.conf` | ✅ `--process "nginx: master"` + `--sock-dir` |
| **cart**(cache-aware-router) | `cart-config` → `configs/config.yaml`(只重填 `workers` 段) | ✅ `--process cache-aware-router` |
| **监控** | `monitor-conf` → `conf.d/<model>.monitor.conf` | ❌ 自身每 60s 热加载 |

### openresty

- **ConfigMap 交付(为什么挂子目录)**:ConfigMap 整卷挂会覆盖整个目录,而 `.conf` 和 `lua/` 同在 `conf.d/`。
  所以把 `session_route*.conf` 移到子目录 **`conf.d/routes/`**,ConfigMap(`openresty-conf`)只挂到那里;
  `lua/` + `router_locations.inc` + `nginx.conf` + **8080 dispatch(`session_base.conf`)** 仍烤镜像。
  `nginx.conf` 的 include 从 `conf.d/*.conf` 改成 `conf.d/routes/*.conf`(`lua_package_path` 不变)。
- **路径路由(单一对外端口)**:
  - 镜像 baked 一个 `listen 8080` 的 dispatch server,按请求路径首段 `/<route>/`
    运行时派生到 per-model server 的 unix socket(`<prefix>/sock/<route>.sock`)—— 单一对外端口、零映射表。
  - per-model server 只 `listen unix:.../<route>.sock`(不占 TCP 端口),故 `spec.nginx.route` = 外部路径 key
    = socket 名;autoconfig 生成的 `session_route_<route>.conf` 里就是这个 socket listen。
  - dispatch 把打 8080 的真实客户端 IP 经 `X-Real-IP` 透传,per-model `set_real_ip_from unix:` 还原
    `$remote_addr` → `allow 127.0.0.1` 的调参端点仍只对 in-pod 本地开放。
- **reload sidecar**:`--process "nginx: master"` + **`--sock-dir`**。

### cart

- **ConfigMap 交付**:整卷挂 `cart-config` → cart 启动 `-c configs/config.yaml`。底稿(`server`/`cache`/`health`)
  由 CART chart 的 `values.baseConfig` 建在此 ConfigMap,autoconfig 只重填 `workers` 段。
- **reload sidecar**:`--process cache-aware-router`。

### 监控

- **ConfigMap 交付**:整卷挂 `monitor-conf` → `conf.d/<model>.monitor.conf`(多模型共享,每模型一个 key)。
- **无 reload sidecar**:自身每 60s 热加载,不需要 SIGHUP(区别于 openresty/cart)。

### reload sidecar 接入(openresty / cart 通用)

机制见上「reload —— 配置热重载」。接入 = 消费方 pod 加一个 `autoconfig-reload` 容器
(`shareProcessNamespace: true` 才能发 SIGHUP + 输出 ConfigMap **整卷挂**到 `--watch`),args:

```yaml
# openresty
args: ["--watch","/watch","--process","nginx: master","--sock-dir","/usr/local/openresty/nginx/sock"]
# cart
args: ["--watch","/watch","--process","cache-aware-router"]
```

## 构建(三个镜像)

多阶段 build:golang builder 编译 → 产物打进运行期基础镜像。**不 vendor**;
`go.sum` 入库 + `GOSUMDB=off` 保证可复现,`GOTOOLCHAIN=local` 防联网拉工具链。

基础镜像与 goproxy 都是 `ARG`,**默认走公网**,clone 下来即可构建:

```bash
docker build                        -t autoconfig:dev        .   # controller(cmd/)
docker build -f Dockerfile.reload   -t autoconfig-reload:dev .   # reload sidecar
docker build -f Dockerfile.hagate   -t autoconfig-hagate:dev .   # hagate sidecar
```

| ARG | 默认 | 说明 |
|---|---|---|
| `GO_BASE` | `golang:1.23.3-alpine3.20` | 编译阶段基础镜像 |
| `RUNTIME_BASE` | `python:3.12-alpine` | 运行期基础镜像 |
| `GOPROXY` | `https://proxy.golang.org,direct` | 依赖代理 |

在内网 / 受限网络里换成镜像缓存:

```bash
docker build \
  --build-arg GO_BASE=<registry>/library/golang:1.23.3-alpine3.20 \
  --build-arg RUNTIME_BASE=<registry>/<project>/python:3.12-alpine \
  --build-arg GOPROXY=<your-goproxy> \
  -t autoconfig:dev .
```

`Makefile` 的 `make docker-build` 已带上这组参数(`BUILD_ARGS` 变量可覆盖,走公网时
`make docker-build BUILD_ARGS=`)。CI 在打 git tag 时自动构建并推送三个镜像,
chart 的 `version` / `appVersion` 同步成该 tag —— `values.yaml` 的 `image.tag` 留空即回落到
`appVersion`,**不要在 values 里写死版本号**。

本地快速验证:`go build ./cmd/...`。

## 测试

```bash
make test        # 代码生成 + fmt + vet + go test ./...
```

`test/e2e/` 下是端到端脚本,需要一个可用的 k8s 集群(`kubectl` + `helm`),覆盖:
CRD controller 的发现分桶 / 渲染内容 / status / 扩缩跟随 / fail-safe / finalizer 清理,
以及真 openresty + 真 CART 经 helm chart 的接入(真 reload 热更、路径路由 unix socket、
`openresty -t` 校验、master-standby failover)。脚本默认值用环境变量覆盖,
鉴权 key 等敏感项需显式提供(未设置会直接报错退出,不带默认值)。

## 开发布局(kubebuilder / operator-sdk v4)

标准 operator 布局:`PROJECT` + `Makefile` + `api/v1alpha1`(带 kubebuilder marker 的类型)
+ `internal/controller`(reconciler)+ `internal/{discovery,sink,hagate,reload}`
+ `config/`(kustomize:crd/rbac/manager/default/samples)。

**改了 `api/` 类型或 `+kubebuilder:` marker 后**,跑生成、提交生成物(CI 不跑生成,只编译):

```bash
make generate manifests    # controller-gen 生成 deepcopy + config/crd/bases + config/rbac/role.yaml,并同步 CRD 到 helm/crds
make test                  # 生成 + fmt + vet + go test
```

工具用 `go run ...@version`(见 Makefile),不装二进制、不入库。

**部署两条路都可**:生产用 **Helm**(`deploy/helm/autoconfig`);kustomize 用 `make deploy`
(`config/default`)。两者的 CRD/RBAC 同源(都来自 `config/` 的生成物)。

## 注意(踩坑)

- **ConfigMap 必须整卷挂**(非 subPath)才会随更新自动同步;kubelet 同步有 **~1min 延迟** —— 对 peer 更新可接受
  (health-timer + proxy_next_upstream 兜过渡),对 CART 反而是天然去抖(reload 会重建 radix tree)。
- **fail-safe**:发现结果为空绝不写空(CART 拒绝空 workers;openresty 会丢全部流量)。
- **reload 找 pid 只比 argv[0]**(不是整条 cmdline,也不用 comm —— comm 截断 15 字符):否则 sidecar 自己的
  `--process nginx: master` 参数会自匹配。规则:argv[0] 相等 / basename 相等 / 以 match 开头
  (nginx master 的 argv[0] = `nginx: master process ...`)。
- **env 名别撞 k8s Service 注入**:若有名为 `cart` 的 Service,k8s 会注入 `CART_PORT=tcp://...`;
  autoconfig 的 env 前缀统一 `PS_`。

### 为什么限流的默认 key 不是 `$binary_remote_addr`

生产链路是 dispatch(`:8080` TCP)→ `proxy_pass` 到 **unix socket** → 各路由的 server 块。
在 unix socket 那一跳上没有 IP,实测路由层拿到的是:

```
经 dispatch → unix socket:  remote_addr=[unix:]  xff=[127.0.0.1]  xrealip=[127.0.0.1]
客户端带 XFF 时:            remote_addr=[unix:]  xff=[203.0.113.7, 127.0.0.1]
直连对照(不经 socket):      remote_addr=[127.0.0.1]
```

`$remote_addr` 恒等于字符串 `unix:` —— 每个请求算出同一个 key,
`limit_conn 4` 就成了「整个服务同时只许 4 条」,而不是「每 IP 4 条」。
这与外层是不是网关无关,是 dispatch→路由走 unix socket 这个结构决定的。
