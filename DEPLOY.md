# k8s 部署手册(autoconfig 路由栈 + 测试模型 + ModelRoute)

在一套干净的 k8s 集群上,把整套「ModelRoute 驱动的 LLM 路由栈」部署起来,并跑通一个测试模型(以 **qwen** 为例;opt 等其它模型同样方式接入)。
所有组件镜像 + helm chart 都由各仓 CI 打 git tag 后产出到 harbor / ChartMuseum,本文档只做 `helm install` + `kubectl apply`。

## 0. 架构与依赖顺序

```
                    ┌── autoconfig(operator)──┐  watch ModelRoute + EndpointSlice
   ModelRoute(CR) ─▶│  渲染 3 个 ConfigMap:     │  →  openresty-conf / cart-<m>-config / monitor-conf
                    └──────────┬────────────────┘
        ┌──────────────┬───────┴───────┬──────────────┐
   openresty        cart-<model>      monitor        (各自挂对应 ConfigMap,reload sidecar 热更)
   (对外入口 8080)   (cache 亲和路由)   (dashboard)
        │                │
        └── 3 层 peer:cart(优先)→ backend pod-IP → backend-svc VIP 兜底 ──▶  模型后端(vllm/sglang)
```

**部署顺序(有依赖,别颠倒)**:
1. 前置:helm repo(ChartMuseum)+ namespace
2. **autoconfig**(先装 —— 它带 ModelRoute CRD + controller;后面组件都消费它产出的 ConfigMap)
3. 测试模型后端(qwen)
4. **cart**(每模型一个;`waitForWorkers` 会 Init 等 autoconfig 写入 workers)
5. **openresty**(对外入口)
6. **monitor**(dashboard + MySQL)
7. **ModelRoute CR**(qwen)→ autoconfig 据此填三个 ConfigMap → cart 就绪、openresty 出路由、monitor 出监控
8. 验证

镜像统一在 `harbor.4pd.io/hardcore-tech/`;chart 统一在 ChartMuseum `https://harbor.4pd.io/chartrepo/hardcore-tech`。

---

## 1. 前置

```bash
# 1.1 helm 加 ChartMuseum 仓(harbor 的 hardcore-tech project 允许匿名 pull → 只读无需凭证)
helm repo add harbor-chart-repo https://harbor.4pd.io/chartrepo/hardcore-tech
helm repo update harbor-chart-repo

# 1.2 确认能看到各 chart(注:helm search 对本 ChartMuseum 偶发空,用 helm show chart 按版本确认)
helm show chart harbor-chart-repo/autoconfig         --version 0.3.28     | grep -E '^name|^version'
helm show chart harbor-chart-repo/openresty          --version 0.1.1      | grep -E '^name|^version'
helm show chart harbor-chart-repo/cache_aware_router --version 0.6.2-k8s  | grep -E '^name|^version'
helm show chart harbor-chart-repo/monitor            --version 0.1.3      | grep -E '^name|^version'

# 1.3 namespace(qwen ns 由后端 sample 自带 Namespace,无需先建)
kubectl create ns llm-route   2>/dev/null || true
kubectl create ns monitoring  2>/dev/null || true
```

> chart 版本会随各仓发版变化;取最新版本(匿名可读):`curl -s https://harbor.4pd.io/api/chartrepo/hardcore-tech/charts/<chart>`。

---

## 2. autoconfig(operator + CRD)

chart 自带 `crds/`(ModelRoute CRD)+ controller Deployment(2 副本 leader 选举)+ RBAC。

```bash
helm -n llm-route install autoconfig harbor-chart-repo/autoconfig --version 0.3.28 \
  --set fullnameOverride=autoconfig-controller

# 校验:CRD 装上 + controller Running
kubectl get crd modelroutes.routing.gpucluster.io
kubectl -n llm-route rollout status deploy/autoconfig-controller
```

---

## 3. 测试模型后端(以 qwen 为例)

sample 在 `config/samples/`,自带 Namespace + Deployment + Service。

- `qwen-backend.yaml`:sglang `Qwen3.5-4B`(ns `qwen`,`qwen-svc` NodePort:30055;**`--enable-metrics`** 否则 monitor 采不到 KV/running/waiting;`terminationGracePeriodSeconds:3600` 排空长请求)

```bash
kubectl apply -f config/samples/qwen-backend.yaml
kubectl -n qwen rollout status deploy/qwen
# sglang /metrics 需 --enable-metrics 才 200(默认 404):
kubectl -n qwen exec deploy/qwen -- sh -c "curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8000/metrics"
```

> **opt 同理**:`config/samples/opt-backend.yaml`(vLLM `opt-125m`,ns `opt`,`opt-svc` ClusterIP:8000;`--shutdown-timeout=3540` 优雅停机)+ `modelroute-opt.yaml`,后续步骤把 `qwen` 换成 `opt` 即可。
> 生产模型(如 kimi 用 LeaderWorkerSet TP8/PP2)部署方式不同(见 `scripts/k8s-llm/`),但接入路由的方式一样:建 Service + 写 ModelRoute。

---

## 4. cart(cache_aware_router,每模型一个)

**cart 是「一个模型一个」**(radix 前缀缓存只对单模型有效)。`fullnameOverride` 决定 Service 名 + config ConfigMap 名(`<name>-config`),ModelRoute 的 `cart.service` / `cart.outputConfigMap` 要对上。
`waitForWorkers=true`:cart pod 停在 Init 等 autoconfig 写 workers(第 7 步 apply ModelRoute 后就绪),不会 CrashLoop。

```bash
# reload/hagate 侧车镜像属于 autoconfig 仓(跨仓引用),版本按当前 autoconfig 发版对齐
RELOAD=harbor.4pd.io/hardcore-tech/autoconfig-reload:0.3.28
HAGATE=harbor.4pd.io/hardcore-tech/autoconfig-hagate:0.3.28

helm -n llm-route install cart-qwen harbor-chart-repo/cache_aware_router --version 0.6.2-k8s \
  --set fullnameOverride=cart-qwen --set reload.image=$RELOAD --set ha.image=$HAGATE

# 此时 cart pod 会停在 Init(等 workers),属正常;第 7 步后转 Running
kubectl -n llm-route get pods | grep cart-
```

> opt 同理:`--set fullnameOverride=cart-opt`(要与 `modelroute-opt.yaml` 的 `cart.service`/`cart.outputConfigMap` 对上)。

---

## 5. openresty(对外入口)

chart 挂载 autoconfig 产出的 `openresty-conf` ConfigMap(各 `session_route_<model>.conf`),reload sidecar 收 SIGHUP 热更路由。
`image.tag` 已是 chart 默认(= 0.1.1);`bodylog.host` 指向 bodylog-listener(k8s 里需 FQDN 或可解析地址)。

```bash
helm -n llm-route install openresty harbor-chart-repo/openresty --version 0.1.1 \
  --set fullnameOverride=openresty \
  --set bodylog.host=192.0.2.31 \
  --set reload.image=harbor.4pd.io/hardcore-tech/autoconfig-reload:0.3.28 \
  --set ha.image=harbor.4pd.io/hardcore-tech/autoconfig-hagate:0.3.28

kubectl -n llm-route rollout status deploy/openresty
```

> openresty Service 是 **ClusterIP:8080**(集群内访问);对外测试用 `kubectl -n llm-route port-forward svc/openresty 18080:8080`。

---

## 6. monitor(dashboard + MySQL)

chart 自带 MySQL(持久化 state + 时序);读 autoconfig 产出的 `monitor-conf`(service/nginx/router 行,60s 热加载)。

**生产推荐:密钥走 `existingSecret`(带外建,不归 helm 管)** —— 这样 `helm upgrade` 无论带不带 `--set` 都碰不到密钥,避免「误用 `--set` 不带 `--reuse-values` → 密钥被刷成占位」的坑(app + mysql 两份密钥都能外置)。

```bash
# ① 带外建两份 secret(模板见 monitor 仓 k8s/secret.example.yaml,改成真值)——注意 mysql 密码两处要一致
kubectl -n monitoring apply -f secret.example.yaml    # llm-monitor-secret + llm-monitor-mysql-secret

# ② install:关掉 chart 自建密钥,引用带外的
#    (nginxHost / bodylogSummaryURL 已是 chart 默认值——bodylog 默认指 ts34,换集群才 --set 覆盖)
helm -n monitoring install monitor harbor-chart-repo/monitor --version 0.1.4 \
  --set secret.create=false \
  --set secret.existingSecret=llm-monitor-secret \
  --set mysql.auth.existingSecret=llm-monitor-mysql-secret
kubectl -n monitoring rollout status deploy/monitor
```

> **快速起(测试用,chart 自建密钥)**:不想带外建 secret 时,可用 values 文件把密钥/密码写进去(`secret.data.*` + `mysql.auth.*`,占位改真值,**别提交 repo**)、`create:true` 装。但这种模式下升级务必 `--reuse-values`,且带 `--set` 时尤其小心(见 §9)。

> dashboard 是 **NodePort:30080** → `http://<任一 node IP>:30080`(如 `http://192.0.2.20:30080`),Basic Auth `admin/<WEB_PASS>`;`/tpm` 子页独立 Auth `tpm/<TPM_PASS>`。

---

## 7. 安装 ModelRoute(触发全栈自动配置)

ModelRoute 是**中心配置**:autoconfig 据此同时写 openresty-conf / cart-<model>-config / monitor-conf。
sample 在 `config/samples/`,`modelroute-qwen.yaml`:qwen 三层路由(cart-qwen → backend pod-IP → backend-svc VIP 兜底);discovery `qwen/qwen-svc`;monitor model `qwen`。

```bash
kubectl apply -f config/samples/modelroute-qwen.yaml

# autoconfig 会在 ~秒级 reconcile;ConfigMap→pod 挂载传播有 ~1min kubelet 同步 lag
kubectl -n llm-route get modelroute qwen        # READY 应为 true,BACKENDS≥1,CART=1
```

apply 后应观察到:cart-qwen pod 从 Init 转 **Running**(autoconfig 写入 workers);openresty 出现 `session_route_qwen.conf`;monitor dashboard 出现 qwen 的 service 行。

> **ModelRoute 放哪个 ns?** 本例放 `llm-route`(与 cart/openresty 同 ns,sample 里 `cart.service: cart-qwen`、`nginx.service: openresty` 用裸名即可)。controller 是全集群 watch(ClusterRole),ModelRoute 放 model ns(如 `qwen`)也行,但那些**裸引用会默认解析到 ModelRoute 自己的 ns** → 需显式加前缀 `llm-route/cart-qwen`、`llm-route/openresty`(各 `outputConfigMap` 本就带 ns,不用改)。
> **opt 同理**:`kubectl apply -f config/samples/modelroute-opt.yaml`(discovery `opt/opt-svc`、cart-opt、monitor model `opt-125m`)。

---

## 8. 验证(端到端)

```bash
# 8.1 ModelRoute 就绪
kubectl -n llm-route get modelroute

# 8.2 cart 就绪(3/3,含 cart + reload + hagate 侧车)
kubectl -n llm-route get pods | grep -E 'cart-|openresty'

# 8.3 端到端经 openresty → cart → 后端(在 openresty pod 内打本地 8080)
ORP=$(kubectl -n llm-route get pod -l app.kubernetes.io/name=openresty -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
[ -z "$ORP" ] && ORP=$(kubectl -n llm-route get pods -o name | grep openresty | head -1 | cut -d/ -f2)
kubectl -n llm-route exec $ORP -c openresty -- sh -c \
  "curl -s -o /dev/null -w 'qwen /v1/models=%{http_code}\n' http://127.0.0.1:8080/qwen/v1/models -H 'Authorization: Bearer REDACTED-SEE-DEPLOY-DOCS'"
# chat 流(应 200,并回 X-Routed-Peer 头):
kubectl -n llm-route exec $ORP -c openresty -- sh -c \
  "curl -s -D - -o /dev/null http://127.0.0.1:8080/qwen/v1/chat/completions -H 'Content-Type: application/json' \
   -H 'Authorization: Bearer REDACTED-SEE-DEPLOY-DOCS' \
   -d '{\"model\":\"qwen\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":8}' | grep -iE 'HTTP/|x-routed-peer'"

# 8.4 monitor 采到该模型(dashboard 或 /api/status)
kubectl -n monitoring exec deploy/monitor -- python3 -c \
 'import urllib.request,base64,json;r=urllib.request.Request("http://127.0.0.1:8080/api/status");r.add_header("Authorization","Basic "+base64.b64encode(b"admin:<WEB_PASS>").decode());print("api/status",json.load(urllib.request.urlopen(r)) and "OK")'
```

`X-Routed-Peer` 头出现 = 请求确实经 cart 路由到了真实后端(cart 的 `proxy.add_routed_peer_header=true`)。

---

## 9. 升级 / 回滚(chart 已在 ChartMuseum)

发新版流程:改代码 → 各仓打 git tag(CI 自动出镜像 + push chart 到 ChartMuseum)→ 集群 `helm upgrade`。

```bash
helm repo update harbor-chart-repo
# --reuse-values 保留安装时的 fullnameOverride / 密钥 / bodylog.host 等
helm -n llm-route  upgrade autoconfig harbor-chart-repo/autoconfig         --version <新版> --reuse-values
helm -n llm-route  upgrade openresty  harbor-chart-repo/openresty          --version <新版> --reuse-values
helm -n llm-route  upgrade cart-qwen  harbor-chart-repo/cache_aware_router --version <新版> --reuse-values   # 每个 cart-<model> 各升一次
helm -n monitoring upgrade monitor    harbor-chart-repo/monitor            --version <新版> --reuse-values

helm -n <ns> history <release>            # 看修订
helm -n <ns> rollback <release> <REV>     # 回滚
```

> chart 内容不变时,`helm upgrade` 只更新 release 元数据、**不重启 pod**(渲染出的 spec 一致),零中断。
> chart 版本命名:多数仓 = git tag(0.1.x / 0.3.x);**cache_aware_router 例外** —— git tag 带前导 `v`(如 `v0.6.2-k8s`),chart version 去掉 `v`(`0.6.2-k8s`,SemVer2 不许带 v),`--version` 用去 v 的。

---

## 10. 卸载 / 清理

```bash
kubectl delete -f config/samples/modelroute-qwen.yaml
helm -n llm-route  uninstall openresty cart-qwen autoconfig      # opt 同理:再 uninstall cart-opt
helm -n monitoring uninstall monitor
kubectl delete -f config/samples/qwen-backend.yaml               # 连带删 qwen ns(opt 同理删 opt-backend.yaml)
kubectl delete crd modelroutes.routing.gpucluster.io            # 如需彻底移除 CRD
```
