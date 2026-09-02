// Package controller 是 ModelRoute 的 controller-runtime 调谐器。
// 复用 internal/discovery 与 internal/sink;只把「输入」从 ConfigMap 换成 CR,并回写 status。
package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	slov1 "autoconfig/api/inference/v1alpha1"
	routingv1 "autoconfig/api/v1alpha1"
	"autoconfig/internal/config"
	"autoconfig/internal/discovery"
	"autoconfig/internal/sink"
)

const (
	finalizer = "routing.gpucluster.io/cleanup"
	// 重新发现的轮询周期(informer 事件之外的兜底 resync)。
	resyncEvery = 10 * time.Second
	// dispatchPortName:openresty chart Service 里路径路由 dispatch 端口的【名字】。monitor 的 nginx 行
	// 要探 dispatch 口 + /<route> 路径(路径路由),autoconfig 按此名从 openresty Service 取端口号
	//(端口数字只存在于 chart Service,autoconfig 不硬编码)。per-model 路由 conf 只监听 unix socket、不涉端口。
	dispatchPortName = "dispatch"
	// sloCondType:SLO 下发结果的 condition 类型(与 "Ready" 分开 —— SLO 下发不了不影响转发)
	sloCondType = "SLOSynced"
)

// ModelRouteReconciler 调谐 ModelRoute。
// Client 管 CR + ConfigMap;Clientset 供 discovery(EndpointSlice/pod 发现)。
type ModelRouteReconciler struct {
	client.Client
	Clientset kubernetes.Interface
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=routing.gpucluster.io,resources=modelroutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=routing.gpucluster.io,resources=modelroutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=routing.gpucluster.io,resources=modelroutes/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list
// +kubebuilder:rbac:groups=inference.x-k8s.io,resources=llmslorequirements,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *ModelRouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var rb routingv1.ModelRoute
	if err := r.Get(ctx, req.NamespacedName, &rb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 删除:跑 finalizer 清理(只摘掉共享 openresty ConfigMap 里自己那个 key)
	if !rb.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&rb, finalizer) {
			if err := r.cleanupSharedKeys(ctx, &rb); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&rb, finalizer)
			if err := r.Update(ctx, &rb); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&rb, finalizer) {
		controllerutil.AddFinalizer(&rb, finalizer)
		if err := r.Update(ctx, &rb); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// 1) 发现后端桶(喂 CART workers + nginx backend 来源)
	backendTarget := discoveryTarget(
		rb.Spec.Discovery.Service, rb.Spec.Discovery.Selector, rb.Spec.Discovery.Port, rb.Spec.Discovery.IncludeNotReady, rb.Namespace)
	backends, err := discovery.Discover(ctx, r.Clientset, backendTarget)
	if err != nil {
		// 发现失败(如端口不唯一、Service 不存在):写进 status 让 kubectl describe 看得到,而非只进日志
		r.setStatus(ctx, req.NamespacedName, 0, 0, false, "DiscoverError", fmt.Sprintf("discover backends: %v", err))
		return ctrl.Result{RequeueAfter: resyncEvery}, nil
	}
	// 逐 peer 标注 GPU 型号(各自所在节点的 GFD label);openresty peer 的 gpu 命名字段与 monitor
	// 的 service 行都用它。混布场景下每个 peer 各自正确;推不出留空、不影响其余流程。
	discovery.AnnotateGPUTypes(ctx, r.Clientset, backends)
	if len(backends) == 0 {
		// fail-safe:绝不写空(CART 拒空 workers、openresty 会丢全部流量)。标记 not-ready 后重试。
		log.Info("0 ready backends, keep last config (fail-safe)")
		r.setStatus(ctx, req.NamespacedName, 0, 0, false, "NoBackends", "no ready backends")
		return ctrl.Result{RequeueAfter: resyncEvery}, nil
	}

	// 2) CART(可选):渲染 workers 写 cart-config,并发现 CART pod 供 openresty 引用
	var cartPeers []config.Peer
	if c := rb.Spec.Cart; c != nil {
		// 底稿(server/cache/health/proxy)来自 chart 建的 cart-config(values.baseConfig);autoconfig 只覆盖 workers 键。
		// 读现有 config.yaml 当底稿(YAML 解析,替换 workers)。
		// fail-safe:底稿读空(ConfigMap 缺失 / 瞬时读失败 / config.yaml 空)时【绝不】用内置最小默认去写——
		// 那会把 chart 的 server.port/proxy.add_routed_peer_header/health.endpoint 整段 clobber 掉,
		// 与【运行中 cart 启动时读到的 base】不一致 → cart 只接受「仅 workers 变化」的 reload → 整包拒绝 →
		// workers 永久冻结在启动集合(2026-08-13 mf-fallback 踩坑:live 卡 4 而实际后端 8)。
		// 保留现有 ConfigMap(base + 上次 workers),本轮跳过 cart 写入、requeue 重试;discovery/nginx 照常走。
		base := r.readConfigMapKey(ctx, c.OutputConfigMap, "config.yaml", rb.Namespace)
		if base == "" {
			log.Info("cart base config.yaml not readable, keep last cart config (fail-safe, no clobber)", "cm", c.OutputConfigMap)
		} else {
			cartYAML, rerr := sink.RenderCart(base, backends, c.MaxLoad)
			if rerr != nil {
				return ctrl.Result{}, fmt.Errorf("render cart config: %w", rerr)
			}
			if err := r.writeConfigMap(ctx, &rb, c.OutputConfigMap, map[string]string{"config.yaml": cartYAML}); err != nil {
				return r.configMapWriteResult(ctx, req.NamespacedName, len(backends), 0, c.OutputConfigMap, err)
			}
		}
		// CART 作为 openresty 的 priority-1 单一上游:走 CART Service 的 ClusterIP(VIP)而非 pod IP。
		// CART 是 hagate master-standby(Service selector 带 active 标签 → VIP 恒指当前 leader);用 VIP →
		// CART rollout / failover 对 openresty 透明(VIP 不变、kube-proxy 维护),autoconfig 无需重写/reload。
		// 没配 service(仅 selector)时退回 pod IP 发现(拿不到 VIP)。
		cartTarget := discoveryTarget(c.Service, c.Selector, c.Port, false, rb.Namespace)
		if cartTarget.Service != "" {
			cartPeers, err = discovery.ServiceClusterIP(ctx, r.Clientset, cartTarget)
		} else {
			cartPeers, err = discovery.Discover(ctx, r.Clientset, cartTarget)
		}
		if err != nil {
			r.setStatus(ctx, req.NamespacedName, len(backends), 0, false, "DiscoverError", fmt.Sprintf("discover cart: %v", err))
			return ctrl.Result{RequeueAfter: resyncEvery}, nil
		}
	}

	// 3) nginx:按 peers 组(cart 优先 + backend + backend-svc 兜底)拼 peers → 模板生成整条 conf
	peersByTarget := map[string][]config.Peer{"backend": backends, "cart": cartPeers}

	// backend-svc:后端 Service 的 ClusterIP(VIP)作最低优先级【静态兜底】peer(见 sample:use:backend-svc,priority:-1)。
	// 意义:autoconfig(operator)宕 + 后端 rollout 时,openresty 里 pod-IP 层是死 IP 且没人重写 → 若无兜底则全断;
	// VIP 由 kube-proxy 维护、operator 不参与,pod-IP 层全 banned 后 openresty 级联到它 → 降级(无亲和)但不全断。
	// 仅当有 peer 引用 backend-svc、且后端走 Service 发现(selector 无 VIP)时解析;解析不到就跳过(不阻塞路由)。
	if usesBackendSvc(rb.Spec.Nginx.Peers) {
		if svc := rb.Spec.Discovery.Service; svc != "" {
			svcPeers, serr := discovery.ServiceClusterIP(ctx, r.Clientset, discoveryTarget(svc, "", rb.Spec.Discovery.Port, false, rb.Namespace))
			if serr != nil {
				log.Error(serr, "解析 backend-svc ClusterIP 兜底失败,跳过兜底 peer")
			} else {
				peersByTarget["backend-svc"] = svcPeers
			}
		} else {
			log.Info("peers 引用 backend-svc 但 discovery 用 selector(无 ClusterIP)——跳过兜底 peer")
		}
	}
	// 后端单实例并发(供 cart 动态并发用):backend 组的 maxConcurrency。
	// CEL 校验保证「maxConcurrencyFromBackend → backend.maxConcurrency>0」,故不需 default_max 兜底;
	// 万一为 0(校验被绕过),cart 并发=0 → 运行时 nginx 用 conf 里的 default_max 兜。
	backendPerInstance := 0
	for _, s := range rb.Spec.Nginx.Peers {
		if s.Use == "backend" && s.MaxConcurrency > 0 {
			backendPerInstance = s.MaxConcurrency
		}
	}
	var sources []config.RouteSource
	for _, s := range rb.Spec.Nginx.Peers {
		if s.Use == "cart" && rb.Spec.Cart == nil {
			continue // 声明了 cart 来源但没配 cart,跳过
		}
		if s.Use == "backend-svc" && len(peersByTarget["backend-svc"]) == 0 {
			continue // 兜底 VIP 没解析到(selector 模式 / 解析失败),跳过
		}
		mc := s.MaxConcurrency
		if s.Use == "cart" && s.MaxConcurrencyFromBackend {
			mc = backendPerInstance * len(backends) // CART 总容量 = 后端单实例并发 × 后端数(随扩缩自动变)
		}
		// 健康探测路径:cart 层默认 /health(其 /v1/models 是缓存端点、worker 全挂也返 200,不能当健康信号);
		// 其余层默认空=用 route 的 /v1/models。显式 probePath 覆盖(含把 cart 设回 /v1/models)。
		probePath := s.ProbePath
		if probePath == "" && s.Use == "cart" {
			probePath = "/health"
		}
		sources = append(sources, config.RouteSource{Target: s.Use, Priority: s.Priority, MaxConcurrency: mc, ProbePath: probePath})
	}
	route := nginxRoute(&rb)
	// 有 backend-svc VIP 兜底层时,默认开跨层 retry:高优层(如 cart)返 5xx → proxy_next_upstream 单请求
	// 即刻兜到低优 VIP,不必等 health-timer ban 掉高优层(省 ~30-45s 空窗)。用户在 nginx.values 显式设则尊重。
	extra := rb.Spec.Nginx.Values
	if usesBackendSvc(rb.Spec.Nginx.Peers) {
		extra = withDefaults(extra, map[string]string{"cross_tier_fallback": "true", "max_more_tries": "3"})
	}
	// SLO(可选):把匹配本路由的 LLMSLORequirement 翻译成 ttft_metrics / tps_metrics,
	// 与其它调优项一起渲染进同一份 conf(改 CRD → 重写 conf → reload sidecar SIGHUP)。
	// 翻译失败**不阻塞 peers 下发** —— peers 才是"不下发就断流"的东西;SLO 缺席只是让
	// 引擎回落到 conf 里的静态阈值(与接 CRD 前一致)。结果写进 SLOSynced condition
	// (见 syncSLOCondition:为什么不打 Ready=false、以及为什么不能只打日志)。
	raw := map[string]string(nil)
	if s := rb.Spec.SLO; s != nil {
		m, warns, found, serr := r.sloMetricsFor(ctx, &rb, s)
		for _, w := range warns {
			log.Info("SLO: " + w)
		}
		if serr != nil {
			log.Error(serr, "翻译 LLMSLORequirement 失败,本轮不下发 SLO(openresty 回落静态阈值)")
		} else if !m.Empty() {
			raw = sink.WithSLOMetrics(nil, m)
		}
		r.syncSLOCondition(ctx, &rb, m, warns, found, serr)
	} else {
		// spec.slo 被移除:摘掉 SLOSynced —— 否则曾经的 False/TranslateError 会永久留在
		// status 上,describe 一直显示一个吓人的失败态,而这条路由早就不用 SLO 了。
		r.clearSLOCondition(ctx, &rb)
	}
	conf, err := sink.RenderRoute(sink.RouteData{
		Route: route,
		Extra: extra, // 任意调优项,原样渲染
		Raw:   raw,   // 已是 lua 字面量的片段(SLO 指标表)
		Peers: sink.ResolveSources(peersByTarget, sources, "backend"),
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("render route: %w", err)
	}
	if err := r.writeConfigMap(ctx, &rb, rb.Spec.Nginx.OutputConfigMap,
		map[string]string{openrestyKey(route): conf}); err != nil {
		return r.configMapWriteResult(ctx, req.NamespacedName, len(backends), len(cartPeers), rb.Spec.Nginx.OutputConfigMap, err)
	}

	// 4) monitor(可选):service(后端)+ nginx(openresty 入口)+ router(CART)行(共享 ConfigMap,每模型一个 key)
	if m := rb.Spec.Monitor; m != nil {
		var nginxPeers, routerPeers []config.Peer
		nginxName := ""
		// nginx: 行 —— 复用 spec.nginx 的入口 Service/selector(默认开;spec.monitor.nginx:false 关;没配 service/selector 则跳过)
		if (m.Nginx == nil || *m.Nginx) && (rb.Spec.Nginx.Service != "" || rb.Spec.Nginx.Selector != "") {
			// 端口用【本模型的 nginx.listen】→ monitor 每个 nginx 端口代表一个模型(每模型独立 key,不 dedup)
			nginxTarget := discoveryTarget(rb.Spec.Nginx.Service, rb.Spec.Nginx.Selector, 0, false, rb.Namespace)
			nginxTarget.PortName = dispatchPortName // 按名从 openresty Service 取 dispatch 端口(号在 chart)
			// 探 openresty 入口用【Service VIP】而非 pod IP:openresty 是 master-standby,active pod 随
			// 切换/滚动 churn → 探已删的 pod IP 会 Connection refused 误报。VIP 恒指 active leader,稳定不 churn。
			// (只配 selector、没建 Service 时无 VIP 可用 → 回退 pod IP 发现。)
			if rb.Spec.Nginx.Service != "" {
				nginxPeers, err = discovery.ServiceClusterIP(ctx, r.Clientset, nginxTarget)
			} else {
				nginxPeers, err = discovery.Discover(ctx, r.Clientset, nginxTarget)
			}
			if err != nil {
				r.setStatus(ctx, req.NamespacedName, len(backends), len(cartPeers), false, "DiscoverError", fmt.Sprintf("discover nginx: %v", err))
				return ctrl.Result{RequeueAfter: resyncEvery}, nil
			}
			if nginxName = m.Model; nginxName == "" { // 名字按模型,每模型/每端口一条 nginx 行
				nginxName = rb.Name
			}
		}
		if rb.Spec.Cart != nil && (m.Router == nil || *m.Router) { // 复用已探测的 CART pod 作 router
			routerPeers = cartPeers
		}
		// gpuType:显式配了整条 route 用显式覆盖;为空则用上面逐 peer 标注的结果(支持混布)。
		monConf := sink.RenderMonitor(rb.Name, m.Model, m.GPUType, nginxName, route, backends, nginxPeers, routerPeers)
		if err := r.writeConfigMap(ctx, &rb, m.OutputConfigMap,
			map[string]string{monitorKey(rb.Name): monConf}); err != nil {
			return r.configMapWriteResult(ctx, req.NamespacedName, len(backends), len(cartPeers), m.OutputConfigMap, err)
		}
	}

	// 5) 回写 status
	r.setStatus(ctx, req.NamespacedName, len(backends), len(cartPeers), true, "Synced", "synced")
	return ctrl.Result{RequeueAfter: resyncEvery}, nil
}

// ── SLO(LLMSLORequirement → session_route_<route>.conf 的 ttft_metrics / tps_metrics)──

// sloServiceID 读本路由声明的 serviceId。**只读显式字段,不做推导。**
//
// 早先这里会在字段为空时从 discovery.service 截掉 "-leader" 后缀推一个出来。已删:
// 那是拿命名规则猜「这条路由该读谁的 SLO」,猜错不报错 —— 要么静默回落静态阈值,
// 要么套用别的服务的 SLO。关联关系写在 spec.slo.serviceId 里,是唯一真相。
func sloServiceID(rb *routingv1.ModelRoute) string {
	if rb.Spec.SLO == nil {
		return "" // 防御:两个现有调用点都已 guard,但别让第三个调用点踩 nil panic
	}
	return rb.Spec.SLO.ServiceID
}

// sloMetricsFor 查同 ns 里 serviceId 匹配的 LLMSLORequirement,翻译成 lua 指标表。
// 找不到 → 返回空(引擎回落静态阈值)。这一点必须是"空"而不是"保留上次的值":
// CRD 被删之后如果还留着旧指标,阈值会永久冻结在删除前那一版。
// 这里天然满足 —— conf 每轮都整份重渲染,没写进去就是没有。
// found 区分两种「没有指标」:CRD 压根不存在 vs CRD 存在但没产出可用的 default metrics
// (比如只写了 ranges —— 本期不支持)。两者都回落静态阈值,但排查方向完全相反,
// 混成一个 "NoRequirement" 会把人指向错误的地方(去查 serviceId 对不对,而真正的原因
// 是"你写的是 ranges")。
func (r *ModelRouteReconciler) sloMetricsFor(ctx context.Context, rb *routingv1.ModelRoute, s *routingv1.SLOSpec) (m sink.SLOMetrics, warns []string, found bool, err error) {
	sid := sloServiceID(rb)
	if sid == "" {
		return sink.SLOMetrics{}, nil, false, fmt.Errorf("spec.slo.serviceId 为空 —— 必须显式声明要读哪个 LLMSLORequirement(不再从 Service 名推导)")
	}
	var list slov1.LLMSLORequirementList
	if err := r.List(ctx, &list, client.InNamespace(rb.Namespace)); err != nil {
		return sink.SLOMetrics{}, nil, false, fmt.Errorf("list LLMSLORequirement: %w", err)
	}
	// 按 spec.serviceId 精确匹配(不是按对象名 —— 两者可以不同)。
	// 同 ns 内 serviceId 重复时**报错而非取第一个**:List 的顺序没有保证,取第一个会让
	// 生效的是哪份 SLO 随 informer 缓存顺序漂移,阈值时而 A 时而 B、还查不出原因。
	var hit *slov1.LLMSLORequirement
	for i := range list.Items {
		if list.Items[i].Spec.ServiceID != sid {
			continue
		}
		if hit != nil {
			return sink.SLOMetrics{}, nil, false, fmt.Errorf(
				"同 ns 内有多个 serviceId=%s 的 LLMSLORequirement(%s、%s ...)—— 无法确定用哪份,请删掉重复的",
				sid, hit.Name, list.Items[i].Name)
		}
		hit = &list.Items[i]
	}
	if hit == nil {
		return sink.SLOMetrics{}, nil, false, nil
	}
	m, warns, err = sink.RenderSLOMetrics(hit.Spec)
	return m, warns, true, err
}

// syncSLOCondition 把 SLO 下发结果写进 status 的 SLOSynced condition。
//
// 为什么**不**把 Ready 打成 false:SLO 下发不了不影响转发 —— peers 照常下发,引擎回落到
// conf 里的静态阈值。把 Ready 打成 false 会让「这条路由通不通」这个信号失真。
//
// 为什么**不能只打日志**:本文件其余所有失败路径(DiscoverError / NoBackends /
// ConfigMapMissing)都写 status,理由同一条 —— 让 kubectl describe 看得到。而 SLO 这里最常见
// 的失败是**永久性**的:selector 发现的路由推不出 serviceId(没有 Service 名可截),
// 只进 operator 日志的话,用户会看到一个 Ready=true、却永远没有 SLO 的路由,毫无线索。
//
// 只在**内容真的变了**时才写 —— 否则每 10s resync 都会打一次 status update。
func (r *ModelRouteReconciler) syncSLOCondition(ctx context.Context, rb *routingv1.ModelRoute,
	m sink.SLOMetrics, warns []string, found bool, serr error) {

	cond := metav1.Condition{Type: sloCondType, Status: metav1.ConditionTrue, Reason: "Synced"}
	switch {
	case serr != nil:
		cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, "TranslateError", serr.Error()
	case !found:
		// 同 ns 里压根没有这个 serviceId 的 CRD。不是错误(引擎回落静态),但要能看出来 ——
		// 否则「配了 slo 却没生效」和「CRD 本来就没写」分不开。
		cond.Reason = "NoRequirement"
		cond.Message = fmt.Sprintf("同 ns 内没有 serviceId=%s 的 LLMSLORequirement,使用静态阈值", sloServiceID(rb))
	case m.Empty():
		// CRD **存在**,但没产出可用指标 —— 最典型的就是只写了 ranges(本期不支持)。
		// 这一条以前和上面那条共用 "NoRequirement" + 「没有匹配的 LLMSLORequirement」,
		// 会把人指去查 serviceId 对不对,而真正的原因写在 warns 里。
		cond.Reason = "NothingApplicable"
		cond.Message = fmt.Sprintf("serviceId=%s 的 LLMSLORequirement 存在,但没有可用的 default metrics,使用静态阈值", sloServiceID(rb))
	default:
		cond.Message = "ttft_metrics/tps_metrics 已下发"
	}
	// warns(如 ranges 被忽略)在**所有**分支都带上 —— 它往往就是"为什么没生效"的答案
	if len(warns) > 0 {
		cond.Message += ";" + strings.Join(warns, ";")
	}

	// 与已有的 condition 比对,一致就不写(避免 resync 每轮一次 status update)
	for _, c := range rb.Status.Conditions {
		if c.Type == cond.Type && c.Status == cond.Status &&
			c.Reason == cond.Reason && c.Message == cond.Message {
			return
		}
	}
	r.updateStatus(ctx, rb, "写 SLOSynced condition 失败", func(cur *routingv1.ModelRoute) {
		cond.LastTransitionTime = metav1.Now()
		cond.ObservedGeneration = cur.Generation
		setCondition(&cur.Status.Conditions, cond)
	})
}

// clearSLOCondition 在 spec.slo 被移除后摘掉 SLOSynced。
// 不做的话,之前留下的 False/TranslateError 会永久挂在 status 上,describe 一直显示失败态,
// 而这条路由早就不用 SLO 了。只在确实存在时才写。
func (r *ModelRouteReconciler) clearSLOCondition(ctx context.Context, rb *routingv1.ModelRoute) {
	present := false
	for _, c := range rb.Status.Conditions {
		if c.Type == sloCondType {
			present = true
			break
		}
	}
	if !present {
		return
	}
	r.updateStatus(ctx, rb, "摘 SLOSynced condition 失败", func(cur *routingv1.ModelRoute) {
		removeCondition(&cur.Status.Conditions, sloCondType)
	})
}

// updateStatus 读-改-写 status 的公共壳(RetryOnConflict 内重新 Get,避免拿陈旧对象覆盖)。
func (r *ModelRouteReconciler) updateStatus(ctx context.Context, rb *routingv1.ModelRoute, failMsg string, mutate func(*routingv1.ModelRoute)) {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur routingv1.ModelRoute
		if err := r.Get(ctx, types.NamespacedName{Namespace: rb.Namespace, Name: rb.Name}, &cur); err != nil {
			return err
		}
		mutate(&cur)
		return r.Status().Update(ctx, &cur)
	})
	if err != nil {
		logf.FromContext(ctx).Error(err, failMsg)
	}
}

// modelRoutesForSLO:LLMSLORequirement 变化 → 找出同 ns 里 serviceId 匹配的 ModelRoute 入队。
// 没有它,改 CRD 阈值要等 10s resync 才生效(能接受,但事件驱动几乎零成本)。
func (r *ModelRouteReconciler) modelRoutesForSLO(ctx context.Context, obj client.Object) []reconcile.Request {
	slo, ok := obj.(*slov1.LLMSLORequirement)
	if !ok || slo.Spec.ServiceID == "" {
		return nil
	}
	var mrs routingv1.ModelRouteList
	if err := r.List(ctx, &mrs, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range mrs.Items {
		if mrs.Items[i].Spec.SLO == nil {
			continue
		}
		if sloServiceID(&mrs.Items[i]) == slo.Spec.ServiceID {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: mrs.Items[i].Namespace, Name: mrs.Items[i].Name}})
		}
	}
	return reqs
}

// openrestyKey 是这条路由在 openresty ConfigMap 里的 key(= 文件名)。
func openrestyKey(route string) string { return "session_route_" + route + ".conf" }

// nginxRoute 返回本路由的短名:spec.nginx.route 优先,省略则用 metadata.name。
func nginxRoute(rb *routingv1.ModelRoute) string {
	if rb.Spec.Nginx.Route != "" {
		return rb.Spec.Nginx.Route
	}
	return rb.Name
}

// monitorKey 是这个模型在共享 monitor ConfigMap 里的 key。
func monitorKey(name string) string { return name + ".monitor.conf" }

// writeConfigMap 把 data 的 key merge 进目标 ConfigMap(其余 key 保留)。
// **只更新已有 ConfigMap,不创建**——三个目标 CM(cart/openresty/monitor)的生命周期都归 helm chart / 手工所有,
// autoconfig 只改内容(不设 ownerRef、不创建、删 RB 时只摘 key)。不存在则返回 NotFound(由调用方转成 status 提示 + 重试等 chart)。
func (r *ModelRouteReconciler) writeConfigMap(ctx context.Context, rb *routingv1.ModelRoute, ref string, data map[string]string) error {
	ns, name := splitNSName(ref, rb.Namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cm corev1.ConfigMap
		if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &cm); err != nil {
			return err // 含 NotFound —— 不创建,交调用方处理
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		changed := false
		for k, v := range data {
			if cm.Data[k] != v {
				cm.Data[k] = v
				changed = true
			}
		}
		if !changed {
			return nil // 无变化不写,避免多余 reload
		}
		return r.Update(ctx, &cm)
	})
}

// configMapWriteResult 把 writeConfigMap 的错误转成 reconcile 结果:目标 CM 不存在(chart 还没建)→
// 写 status(ConfigMapMissing)+ 重试等它;其它错误 → 返回让 controller-runtime 退避重试。
func (r *ModelRouteReconciler) configMapWriteResult(ctx context.Context, nn types.NamespacedName, backends, cartPeers int, ref string, err error) (ctrl.Result, error) {
	if apierrors.IsNotFound(err) {
		r.setStatus(ctx, nn, backends, cartPeers, false, "ConfigMapMissing", fmt.Sprintf("目标 ConfigMap %q 不存在(chart 未建?);autoconfig 只更新不创建", ref))
		return ctrl.Result{RequeueAfter: resyncEvery}, nil
	}
	return ctrl.Result{}, fmt.Errorf("write configmap %s: %w", ref, err)
}

// cleanupSharedKeys 删 RB 时,从【共享多-key】ConfigMap(openresty / 可选 monitor)里摘掉自己那个 key。
// ConfigMap 本体归 chart / 手工所有,不由 autoconfig 删除。
//
// ⚠️ 【不碰 cart ConfigMap】:cart 是每模型【独占】ConfigMap,整份内容就是 `config.yaml` 这一个 key
// (chart 建的 baseConfig + autoconfig 填的 workers),不是共享多-key。删掉 config.yaml = 把 cart 配置整个清空
// → 下次 reconcile 读到 base="" → RenderCart 本会 clobber 成最小默认(6700/无 proxy)→ 与运行中 cart 不一致
// → cart 只接受「仅 workers 变化」的 reload → 整包拒绝 → workers 永久冻结(2026-08-13 mf-fallback 因 route
// 改名 delete+readd MR 踩坑)。MR 删掉后 cart 也随之下线,残留的 workers 无害;MR 若再加回,base 原样保留。
func (r *ModelRouteReconciler) cleanupSharedKeys(ctx context.Context, rb *routingv1.ModelRoute) error {
	if err := r.removeConfigMapKey(ctx, rb.Spec.Nginx.OutputConfigMap, openrestyKey(nginxRoute(rb)), rb.Namespace); err != nil {
		return err
	}
	if m := rb.Spec.Monitor; m != nil {
		if err := r.removeConfigMapKey(ctx, m.OutputConfigMap, monitorKey(rb.Name), rb.Namespace); err != nil {
			return err
		}
	}
	return nil
}

// removeConfigMapKey 从共享 ConfigMap 里删掉一个 key(不存在则跳过)。
func (r *ModelRouteReconciler) removeConfigMapKey(ctx context.Context, ref, key, defaultNS string) error {
	ns, name := splitNSName(ref, defaultNS)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cm corev1.ConfigMap
		err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &cm)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, ok := cm.Data[key]; !ok {
			return nil
		}
		delete(cm.Data, key)
		return r.Update(ctx, &cm)
	})
}

func (r *ModelRouteReconciler) setStatus(ctx context.Context, nn types.NamespacedName, backends, cartPeers int, ready bool, reason, msg string) {
	log := logf.FromContext(ctx)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var rb routingv1.ModelRoute
		if err := r.Get(ctx, nn, &rb); err != nil {
			return err
		}
		rb.Status.Backends = backends
		rb.Status.CartPeers = cartPeers
		rb.Status.Ready = ready
		rb.Status.ObservedGeneration = rb.Generation
		now := metav1.Now()
		rb.Status.LastSyncTime = &now
		cond := metav1.Condition{
			Type: "Ready", Reason: reason, Message: msg, LastTransitionTime: now,
			Status: metav1.ConditionFalse, ObservedGeneration: rb.Generation,
		}
		if ready {
			cond.Status = metav1.ConditionTrue
		}
		setCondition(&rb.Status.Conditions, cond)
		return r.Status().Update(ctx, &rb)
	})
	if err != nil {
		log.Error(err, "update status failed")
	}
}

func removeCondition(conds *[]metav1.Condition, typ string) {
	out := (*conds)[:0]
	for _, c := range *conds {
		if c.Type != typ {
			out = append(out, c)
		}
	}
	*conds = out
}

func setCondition(conds *[]metav1.Condition, c metav1.Condition) {
	for i := range *conds {
		if (*conds)[i].Type == c.Type {
			if (*conds)[i].Status == c.Status {
				c.LastTransitionTime = (*conds)[i].LastTransitionTime // 状态没变不刷时间
			}
			(*conds)[i] = c
			return
		}
	}
	*conds = append(*conds, c)
}

// readConfigMapKey 读某 ConfigMap("ns/name" 或裸名)的一个 key,不存在返回 ""。
func (r *ModelRouteReconciler) readConfigMapKey(ctx context.Context, ref, key, defaultNS string) string {
	ns, name := splitNSName(ref, defaultNS)
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &cm); err != nil {
		return ""
	}
	return cm.Data[key]
}

func splitNSName(ref, defaultNS string) (ns, name string) {
	if i := strings.Index(ref, "/"); i >= 0 {
		return ref[:i], ref[i+1:]
	}
	return defaultNS, ref
}

// discoveryTarget 构造发现 Target。service 支持 "ns/name"(跨 ns 发现,让 ModelRoute 可放别的 ns),
// 裸名默认用 ModelRoute 的 ns。selector 路径不支持 ns/name(用默认 ns)。
// usesBackendSvc 判断 peers 里有没有引用 backend-svc(后端 Service VIP 兜底)来源。
func usesBackendSvc(peers []routingv1.RoutePeer) bool {
	for _, p := range peers {
		if p.Use == "backend-svc" {
			return true
		}
	}
	return false
}

// withDefaults 返回 base 的副本并补上 defaults 里 base 未设的 key(用户在 base 显式设的值优先,不覆盖)。
func withDefaults(base, defaults map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range defaults {
		if _, ok := out[k]; !ok {
			out[k] = v
		}
	}
	return out
}

func discoveryTarget(service, selector string, port int, includeNotReady bool, defaultNS string) config.Target {
	ns := defaultNS
	if service != "" {
		ns, service = splitNSName(service, defaultNS)
	}
	return config.Target{Namespace: ns, Service: service, Selector: selector, Port: port, IncludeNotReady: includeNotReady}
}

// SetupWithManager 注册两类发现的事件驱动:
//   - watch EndpointSlice → 映射到「用 service 发现」的 ModelRoute(后端/CART/openresty 走 Service 路径)
//   - watch Pod          → 映射到「用 selector 发现」的 ModelRoute(没建 Service 的单机/单卡走 pod label 路径)
//
// 两条都在端点变化(增删/ready 翻转都会 bump resourceVersion → 元数据 watch 收到事件)时立即重算,
// 不用等 10s resync;10s resync 仅作兜底。两个 watch 都用 OnlyMetadata(cache 只存元数据,不存 spec/status/endpoints)。
func (r *ModelRouteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&routingv1.ModelRoute{}).
		Watches(&discoveryv1.EndpointSlice{}, handler.EnqueueRequestsFromMapFunc(r.modelRoutesForEndpointSlice), builder.OnlyMetadata).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.modelRoutesForPod), builder.OnlyMetadata).
		// LLMSLORequirement 不能用 OnlyMetadata:映射函数要读 spec.serviceId 才知道该唤醒谁。
		Watches(&slov1.LLMSLORequirement{}, handler.EnqueueRequestsFromMapFunc(r.modelRoutesForSLO)).
		Complete(r)
}

// modelRoutesForPod:Pod 变化 → 找出「用 selector(pod label)发现」且 selector 命中该 pod 的 ModelRoute 入队。
// 只处理 selector 路径(service 路径由 EndpointSlice watch 负责);selector 发现按 ModelRoute 自身 ns,故只匹配同 ns 的 pod。
func (r *ModelRouteReconciler) modelRoutesForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	podLabels := obj.GetLabels()
	if len(podLabels) == 0 {
		return nil
	}
	ns := obj.GetNamespace()
	var mrs routingv1.ModelRouteList
	if err := r.List(ctx, &mrs); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range mrs.Items {
		if referencesPodBySelector(&mrs.Items[i], ns, podLabels) {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: mrs.Items[i].Namespace, Name: mrs.Items[i].Name}})
		}
	}
	return reqs
}

// referencesPodBySelector 判断 mr 的 discovery/cart/nginx 里有没有「selector 路径」命中 (podNS, podLabels) 的 pod。
// selector 路径 = 该 target 只配了 selector 没配 service,且发现 ns = mr.Namespace(selector 不支持 ns/name)。
func referencesPodBySelector(mr *routingv1.ModelRoute, podNS string, podLabels map[string]string) bool {
	if mr.Namespace != podNS {
		return false
	}
	sel := func(service, selector string) bool {
		if service != "" || selector == "" {
			return false // 走 service 路径,或没配 selector
		}
		s, err := labels.Parse(selector)
		return err == nil && s.Matches(labels.Set(podLabels))
	}
	if sel(mr.Spec.Discovery.Service, mr.Spec.Discovery.Selector) {
		return true
	}
	if c := mr.Spec.Cart; c != nil && sel(c.Service, c.Selector) {
		return true
	}
	if sel(mr.Spec.Nginx.Service, mr.Spec.Nginx.Selector) { // nginx 入口 selector 路径
		return true
	}
	return false
}

// modelRoutesForEndpointSlice:EndpointSlice 变化 → 找出 discovery/cart/nginx.service 指向其 Service 的 ModelRoute 入队。
func (r *ModelRouteReconciler) modelRoutesForEndpointSlice(ctx context.Context, obj client.Object) []reconcile.Request {
	svc := obj.GetLabels()["kubernetes.io/service-name"]
	if svc == "" {
		return nil
	}
	ns := obj.GetNamespace()
	var mrs routingv1.ModelRouteList
	if err := r.List(ctx, &mrs); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range mrs.Items {
		if referencesService(&mrs.Items[i], ns, svc) {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: mrs.Items[i].Namespace, Name: mrs.Items[i].Name}})
		}
	}
	return reqs
}

// referencesService 判断 mr 的 discovery/cart/nginx.service 是否指向 (ns, svc)(支持 ns/name 跨 ns)。
func referencesService(mr *routingv1.ModelRoute, ns, svc string) bool {
	match := func(ref string) bool {
		if ref == "" {
			return false
		}
		rns, rname := splitNSName(ref, mr.Namespace)
		return rns == ns && rname == svc
	}
	if match(mr.Spec.Discovery.Service) {
		return true
	}
	if c := mr.Spec.Cart; c != nil && match(c.Service) {
		return true
	}
	if match(mr.Spec.Nginx.Service) { // nginx 入口 Service(供 monitor nginx 行);扩缩即时重算
		return true
	}
	return false
}
