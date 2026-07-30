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

	routingv1 "autoconfig/api/v1alpha1"
	"autoconfig/internal/config"
	"autoconfig/internal/discovery"
	"autoconfig/internal/sink"
)

const (
	finalizer = "routing.gpucluster.io/cleanup"
	// 重新发现的轮询周期(informer 事件之外的兜底 resync)。
	resyncEvery = 10 * time.Second
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
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch

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

	// 1) 发现后端桶(喂 CART workers + openresty backend 来源)
	backends, err := discovery.Discover(ctx, r.Clientset, discoveryTarget(
		rb.Spec.Discovery.Service, rb.Spec.Discovery.Selector, rb.Spec.Discovery.Port, rb.Spec.Discovery.IncludeNotReady, rb.Namespace))
	if err != nil {
		// 发现失败(如端口不唯一、Service 不存在):写进 status 让 kubectl describe 看得到,而非只进日志
		r.setStatus(ctx, req.NamespacedName, 0, 0, false, "DiscoverError", fmt.Sprintf("discover backends: %v", err))
		return ctrl.Result{RequeueAfter: resyncEvery}, nil
	}
	if len(backends) == 0 {
		// fail-safe:绝不写空(CART 拒空 workers、openresty 会丢全部流量)。标记 not-ready 后重试。
		log.Info("0 ready backends, keep last config (fail-safe)")
		r.setStatus(ctx, req.NamespacedName, 0, 0, false, "NoBackends", "no ready backends")
		return ctrl.Result{RequeueAfter: resyncEvery}, nil
	}

	// 2) CART(可选):渲染 workers 写 cart-config,并发现 CART pod 供 openresty 引用
	var cartPeers []config.Peer
	if c := rb.Spec.Cart; c != nil {
		// 底稿(server/cache/health)来自 chart 建的 cart-config(values.baseConfig);autoconfig 只覆盖 workers 键。
		// 读现有 config.yaml 当底稿(YAML 解析,替换 workers);读不到则用内置默认。
		cartYAML, rerr := sink.RenderCart(r.readConfigMapKey(ctx, c.OutputConfigMap, "config.yaml", rb.Namespace), backends, c.MaxLoad)
		if rerr != nil {
			return ctrl.Result{}, fmt.Errorf("render cart config: %w", rerr)
		}
		if err := r.writeConfigMap(ctx, &rb, c.OutputConfigMap, map[string]string{"config.yaml": cartYAML}); err != nil {
			return r.configMapWriteResult(ctx, req.NamespacedName, len(backends), 0, c.OutputConfigMap, err)
		}
		cartPeers, err = discovery.Discover(ctx, r.Clientset, discoveryTarget(c.Service, c.Selector, c.Port, false, rb.Namespace))
		if err != nil {
			r.setStatus(ctx, req.NamespacedName, len(backends), 0, false, "DiscoverError", fmt.Sprintf("discover cart: %v", err))
			return ctrl.Result{RequeueAfter: resyncEvery}, nil
		}
	}

	// 3) openresty:按 peers 组(cart 优先 + backend 兜底)拼 peers → 模板生成整条 conf
	peersByTarget := map[string][]config.Peer{"backend": backends, "cart": cartPeers}
	// 后端单实例并发(供 cart 动态并发用):backend 组的 maxConcurrency。
	// CEL 校验保证「maxConcurrencyFromBackend → backend.maxConcurrency>0」,故不需 default_max 兜底;
	// 万一为 0(校验被绕过),cart 并发=0 → 运行时 openresty 用 conf 里的 default_max 兜。
	backendPerInstance := 0
	for _, s := range rb.Spec.Openresty.Peers {
		if s.Use == "backend" && s.MaxConcurrency > 0 {
			backendPerInstance = s.MaxConcurrency
		}
	}
	var sources []config.RouteSource
	for _, s := range rb.Spec.Openresty.Peers {
		if s.Use == "cart" && rb.Spec.Cart == nil {
			continue // 声明了 cart 来源但没配 cart,跳过
		}
		mc := s.MaxConcurrency
		if s.Use == "cart" && s.MaxConcurrencyFromBackend {
			mc = backendPerInstance * len(backends) // CART 总容量 = 后端单实例并发 × 后端数(随扩缩自动变)
		}
		sources = append(sources, config.RouteSource{Target: s.Use, Priority: s.Priority, MaxConcurrency: mc})
	}
	conf, err := sink.RenderRoute(sink.RouteData{
		Route:  rb.Spec.Openresty.Route,
		Listen: rb.Spec.Openresty.Listen,
		Extra:  rb.Spec.Openresty.Values, // 任意调优项,原样渲染
		Peers:  sink.ResolveSources(peersByTarget, sources, "backend"),
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("render route: %w", err)
	}
	if err := r.writeConfigMap(ctx, &rb, rb.Spec.Openresty.OutputConfigMap,
		map[string]string{openrestyKey(rb.Spec.Openresty.Route): conf}); err != nil {
		return r.configMapWriteResult(ctx, req.NamespacedName, len(backends), len(cartPeers), rb.Spec.Openresty.OutputConfigMap, err)
	}

	// 4) monitor(可选):service(后端)+ nginx(openresty 入口)+ router(CART)行(共享 ConfigMap,每模型一个 key)
	if m := rb.Spec.Monitor; m != nil {
		var nginxPeers, routerPeers []config.Peer
		nginxName := ""
		if n := m.Nginx; n != nil { // 探测 openresty 入口 pod
			// 端口用【本模型的 openresty.listen】→ monitor 每个 nginx 端口代表一个模型(每模型独立 key,不 dedup)
			nginxPeers, err = discovery.Discover(ctx, r.Clientset, discoveryTarget(n.Service, n.Selector, rb.Spec.Openresty.Listen, n.IncludeNotReady, rb.Namespace))
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
		monConf := sink.RenderMonitor(rb.Name, m.Model, m.GPUType, nginxName, backends, nginxPeers, routerPeers)
		if err := r.writeConfigMap(ctx, &rb, m.OutputConfigMap,
			map[string]string{monitorKey(rb.Name): monConf}); err != nil {
			return r.configMapWriteResult(ctx, req.NamespacedName, len(backends), len(cartPeers), m.OutputConfigMap, err)
		}
	}

	// 5) 回写 status
	r.setStatus(ctx, req.NamespacedName, len(backends), len(cartPeers), true, "Synced", "synced")
	return ctrl.Result{RequeueAfter: resyncEvery}, nil
}

// openrestyKey 是这条路由在 openresty ConfigMap 里的 key(= 文件名)。
func openrestyKey(route string) string { return "session_route_" + route + ".conf" }

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

// cleanupSharedKeys 删 RB 时,从各共享 ConfigMap(cart / openresty / 可选 monitor)里摘掉自己那个 key。
// ConfigMap 本体归 chart / 手工所有,不由 autoconfig 删除。
func (r *ModelRouteReconciler) cleanupSharedKeys(ctx context.Context, rb *routingv1.ModelRoute) error {
	if c := rb.Spec.Cart; c != nil {
		if err := r.removeConfigMapKey(ctx, c.OutputConfigMap, "config.yaml", rb.Namespace); err != nil {
			return err
		}
	}
	if err := r.removeConfigMapKey(ctx, rb.Spec.Openresty.OutputConfigMap, openrestyKey(rb.Spec.Openresty.Route), rb.Namespace); err != nil {
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
func discoveryTarget(service, selector string, port int, includeNotReady bool, defaultNS string) config.Target {
	ns := defaultNS
	if service != "" {
		ns, service = splitNSName(service, defaultNS)
	}
	return config.Target{Namespace: ns, Service: service, Selector: selector, Port: port, IncludeNotReady: includeNotReady}
}

// SetupWithManager 注册:watch ModelRoute + watch EndpointSlice(后端/CART/openresty 端点变化事件驱动,
// 映射到引用其 Service 的 ModelRoute 立即重算,不用等 10s resync)。
// 注:selector(pod label)发现路径不经 EndpointSlice,仍靠 resync 兜底。
func (r *ModelRouteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&routingv1.ModelRoute{}).
		// OnlyMetadata:cache 只存 EndpointSlice 元数据(label/ns,mapFunc 只需这些),不存 endpoints 列表 → 省内存;
		// 实际端点发现走 clientset live List(不经 cache)。
		Watches(&discoveryv1.EndpointSlice{}, handler.EnqueueRequestsFromMapFunc(r.modelRoutesForEndpointSlice), builder.OnlyMetadata).
		Complete(r)
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
	if m := mr.Spec.Monitor; m != nil && m.Nginx != nil && match(m.Nginx.Service) {
		return true
	}
	return false
}
