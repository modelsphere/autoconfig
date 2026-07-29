// Package controller 是 RouterBinding 的 controller-runtime 调谐器。
// 复用 internal/discovery 与 internal/sink;只把「输入」从 ConfigMap 换成 CR,并回写 status。
package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	routingv1 "autoconfig/api/v1alpha1"
	"autoconfig/internal/config"
	"autoconfig/internal/discovery"
	"autoconfig/internal/sink"
)

const (
	finalizer = "routing.4pd.io/cleanup"
	// 重新发现的轮询周期(informer 事件之外的兜底 resync)。
	resyncEvery = 10 * time.Second
)

// RouterBindingReconciler 调谐 RouterBinding。
// Client 管 CR + ConfigMap;Clientset 供 discovery(EndpointSlice/pod 发现)。
type RouterBindingReconciler struct {
	client.Client
	Clientset kubernetes.Interface
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=routing.4pd.io,resources=routerbindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=routing.4pd.io,resources=routerbindings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=routing.4pd.io,resources=routerbindings/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch

func (r *RouterBindingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var rb routingv1.RouterBinding
	if err := r.Get(ctx, req.NamespacedName, &rb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 删除:跑 finalizer 清理(只摘掉共享 openresty ConfigMap 里自己那个 key)
	if !rb.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&rb, finalizer) {
			if err := r.cleanupOpenrestyKey(ctx, &rb); err != nil {
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
	backends, err := discovery.Discover(ctx, r.Clientset, config.Target{
		Namespace:       rb.Namespace,
		Service:         rb.Spec.Discovery.Service,
		Selector:        rb.Spec.Discovery.Selector,
		Port:            rb.Spec.Discovery.Port,
		IncludeNotReady: rb.Spec.Discovery.IncludeNotReady,
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("discover backends: %w", err)
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
		base := ""
		if ref := c.BaseConfigRef; ref != nil {
			var cm corev1.ConfigMap
			if err := r.Get(ctx, types.NamespacedName{Namespace: rb.Namespace, Name: ref.Name}, &cm); err != nil {
				return ctrl.Result{}, fmt.Errorf("read cart baseConfigRef %s/%s: %w", rb.Namespace, ref.Name, err)
			}
			base = cm.Data[ref.Key]
		}
		cartYAML := sink.RenderCart(base, backends, c.MaxLoad)
		if err := r.writeConfigMap(ctx, &rb, c.OutputConfigMap, map[string]string{"config.yaml": cartYAML}, true); err != nil {
			return ctrl.Result{}, fmt.Errorf("write cart configmap: %w", err)
		}
		cartPeers, err = discovery.Discover(ctx, r.Clientset, config.Target{
			Namespace: rb.Namespace, Service: c.Service, Selector: c.Selector, Port: c.Port,
		})
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("discover cart pods: %w", err)
		}
	}

	// 3) openresty:按 sources(cart 优先 + backend 兜底)拼 peers → 模板生成整条 conf
	peersByTarget := map[string][]config.Peer{"backend": backends, "cart": cartPeers}
	var sources []config.RouteSource
	for _, s := range rb.Spec.Openresty.Sources {
		if s.Use == "cart" && rb.Spec.Cart == nil {
			continue // 声明了 cart 来源但没配 cart,跳过
		}
		sources = append(sources, config.RouteSource{Target: s.Use, Priority: s.Priority, MaxConcurrency: s.MaxConcurrency})
	}
	values := map[string]interface{}{"route": rb.Spec.Openresty.Route, "listen": rb.Spec.Openresty.Listen}
	for k, v := range rb.Spec.Openresty.Values {
		values[k] = v
	}
	conf, err := sink.RenderRoute("", values, sink.ResolveSources(peersByTarget, sources, "backend"))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("render route: %w", err)
	}
	if err := r.writeConfigMap(ctx, &rb, rb.Spec.Openresty.OutputConfigMap,
		map[string]string{openrestyKey(rb.Spec.Openresty.Route): conf}, false); err != nil {
		return ctrl.Result{}, fmt.Errorf("write openresty configmap: %w", err)
	}

	// 4) 回写 status
	r.setStatus(ctx, req.NamespacedName, len(backends), len(cartPeers), true, "Synced", "synced")
	return ctrl.Result{RequeueAfter: resyncEvery}, nil
}

// openrestyKey 是这条路由在 openresty ConfigMap 里的 key(= 文件名)。
func openrestyKey(route string) string { return "session_route_" + route + ".conf" }

// writeConfigMap 把 data 的 key merge 进目标 ConfigMap(其余 key 保留),不存在则建。
// exclusive=true(cart 专属)时设 controllerReference → 删 RB 级联 GC;共享的 openresty 不设(靠 finalizer 摘 key)。
func (r *RouterBindingReconciler) writeConfigMap(ctx context.Context, rb *routingv1.RouterBinding, ref string, data map[string]string, exclusive bool) error {
	ns, name := splitNSName(ref, rb.Namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cm corev1.ConfigMap
		err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &cm)
		if apierrors.IsNotFound(err) {
			cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Data: map[string]string{}}
			for k, v := range data {
				cm.Data[k] = v
			}
			if exclusive && ns == rb.Namespace {
				if err := controllerutil.SetControllerReference(rb, &cm, r.Scheme); err != nil {
					return err
				}
			}
			return r.Create(ctx, &cm)
		}
		if err != nil {
			return err
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
		if exclusive && ns == rb.Namespace {
			if err := controllerutil.SetControllerReference(rb, &cm, r.Scheme); err == nil {
				changed = true
			}
		}
		if !changed {
			return nil // 无变化不写,避免多余 reload
		}
		return r.Update(ctx, &cm)
	})
}

// cleanupOpenrestyKey 删 RB 时,从共享 openresty ConfigMap 里摘掉自己那个 key。
func (r *RouterBindingReconciler) cleanupOpenrestyKey(ctx context.Context, rb *routingv1.RouterBinding) error {
	ns, name := splitNSName(rb.Spec.Openresty.OutputConfigMap, rb.Namespace)
	key := openrestyKey(rb.Spec.Openresty.Route)
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

func (r *RouterBindingReconciler) setStatus(ctx context.Context, nn types.NamespacedName, backends, cartPeers int, ready bool, reason, msg string) {
	log := logf.FromContext(ctx)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var rb routingv1.RouterBinding
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

func splitNSName(ref, defaultNS string) (ns, name string) {
	if i := strings.Index(ref, "/"); i >= 0 {
		return ref[:i], ref[i+1:]
	}
	return defaultNS, ref
}

// SetupWithManager 注册:watch RouterBinding + 自己拥有的 ConfigMap(cart-config)。
func (r *RouterBindingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&routingv1.RouterBinding{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}
