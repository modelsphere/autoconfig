package controller

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/controller-runtime/pkg/client"

	slov1 "autoconfig/api/inference/v1alpha1"
	routingv1 "autoconfig/api/v1alpha1"
)

func boolp(b bool) *bool { return &b }

func svcClusterIP(name, ns, clusterIP string, port int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       corev1.ServiceSpec{ClusterIP: clusterIP, Ports: []corev1.ServicePort{{Port: port}}},
	}
}

func epslice(name, svc, ns string, ips ...string) *discoveryv1.EndpointSlice {
	var eps []discoveryv1.Endpoint
	for _, ip := range ips {
		eps = append(eps, discoveryv1.Endpoint{Addresses: []string{ip}, Conditions: discoveryv1.EndpointConditions{Ready: boolp(true)}})
	}
	return &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"kubernetes.io/service-name": svc}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   eps,
	}
}

// 端到端调谐:发现后端(EndpointSlice)→ 写 cart-config workers + openresty CART优先/后端兜底 peers + 回写 status。
func TestReconcileGLM(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = routingv1.AddToScheme(scheme)

	rb := &routingv1.ModelRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "glm-5.1-fp8", Namespace: "glm"},
		Spec: routingv1.ModelRouteSpec{
			Discovery: routingv1.Discovery{Service: "glm-leader", Port: 8050},
			Cart: &routingv1.CartSpec{
				Service: "cart-glm", Port: 8071, OutputConfigMap: "glm/cart-config", MaxLoad: 20,
			},
			Nginx: routingv1.NginxSpec{
				Route: "glm", OutputConfigMap: "openresty/openresty-conf",
				Values: map[string]string{"ttft_limit_ms": "60000"},
				Peers: []routingv1.RoutePeer{
					{Use: "cart", Priority: 3, MaxConcurrencyFromBackend: true}, // 动态 = 后端并发 × 后端数
					{Use: "backend", Priority: 2, MaxConcurrency: 100},          // 单实例 100,2 个后端 → cart=200
					{Use: "backend-svc", Priority: 1},                           // 后端 Service VIP 静态兜底(最低优先级)
				},
			},
			Monitor: &routingv1.MonitorSpec{
				OutputConfigMap: "monitor/monitor-conf", Model: "glm-5.1-fp8", GPUType: "H100",
			},
		},
	}
	// chart 预建的 cart-config:底稿(server port 6700)+ 占位 workers;autoconfig 剥掉 workers 段重填
	// chart 预建的三个 ConfigMap(autoconfig 只更新不创建);cart-config 带底稿
	cmCart := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cart-config", Namespace: "glm"},
		Data:       map[string]string{"config.yaml": "server: { host: \"0.0.0.0\", port: 6700 }\nworkers: []"},
	}
	cmOR := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "openresty-conf", Namespace: "openresty"}}
	cmMon := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "monitor-conf", Namespace: "monitor"}}
	cl := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rb, cmCart, cmOR, cmMon).WithStatusSubresource(&routingv1.ModelRoute{}).Build()
	cs := k8sfake.NewSimpleClientset(
		epslice("glm-leader-1", "glm-leader", "glm", "10.1.0.1", "10.1.0.2"),
		epslice("cart-glm-1", "cart-glm", "glm", "10.9.0.1"),
		svcClusterIP("cart-glm", "glm", "10.96.0.71", 8071),   // cart peer 走这个 VIP(非 pod IP)
		svcClusterIP("glm-leader", "glm", "10.96.0.50", 8050), // backend-svc 兜底走后端 Service VIP
	)
	r := &ModelRouteReconciler{Client: cl, Clientset: cs, Scheme: scheme}
	nn := types.NamespacedName{Namespace: "glm", Name: "glm-5.1-fp8"}

	// 第一次:加 finalizer 并 requeue
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn})
	if err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if !res.Requeue {
		t.Fatalf("expected requeue after finalizer add")
	}
	// 第二次:真正干活
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}

	// cart-config:workers = 后端桶
	var cartCM corev1.ConfigMap
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "glm", Name: "cart-config"}, &cartCM); err != nil {
		t.Fatalf("get cart-config: %v", err)
	}
	cy := cartCM.Data["config.yaml"]
	for _, want := range []string{"workers:", `http://10.1.0.1:8050`, `http://10.1.0.2:8050`, "max_load: 20", "port: 6700"} {
		if !strings.Contains(cy, want) {
			t.Errorf("cart config.yaml missing %q\n%s", want, cy)
		}
	}
	if strings.Contains(cy, "10.9.0.1") {
		t.Errorf("cart workers 不应含 CART pod IP\n%s", cy)
	}

	// openresty-conf:CART priority-1(带 maxConcurrency)在前 + 后端 priority-0 在后
	var orCM corev1.ConfigMap
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "openresty", Name: "openresty-conf"}, &orCM); err != nil {
		t.Fatalf("get openresty-conf: %v", err)
	}
	conf := orCM.Data["session_route_glm.conf"]
	// cart 走 CART Service 的 ClusterIP(VIP);动态并发 = 后端 100 × 2 = 200;探针默认 /health(worker-aware,
	// 因 cart 的 /v1/models 是缓存端点、worker 全挂也返 200,不能当健康信号 → 否则 cart 永不 ban、兜底被 mask)。
	wantCart := `{ "10.96.0.71", 8071, "cart-0", 3, 200, probe = "/health" },`
	wantBe := `{ "10.1.0.1", 8050, "backend-0", 2, 100 },`        // 后端 pod IP,maxConcurrency 100,不带 probe → 探 /v1/models
	wantFallback := `{ "10.96.0.50", 8050, "backend-svc-0", 1 },` // 后端 Service VIP 静态兜底,priority 1(最低),探 /v1/models
	// 有 backend-svc VIP 兜底层 → 自动注入跨层 retry(高优层 5xx 单请求即刻兜到低优 VIP)
	for _, w := range []string{wantCart, wantBe, wantFallback, "listen unix:/usr/local/openresty/nginx/sock/glm.sock", "ttft_limit_ms = 60000", "cross_tier_fallback = true", "max_more_tries = 3"} {
		if !strings.Contains(conf, w) {
			t.Errorf("openresty conf missing %q\n%s", w, conf)
		}
	}
	// 顺序:cart(prio3)→ backend(prio2)→ backend-svc(prio1)
	if !(strings.Index(conf, wantCart) < strings.Index(conf, wantBe) && strings.Index(conf, wantBe) < strings.Index(conf, wantFallback)) {
		t.Errorf("peer 顺序应为 cart → backend → backend-svc 兜底\n%s", conf)
	}

	// monitor:共享 ConfigMap 里本模型一个 key,每后端一行 service
	var monCM corev1.ConfigMap
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "monitor", Name: "monitor-conf"}, &monCM); err != nil {
		t.Fatalf("get monitor-conf: %v", err)
	}
	mon := monCM.Data["glm-5.1-fp8.monitor.conf"]
	for _, w := range []string{
		"service: glm-5.1-fp8-0 | http://10.1.0.1:8050 | glm-5.1-fp8 | H100",
		"service: glm-5.1-fp8-1 | http://10.1.0.2:8050 | glm-5.1-fp8 | H100",
	} {
		if !strings.Contains(mon, w) {
			t.Errorf("monitor conf missing %q\n%s", w, mon)
		}
	}

	// status 回写
	var got routingv1.ModelRoute
	if err := cl.Get(context.Background(), nn, &got); err != nil {
		t.Fatalf("get mr: %v", err)
	}
	if got.Status.Backends != 2 || got.Status.CartPeers != 1 || !got.Status.Ready {
		t.Errorf("status: backends=%d cartPeers=%d ready=%v (want 2/1/true)",
			got.Status.Backends, got.Status.CartPeers, got.Status.Ready)
	}
}

// referencesPodBySelector:selector 路径命中同 ns pod 才入队;service 路径 / 跨 ns / 不匹配都不入队。
func TestReferencesPodBySelector(t *testing.T) {
	mkSel := func(ns, sel string) *routingv1.ModelRoute {
		return &routingv1.ModelRoute{
			ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: ns},
			Spec:       routingv1.ModelRouteSpec{Discovery: routingv1.Discovery{Selector: sel}},
		}
	}
	lbl := map[string]string{"app": "kimi", "worker-index": "0"}
	cases := []struct {
		name   string
		mr     *routingv1.ModelRoute
		podNS  string
		labels map[string]string
		want   bool
	}{
		{"selector 命中", mkSel("kimi", "app=kimi,worker-index=0"), "kimi", lbl, true},
		{"selector 不匹配", mkSel("kimi", "app=glm"), "kimi", lbl, false},
		{"跨 ns 不入队", mkSel("kimi", "app=kimi"), "other", lbl, false},
		{"service 路径不由 pod 触发", &routingv1.ModelRoute{
			ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "kimi"},
			Spec:       routingv1.ModelRouteSpec{Discovery: routingv1.Discovery{Service: "kimi/svc"}},
		}, "kimi", lbl, false},
		{"cart selector 命中", &routingv1.ModelRoute{
			ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "kimi"},
			Spec: routingv1.ModelRouteSpec{Discovery: routingv1.Discovery{Service: "kimi/be"},
				Cart: &routingv1.CartSpec{Selector: "app=kimi"}},
		}, "kimi", lbl, true},
	}
	for _, c := range cases {
		if got := referencesPodBySelector(c.mr, c.podNS, c.labels); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// ── SLO(LLMSLORequirement → ttft_metrics / tps_metrics + SLOSynced condition)──

func sloReq(ns, name, serviceID string, ttftSec, otpsTPS float64) *slov1.LLMSLORequirement {
	return &slov1.LLMSLORequirement{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: slov1.LLMSLORequirementSpec{
			ServiceID: serviceID,
			TTFT:      &slov1.SLOTarget{Default: &slov1.SLODefault{Metrics: []slov1.SLOMetric{{Type: "p80", Threshold: ttftSec}}}},
			OTPS:      &slov1.SLOTarget{Default: &slov1.SLODefault{Metrics: []slov1.SLOMetric{{Type: "p80", Threshold: otpsTPS}}}},
		},
	}
}

// sloFixture 搭一条最小可 reconcile 的 ModelRoute(service 发现 + 直连后端,无 cart/monitor)。
func sloFixture(t *testing.T, slo *routingv1.SLOSpec, discovery routingv1.Discovery,
	objs ...client.Object) (*ModelRouteReconciler, types.NamespacedName, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = routingv1.AddToScheme(scheme)
	_ = slov1.AddToScheme(scheme)

	rb := &routingv1.ModelRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "kimi-k2.5", Namespace: "kimi"},
		Spec: routingv1.ModelRouteSpec{
			Discovery: discovery,
			Nginx: routingv1.NginxSpec{
				Route: "kimi-k2.5", OutputConfigMap: "llm-route/openresty-conf",
				Values: map[string]string{"ttft_limit_ms": "30000"},
				Peers:  []routingv1.RoutePeer{{Use: "backend", MaxConcurrency: 50}},
			},
			SLO: slo,
		},
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "openresty-conf", Namespace: "llm-route"}}
	cl := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithObjects(append([]client.Object{rb, cm}, objs...)...).
		WithStatusSubresource(&routingv1.ModelRoute{}).Build()
	// selector 路径要有 pod 才发现得到后端 —— 否则 reconcile 在 len(backends)==0 就 return,
	// 根本走不到 SLO 那一步(第一版测试就是这么假绿的)。
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "kimi-0", Namespace: "kimi", Labels: map[string]string{"app": "kimi"}},
		Status: corev1.PodStatus{PodIP: "10.1.0.1", Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	cs := k8sfake.NewSimpleClientset(
		epslice("kimi-k25-leader-1", "kimi-k25-leader", "kimi", "10.1.0.1"), pod)
	r := &ModelRouteReconciler{Client: cl, Clientset: cs, Scheme: scheme}
	nn := types.NamespacedName{Namespace: "kimi", Name: "kimi-k2.5"}
	for i := 0; i < 2; i++ { // 第一次加 finalizer 并 requeue,第二次真正干活
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
			t.Fatalf("reconcile %d: %v", i+1, err)
		}
	}
	return r, nn, cl
}

func routeConf(t *testing.T, cl client.Client) string {
	t.Helper()
	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(),
		types.NamespacedName{Namespace: "llm-route", Name: "openresty-conf"}, &cm); err != nil {
		t.Fatalf("get openresty-conf: %v", err)
	}
	return cm.Data["session_route_kimi-k2.5.conf"]
}

func condOf(t *testing.T, cl client.Client, nn types.NamespacedName, typ string) *metav1.Condition {
	t.Helper()
	var rb routingv1.ModelRoute
	if err := cl.Get(context.Background(), nn, &rb); err != nil {
		t.Fatalf("get mr: %v", err)
	}
	for i := range rb.Status.Conditions {
		if rb.Status.Conditions[i].Type == typ {
			return &rb.Status.Conditions[i]
		}
	}
	return nil
}

// 线上 kimi 的形状:ModelRoute 引用同 ns 的 LLMSLORequirement "kimi-k25"。
//
// CRD 的 spec.serviceId 这里**故意写成别的值**:匹配只看 metadata.name,
// serviceId 是给 autoscaler 用的、与路由层无关。写成同值的话,这个用例在
// 「改回按 serviceId 匹配」时照样会绿,就测不住这件事了。
func TestReconcileSLO_Rendered(t *testing.T) {
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{Name: "kimi-k25"},
		routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050},
		sloReq("kimi", "kimi-k25", "some-other-service-id", 20, 15))

	conf := routeConf(t, cl)
	for _, want := range []string{
		`ttft_metrics = { { metric = "p80", q = 0.8, threshold = 20000 }, },`, // 秒 → 毫秒
		`tps_metrics = { { metric = "p80", q = 0.2, threshold = 15 }, },`,     // 覆盖率 0.8 → 低尾 q=0.2
		"ttft_limit_ms = 30000,", // 静态默认不被挤掉
	} {
		if !strings.Contains(conf, want) {
			t.Errorf("conf 缺少 %s\n%s", want, conf)
		}
	}
	c := condOf(t, cl, nn, "SLOSynced")
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != "Synced" {
		t.Errorf("SLOSynced 应为 True/Synced,得到 %+v", c)
	}
}

// 没有匹配的 CRD:不是错误(引擎回落静态),但要能从 status 看出来 ——
// 否则「配了 slo 却没生效」和「CRD 本来就没写」分不开。
func TestReconcileSLO_NoRequirement(t *testing.T) {
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{Name: "kimi-k25"},
		routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050})

	if strings.Contains(routeConf(t, cl), "ttft_metrics") {
		t.Errorf("没有 CRD 时不该渲染 ttft_metrics\n%s", routeConf(t, cl))
	}
	c := condOf(t, cl, nn, "SLOSynced")
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != "NoRequirement" {
		t.Errorf("应为 True/NoRequirement,得到 %+v", c)
	}
	if !strings.Contains(c.Message, "kimi-k25") {
		t.Errorf("message 应带上推导出的 serviceId 便于排查,得到 %q", c.Message)
	}
}

// CRD **存在**但只写了 ranges(本期不支持)→ 必须与「CRD 不存在」区分开。
// 共用 NoRequirement + 「没有匹配的 LLMSLORequirement」会把人指去查 serviceId 对不对,
// 而真正的原因(ranges 不支持)只在 warns 里、以前被整个丢掉了。
func TestReconcileSLO_OnlyRanges(t *testing.T) {
	onlyRanges := &slov1.LLMSLORequirement{
		ObjectMeta: metav1.ObjectMeta{Name: "kimi-k25", Namespace: "kimi"},
		Spec: slov1.LLMSLORequirementSpec{
			ServiceID: "kimi-k25",
			TTFT: &slov1.SLOTarget{Ranges: []slov1.SLORange{
				{ContextLengthRangeLow: 0, Metrics: []slov1.SLOMetric{{Type: "p80", Threshold: 20}}}}},
		},
	}
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{Name: "kimi-k25"},
		routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050}, onlyRanges)

	if strings.Contains(routeConf(t, cl), "ttft_metrics") {
		t.Errorf("ranges 不支持,不该渲染 ttft_metrics\n%s", routeConf(t, cl))
	}
	c := condOf(t, cl, nn, "SLOSynced")
	if c == nil || c.Reason != "NothingApplicable" {
		t.Fatalf("应为 NothingApplicable(不是 NoRequirement),得到 %+v", c)
	}
	if !strings.Contains(c.Message, "ranges") {
		t.Errorf("message 必须带上 ranges 被忽略这条 warn —— 那就是「为什么没生效」的答案,得到 %q", c.Message)
	}
}

// spec.slo 从"有"变成"没有":SLOSynced 必须被摘掉,不能永久残留一个失败态。
func TestReconcileSLO_RemovedClearsCondition(t *testing.T) {
	r, nn, cl := sloFixture(t, &routingv1.SLOSpec{}, // serviceId 留空 → False
		routingv1.Discovery{Selector: "app=kimi", Port: 8050})
	if c := condOf(t, cl, nn, "SLOSynced"); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatalf("前置条件:应先有一个 False 的 SLOSynced,得到 %+v", c)
	}

	var rb routingv1.ModelRoute
	if err := cl.Get(context.Background(), nn, &rb); err != nil {
		t.Fatalf("%v", err)
	}
	rb.Spec.SLO = nil
	if err := cl.Update(context.Background(), &rb); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if c := condOf(t, cl, nn, "SLOSynced"); c != nil {
		t.Errorf("移除 spec.slo 后 SLOSynced 应被摘掉,得到 %+v", c)
	}
}

// 配了 spec.slo 却没写 name —— **永久性**失败(不会自愈、也不会随重试变好)。
// 以前只进 operator 日志,用户会看到一个 Ready=true 却永远没有 SLO 的路由,毫无线索。
// 用 selector 发现构造,因为这条路由连 Service 名都没有,是最没法"猜"的形状。
func TestReconcileSLO_MissingName(t *testing.T) {
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{},
		routingv1.Discovery{Selector: "app=kimi", Port: 8050})

	c := condOf(t, cl, nn, "SLOSynced")
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "TranslateError" {
		t.Fatalf("应为 False/TranslateError,得到 %+v", c)
	}
	if !strings.Contains(c.Message, "slo.name") {
		t.Errorf("message 应指出补救办法,得到 %q", c.Message)
	}
	// SLO 失败**不该**把 Ready 打成 false —— peers 照常下发,转发是好的
	if ready := condOf(t, cl, nn, "Ready"); ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready 不该被 SLO 失败带下去,得到 %+v", ready)
	}
}

// spec.slo == nil:完全不碰 SLO,连 condition 都不该出现。
func TestReconcileSLO_Disabled(t *testing.T) {
	_, nn, cl := sloFixture(t, nil, routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050},
		sloReq("kimi", "kimi-k25", "kimi-k25", 20, 15))

	if strings.Contains(routeConf(t, cl), "metrics") {
		t.Errorf("没配 spec.slo 时不该渲染任何 metrics\n%s", routeConf(t, cl))
	}
	if c := condOf(t, cl, nn, "SLOSynced"); c != nil {
		t.Errorf("没配 spec.slo 不该产生 SLOSynced condition,得到 %+v", c)
	}
}

// 跨 ns 引用:slo.name 写 "ns/name" 时应到那个 ns 去取。
// 与 discovery.service / nginx.outputConfigMap 同一套写法。
func TestReconcileSLO_CrossNamespace(t *testing.T) {
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{Name: "slo-shared/kimi-k25"},
		routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050},
		sloReq("slo-shared", "kimi-k25", "whatever", 20, 15))

	if c := condOf(t, cl, nn, "SLOSynced"); c == nil || c.Status != metav1.ConditionTrue || c.Reason != "Synced" {
		t.Fatalf("跨 ns 引用应正常下发,得到 %+v", c)
	}
	if conf := routeConf(t, cl); !strings.Contains(conf, `ttft_metrics = { { metric = "p80", q = 0.8, threshold = 20000 }, },`) {
		t.Errorf("跨 ns 的 SLO 没渲染进 conf\n%s", conf)
	}
}

// 裸名不该跑到别的 ns 去找:CRD 在 slo-shared,ModelRoute 在 kimi,写裸名就应该找不到。
// 这条守的是「裸名 = ModelRoute 自己的 ns」这个默认,别哪天被改成全 ns 搜。
func TestReconcileSLO_BareNameDoesNotCrossNamespace(t *testing.T) {
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{Name: "kimi-k25"},
		routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050},
		sloReq("slo-shared", "kimi-k25", "whatever", 20, 15))

	c := condOf(t, cl, nn, "SLOSynced")
	if c == nil || c.Reason != "NoRequirement" {
		t.Fatalf("裸名应只在本 ns 找 → NoRequirement,得到 %+v", c)
	}
	if !strings.Contains(c.Message, "kimi/kimi-k25") {
		t.Errorf("message 应显示解析后的 ns/name 便于排查,得到 %q", c.Message)
	}
}

func TestSLOName(t *testing.T) {
	mk := func(slo *routingv1.SLOSpec, svc string) *routingv1.ModelRoute {
		return &routingv1.ModelRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: "kimi"},
			Spec:       routingv1.ModelRouteSpec{Discovery: routingv1.Discovery{Service: svc}, SLO: slo},
		}
	}
	// 只读显式字段。**这个用例的重点是"不推导"**:早先这里会把 kimi/kimi-k25-leader
	// 截成 kimi-k25,现在给了 Service 名也必须原样为空 —— 否则就是推导又被加回来了。
	if got := sloName(mk(&routingv1.SLOSpec{}, "kimi/kimi-k25-leader")); got != "" {
		t.Errorf("name 空时不该从 Service 名推导,得到 %q", got)
	}
	if got := sloName(mk(&routingv1.SLOSpec{Name: "x"}, "kimi/kimi-k25-leader")); got != "x" {
		t.Errorf("应原样返回显式 name,得到 %q", got)
	}
	// ns/name 原样返回,拆分由调用点的 splitNSName 做
	if got := sloName(mk(&routingv1.SLOSpec{Name: "other/x"}, "")); got != "other/x" {
		t.Errorf("ns/name 应原样返回,得到 %q", got)
	}
	// spec.slo == nil 不该 panic
	if got := sloName(mk(nil, "kimi/kimi-k25-leader")); got != "" {
		t.Errorf("slo==nil 应返回空,得到 %q", got)
	}
}

// ── openresty key 冲突 ──────────────────────────────────────────────────────
//
// 两条 ModelRoute 用同一个 nginx.route,就会渲染同一个 ConfigMap key,互相覆盖。
// 这不是假想:2026-09-01 线上 modelforge-02-kimi 与 modelforge/fallback-modelforge-01
// 都用 route=fallback-modelforge-0.1,该路由在 8 后端与 1 后端之间来回翻(每翻一次带一次 reload),
// 而两条的 status 都是 Ready/Synced —— 从任何一条上都看不出异常。

// conflictFixture 搭两条抢同一个 key 的 ModelRoute;old 创建更早。
func conflictFixture(t *testing.T) (*ModelRouteReconciler, client.Client, types.NamespacedName, types.NamespacedName) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = routingv1.AddToScheme(scheme)
	_ = slov1.AddToScheme(scheme)

	mk := func(ns, name string, created time.Time, podIP string) *routingv1.ModelRoute {
		return &routingv1.ModelRoute{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns,
				CreationTimestamp: metav1.NewTime(created)},
			Spec: routingv1.ModelRouteSpec{
				Discovery: routingv1.Discovery{Selector: "app=" + name, Port: 8050},
				Nginx: routingv1.NginxSpec{
					Route: "shared-route", OutputConfigMap: "llm-route/openresty-conf", // ← 同一个 key
					Peers: []routingv1.RoutePeer{{Use: "backend", MaxConcurrency: 50}},
				},
				// monitor 与 SLO 都配上:冲突只该拦住 openresty 那一次写,不该殃及这两项
				Monitor: &routingv1.MonitorSpec{OutputConfigMap: "monitoring/monitor-conf", Model: name, GPUType: "H100"},
				SLO:     &routingv1.SLOSpec{Name: "slo-" + name},
			},
		}
	}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldMR := mk("ns-old", "old", t0, "10.1.0.1")
	newMR := mk("ns-new", "new", t0.Add(24*time.Hour), "10.2.0.1")
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "openresty-conf", Namespace: "llm-route"}}
	moncm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "monitor-conf", Namespace: "monitoring"}}

	cl := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithObjects(oldMR, newMR, cm, moncm,
			sloReq("ns-new", "slo-new", "x", 20, 15), sloReq("ns-old", "slo-old", "x", 20, 15)).
		WithStatusSubresource(&routingv1.ModelRoute{}).Build()
	pod := func(ns, name, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: ns, Labels: map[string]string{"app": name}},
			Status: corev1.PodStatus{PodIP: ip, Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		}
	}
	cs := k8sfake.NewSimpleClientset(pod("ns-old", "old", "10.1.0.1"), pod("ns-new", "new", "10.2.0.1"))
	r := &ModelRouteReconciler{Client: cl, Clientset: cs, Scheme: scheme}
	nnOld := types.NamespacedName{Namespace: "ns-old", Name: "old"}
	nnNew := types.NamespacedName{Namespace: "ns-new", Name: "new"}
	for i := 0; i < 2; i++ {
		for _, nn := range []types.NamespacedName{nnOld, nnNew} {
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
				t.Fatalf("reconcile %s: %v", nn, err)
			}
		}
	}
	return r, cl, nnOld, nnNew
}

func sharedKey(t *testing.T, cl client.Client) (string, bool) {
	t.Helper()
	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(),
		types.NamespacedName{Namespace: "llm-route", Name: "openresty-conf"}, &cm); err != nil {
		t.Fatalf("get cm: %v", err)
	}
	v, ok := cm.Data["session_route_shared-route.conf"]
	return v, ok
}

// 在位者(创建更早)赢;后来者拒绝写入并把冲突写进 status。
func TestRouteKeyConflict_IncumbentWins(t *testing.T) {
	_, cl, nnOld, nnNew := conflictFixture(t)

	conf, ok := sharedKey(t, cl)
	if !ok {
		t.Fatal("共享 key 应由在位者写入")
	}
	// 内容必须是 old 的后端(10.1.0.1),不能被 new 覆盖成 10.2.0.1
	if !strings.Contains(conf, "10.1.0.1") || strings.Contains(conf, "10.2.0.1") {
		t.Errorf("key 被后来者覆盖了\n%s", conf)
	}
	if c := condOf(t, cl, nnOld, "Ready"); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("在位者应正常,得到 %+v", c)
	}
	c := condOf(t, cl, nnNew, "Ready")
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "RouteKeyConflict" {
		t.Fatalf("后来者应为 False/RouteKeyConflict,得到 %+v", c)
	}
	if !strings.Contains(c.Message, "ns-old/old") || !strings.Contains(c.Message, "spec.nginx.route") {
		t.Errorf("message 应指出归属者和补救办法,得到 %q", c.Message)
	}
}

// 删掉冲突的输家时,**不能**顺手删掉赢家的 key。
// 输家从没写过这个 key;它一删就是把一条正在服务的路由的配置删掉,
// 而现场只剩"key 凭空消失",几乎无法归因。
func TestRouteKeyConflict_LoserDeletionKeepsWinnerKey(t *testing.T) {
	r, cl, _, nnNew := conflictFixture(t)
	if _, ok := sharedKey(t, cl); !ok {
		t.Fatal("前置:赢家的 key 应存在")
	}

	var loser routingv1.ModelRoute
	if err := cl.Get(context.Background(), nnNew, &loser); err != nil {
		t.Fatalf("%v", err)
	}
	if err := cl.Delete(context.Background(), &loser); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nnNew}); err != nil {
		t.Fatalf("reconcile(删除): %v", err)
	}

	conf, ok := sharedKey(t, cl)
	if !ok {
		t.Fatal("赢家的 key 被输家的 finalizer 删掉了")
	}
	if !strings.Contains(conf, "10.1.0.1") {
		t.Errorf("赢家的内容被动过\n%s", conf)
	}
}

// 归属规则必须与 reconcile 顺序无关:先 reconcile 后来者,结果不变。
// 否则"谁赢"随时间漂移,和不判几乎一样糟。
func TestRouteKeyConflict_OrderIndependent(t *testing.T) {
	r, cl, nnOld, nnNew := conflictFixture(t)
	for i := 0; i < 3; i++ {
		for _, nn := range []types.NamespacedName{nnNew, nnOld, nnNew} { // 故意让后来者多跑
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
				t.Fatalf("%v", err)
			}
		}
	}
	conf, _ := sharedKey(t, cl)
	if !strings.Contains(conf, "10.1.0.1") || strings.Contains(conf, "10.2.0.1") {
		t.Errorf("归属随 reconcile 顺序漂移了\n%s", conf)
	}
}

// ── route 改名 ────────────────────────────────────────────────────────────
//
// finalizer 和清理逻辑只看得到**当前** spec,推不出改名前叫什么,所以改一次名就会
// 泄漏一个 key,而 openresty 会继续加载那条陈旧路由(指向改名前的 peers,且再没人更新它)。
// 2026-09-02 线上 modelforge-0.2 → modelforge-0.2-kimi 改名后就实际留下了一个。

func cmKeys(t *testing.T, cl client.Client) []string {
	t.Helper()
	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(),
		types.NamespacedName{Namespace: "llm-route", Name: "openresty-conf"}, &cm); err != nil {
		t.Fatalf("get cm: %v", err)
	}
	var ks []string
	for k := range cm.Data {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func renameRoute(t *testing.T, r *ModelRouteReconciler, cl client.Client, nn types.NamespacedName, to string) {
	t.Helper()
	var rb routingv1.ModelRoute
	if err := cl.Get(context.Background(), nn, &rb); err != nil {
		t.Fatalf("%v", err)
	}
	rb.Spec.Nginx.Route = to
	if err := cl.Update(context.Background(), &rb); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile(改名后): %v", err)
	}
}

// 改名后旧 key **保留**(operator 不删:它无法知道还有没有调用方在打旧路径),
// 但必须被报出来 —— 孤儿不可见正是线上那次问题的本质。
func TestRouteRename_ReportsOrphanButKeepsIt(t *testing.T) {
	r, nn, cl := sloFixture(t, nil, routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050})
	if got := cmKeys(t, cl); len(got) != 1 || got[0] != "session_route_kimi-k2.5.conf" {
		t.Fatalf("前置:应只有一个 key,得到 %v", got)
	}

	renameRoute(t, r, cl, nn, "kimi-k2.5-v2")

	got := cmKeys(t, cl)
	if len(got) != 2 {
		t.Fatalf("旧 key 不该被 operator 删掉(可能仍在承接流量),期望两个 key,得到 %v", got)
	}
	var rb routingv1.ModelRoute
	if err := cl.Get(context.Background(), nn, &rb); err != nil {
		t.Fatalf("%v", err)
	}
	if rb.Status.AppliedRouteKey != "session_route_kimi-k2.5-v2.conf" {
		t.Errorf("status.appliedRouteKey = %q", rb.Status.AppliedRouteKey)
	}
	want := "llm-route/openresty-conf:session_route_kimi-k2.5.conf"
	if len(rb.Status.OrphanRouteKeys) != 1 || rb.Status.OrphanRouteKeys[0] != want {
		t.Errorf("orphanRouteKeys = %v; want [%s]", rb.Status.OrphanRouteKeys, want)
	}
	c := condOf(t, cl, nn, "OrphanRouteKey")
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("应挂 OrphanRouteKey condition,得到 %+v", c)
	}
	if !strings.Contains(c.Message, "session_route_kimi-k2.5.conf") || !strings.Contains(c.Message, "手工") {
		t.Errorf("message 应指明是哪个 key、且要人工处理,得到 %q", c.Message)
	}
}

// 人手工删掉孤儿 key 之后,condition 必须自动摘掉 —— 不然 status 上永久挂着一条已解决的告警,
// 久了就没人看了。
func TestRouteRename_OrphanConditionClearsAfterCleanup(t *testing.T) {
	r, nn, cl := sloFixture(t, nil, routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050})
	renameRoute(t, r, cl, nn, "kimi-k2.5-v2")
	if c := condOf(t, cl, nn, "OrphanRouteKey"); c == nil {
		t.Fatal("前置:应先有 OrphanRouteKey")
	}

	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(),
		types.NamespacedName{Namespace: "llm-route", Name: "openresty-conf"}, &cm); err != nil {
		t.Fatalf("%v", err)
	}
	delete(cm.Data, "session_route_kimi-k2.5.conf") // 模拟人工清理
	if err := cl.Update(context.Background(), &cm); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("%v", err)
	}

	if c := condOf(t, cl, nn, "OrphanRouteKey"); c != nil {
		t.Errorf("孤儿已清理,condition 应被摘掉,得到 %+v", c)
	}
	var rb routingv1.ModelRoute
	_ = cl.Get(context.Background(), nn, &rb)
	if len(rb.Status.OrphanRouteKeys) != 0 {
		t.Errorf("orphanRouteKeys 应清空,得到 %v", rb.Status.OrphanRouteKeys)
	}
}

// 改名腾位给别人接手:不是孤儿,不该报。
func TestRouteRename_NoOrphanWhenTakenOver(t *testing.T) {
	r, nn, cl := sloFixture(t, nil, routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050})

	// 另一条路由接手 kimi-k2.5 这个名字(它自己的后端)
	taker := &routingv1.ModelRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "taker", Namespace: "kimi"},
		Spec: routingv1.ModelRouteSpec{
			Discovery: routingv1.Discovery{Selector: "app=kimi", Port: 8050},
			Nginx: routingv1.NginxSpec{
				Route: "kimi-k2.5", OutputConfigMap: "llm-route/openresty-conf",
				Peers: []routingv1.RoutePeer{{Use: "backend", MaxConcurrency: 50}},
			},
		},
	}
	if err := cl.Create(context.Background(), taker); err != nil {
		t.Fatalf("%v", err)
	}
	renameRoute(t, r, cl, nn, "kimi-k2.5-v2")

	if c := condOf(t, cl, nn, "OrphanRouteKey"); c != nil {
		t.Errorf("旧 key 已被别的 ModelRoute 接手,不是孤儿,不该报,得到 %+v", c)
	}
	var rb routingv1.ModelRoute
	_ = cl.Get(context.Background(), nn, &rb)
	if len(rb.Status.OrphanRouteKeys) != 0 {
		t.Errorf("orphanRouteKeys 应为空,得到 %v", rb.Status.OrphanRouteKeys)
	}
}

// 冲突输家:openresty 写入被拦,但 **monitor 照常同步**。
// monitor 探的是后端 IP,与 route key 撞名毫无关系;后端还在跑却因为改名撞车丢掉监控是本末倒置,
// 而且冲突可能挂很久(等人来改名),那段时间恰恰最需要监控。
func TestRouteKeyConflict_LoserStillSyncsMonitor(t *testing.T) {
	_, cl, _, _ := conflictFixture(t)

	var cm corev1.ConfigMap
	if err := cl.Get(context.Background(),
		types.NamespacedName{Namespace: "monitoring", Name: "monitor-conf"}, &cm); err != nil {
		t.Fatalf("get monitor cm: %v", err)
	}
	v, ok := cm.Data["new.monitor.conf"]
	if !ok {
		t.Fatalf("输家的 monitor key 缺失(被冲突早退跳过了),现有 keys=%v", keysOf(cm.Data))
	}
	if !strings.Contains(v, "10.2.0.1") {
		t.Errorf("输家的 monitor 行应含自己的后端\n%s", v)
	}
}

// 冲突输家不该报 SLOSynced=Synced —— 这一轮根本没写 conf,
// 说"已下发"会和 Ready=RouteKeyConflict 直接打架,排查时不知道该信哪条。
func TestRouteKeyConflict_LoserDoesNotClaimSLOSynced(t *testing.T) {
	_, cl, _, nnNew := conflictFixture(t)
	if c := condOf(t, cl, nnNew, "SLOSynced"); c != nil && c.Reason == "Synced" {
		t.Errorf("冲突输家不该声称 SLO 已下发(conf 根本没写),得到 %+v", c)
	}
}

func keysOf(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// 归属必须看**谁实际持有这个 key**,不能只看创建时间。
//
// 反例(修复前会踩):MR-A 较新但一直在写 route "x";MR-B 较老,把自己的 route 改成 "x"。
// 只按 creationTimestamp 判的话 MR-B 赢 —— **一条更老的 ModelRoute 改个名就抢走了一条
// 正在服务的路由**,连人带流量。这正是这套机制要防的事故,只是入口从"新建"变成"改名"。
func TestRouteKeyOwner_IncumbentBeatsOlderRenamer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = routingv1.AddToScheme(scheme)
	_ = slov1.AddToScheme(scheme)

	mk := func(ns, name, route string, created time.Time) *routingv1.ModelRoute {
		return &routingv1.ModelRoute{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, CreationTimestamp: metav1.NewTime(created)},
			Spec: routingv1.ModelRouteSpec{
				Discovery: routingv1.Discovery{Selector: "app=" + name, Port: 8050},
				Nginx: routingv1.NginxSpec{
					Route: route, OutputConfigMap: "llm-route/openresty-conf",
					Peers: []routingv1.RoutePeer{{Use: "backend", MaxConcurrency: 50}},
				},
			},
		}
	}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	incumbent := mk("ns-a", "a", "x", t0.Add(24*time.Hour)) // 较新,但在位
	older := mk("ns-b", "b", "y", t0)                       // 较老,稍后改名撞过来
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "openresty-conf", Namespace: "llm-route"}}
	pod := func(ns, name, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: ns, Labels: map[string]string{"app": name}},
			Status: corev1.PodStatus{PodIP: ip, Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		}
	}
	cl := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(incumbent, older, cm).
		WithStatusSubresource(&routingv1.ModelRoute{}).Build()
	cs := k8sfake.NewSimpleClientset(pod("ns-a", "a", "10.1.0.1"), pod("ns-b", "b", "10.2.0.1"))
	r := &ModelRouteReconciler{Client: cl, Clientset: cs, Scheme: scheme}
	nnA := types.NamespacedName{Namespace: "ns-a", Name: "a"}
	nnB := types.NamespacedName{Namespace: "ns-b", Name: "b"}
	for i := 0; i < 2; i++ {
		for _, nn := range []types.NamespacedName{nnA, nnB} {
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
				t.Fatalf("%v", err)
			}
		}
	}
	// 前置:A 在位(status 记着 x),conf 是 A 的后端
	var a routingv1.ModelRoute
	_ = cl.Get(context.Background(), nnA, &a)
	if a.Status.AppliedRouteKey != "session_route_x.conf" {
		t.Fatalf("前置:A 应持有 x,得到 %q", a.Status.AppliedRouteKey)
	}

	renameRoute(t, r, cl, nnB, "x") // 更老的 B 改名撞过来
	for i := 0; i < 2; i++ {
		for _, nn := range []types.NamespacedName{nnA, nnB} {
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
				t.Fatalf("%v", err)
			}
		}
	}

	var cm2 corev1.ConfigMap
	if err := cl.Get(context.Background(),
		types.NamespacedName{Namespace: "llm-route", Name: "openresty-conf"}, &cm2); err != nil {
		t.Fatalf("%v", err)
	}
	conf := cm2.Data["session_route_x.conf"]
	if !strings.Contains(conf, "10.1.0.1") || strings.Contains(conf, "10.2.0.1") {
		t.Errorf("在位者 A 的路由被更老的 B 改名抢走了\n%s", conf)
	}
	if c := condOf(t, cl, nnA, "Ready"); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("在位者应不受影响,得到 %+v", c)
	}
	c := condOf(t, cl, nnB, "Ready")
	if c == nil || c.Reason != "RouteKeyConflict" {
		t.Errorf("改名撞过来的 B 应判冲突,得到 %+v", c)
	}
}

// 同一个模型的多个 ModelRoute(canary baseline/experiment)必须生成**互不相同**的 nginx 名。
//
// 2026-09-08 事故:nginx 名按 m.Model 派生,mf-dummpy / -canary-baseline / -canary-experiment
// 三个 ModelRoute 共用 model=mf-dummpy-test,生成三条同名 `nginx: mf-dummpy-test-nginx-0`,
// monitor 首载解析失败、拒绝启动,整套 k8s 监控挂掉。三个入口 VIP 各不相同,三条行都该保留,
// 只是名字必须唯一 —— 用 ModelRoute 名(k8s 对象名天然唯一)。
func TestMonitorNginxNameUniquePerRoute(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = routingv1.AddToScheme(scheme)

	const model = "mf-dummpy-test"
	names := []string{"mf-dummpy", "mf-dummpy-canary-baseline", "mf-dummpy-canary-experiment"}
	seen := map[string]string{} // nginx 名 → 来自哪个 route

	for i, name := range names {
		rb := &routingv1.ModelRoute{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "llm-route"},
			Spec: routingv1.ModelRouteSpec{
				Discovery: routingv1.Discovery{Service: name + "-leader", Port: 8050},
				Nginx: routingv1.NginxSpec{
					Route: "mf-dummpy", OutputConfigMap: "openresty/openresty-conf",
					Service: "openresty/" + name + "-or", // 每个 route 自己的入口 Service → 不同 VIP
				},
				Monitor: &routingv1.MonitorSpec{
					OutputConfigMap: "monitor/monitor-conf", Model: model, GPUType: "A100",
				},
			},
		}
		cmOR := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "openresty-conf", Namespace: "openresty"}}
		cmMon := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "monitor-conf", Namespace: "monitor"}}
		cl := ctrlfake.NewClientBuilder().WithScheme(scheme).
			WithObjects(rb, cmOR, cmMon).WithStatusSubresource(&routingv1.ModelRoute{}).Build()
		cs := k8sfake.NewSimpleClientset(
			epslice(name+"-leader-1", name+"-leader", "llm-route", "192.168.28.10"),
			// 入口 Service 的端口必须**带名字** dispatch —— controller 按名取(端口号在 chart 里)
			&corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: name + "-or", Namespace: "openresty"},
				Spec: corev1.ServiceSpec{
					ClusterIP: "10.96.88.1" + string(rune('0'+i)),
					Ports:     []corev1.ServicePort{{Name: "dispatch", Port: 8080}},
				},
			},
		)
		r := &ModelRouteReconciler{Client: cl, Clientset: cs, Scheme: scheme}
		nn := types.NamespacedName{Namespace: "llm-route", Name: name}
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
			t.Fatalf("%s reconcile 1: %v", name, err)
		}
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
			t.Fatalf("%s reconcile 2: %v", name, err)
		}

		var monCM corev1.ConfigMap
		if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "monitor", Name: "monitor-conf"}, &monCM); err != nil {
			t.Fatalf("get monitor-conf: %v", err)
		}
		var got string
		for _, v := range monCM.Data {
			for _, line := range strings.Split(v, "\n") {
				if strings.HasPrefix(line, "nginx:") {
					got = strings.TrimSpace(strings.Split(strings.TrimPrefix(line, "nginx:"), "|")[0])
				}
			}
		}
		if got == "" {
			t.Fatalf("%s: 没有生成 nginx 行\n%v", name, monCM.Data)
		}
		if strings.Contains(got, model) {
			t.Errorf("%s: nginx 名 %q 仍按模型名派生 —— 多 route 共模型时必然撞名", name, got)
		}
		if prev, dup := seen[got]; dup {
			t.Fatalf("nginx 名撞车:%q 同时来自 %s 和 %s(正是 2026-09-08 弄挂 monitor 的那个 bug)", got, prev, name)
		}
		seen[got] = name
	}
	if len(seen) != len(names) {
		t.Fatalf("期望 %d 个不同的 nginx 名,实得 %d:%v", len(names), len(seen), seen)
	}
}
