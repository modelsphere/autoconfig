package controller

import (
	"context"
	"strings"
	"testing"

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

// 线上 kimi 的形状:discovery.service = kimi/kimi-k25-leader → serviceId kimi-k25。
func TestReconcileSLO_Rendered(t *testing.T) {
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{ServiceID: "kimi-k25"},
		routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050},
		sloReq("kimi", "kimi-k25", "kimi-k25", 20, 15))

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
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{ServiceID: "kimi-k25"},
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
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{ServiceID: "kimi-k25"},
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

// 配了 spec.slo 却没写 serviceId —— **永久性**失败(不会自愈、也不会随重试变好)。
// 以前只进 operator 日志,用户会看到一个 Ready=true 却永远没有 SLO 的路由,毫无线索。
// 用 selector 发现构造,因为这条路由连 Service 名都没有,是最没法"猜"的形状。
func TestReconcileSLO_MissingServiceID(t *testing.T) {
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{},
		routingv1.Discovery{Selector: "app=kimi", Port: 8050})

	c := condOf(t, cl, nn, "SLOSynced")
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "TranslateError" {
		t.Fatalf("应为 False/TranslateError,得到 %+v", c)
	}
	if !strings.Contains(c.Message, "slo.serviceId") {
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

// 同 ns 内两个 LLMSLORequirement 用了同一个 serviceId:必须报错。
// 静默取 List 的第一个会让"生效的是哪份 SLO"随 informer 缓存顺序漂移 ——
// 阈值时而 A 时而 B,而两份 CRD 看上去都好好的,几乎无法排查。
func TestReconcileSLO_DuplicateServiceID(t *testing.T) {
	_, nn, cl := sloFixture(t, &routingv1.SLOSpec{ServiceID: "kimi-k25"},
		routingv1.Discovery{Service: "kimi-k25-leader", Port: 8050},
		sloReq("kimi", "slo-a", "kimi-k25", 20, 15),
		sloReq("kimi", "slo-b", "kimi-k25", 99, 99))

	c := condOf(t, cl, nn, "SLOSynced")
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "TranslateError" {
		t.Fatalf("应为 False/TranslateError,得到 %+v", c)
	}
	if !strings.Contains(c.Message, "多个") {
		t.Errorf("message 应说明是重复,得到 %q", c.Message)
	}
	// 拿不准用哪份就**一份都不下发**,回落静态 —— 不能赌一个
	if conf := routeConf(t, cl); strings.Contains(conf, "ttft_metrics") {
		t.Errorf("重复时不该下发任何 metrics\n%s", conf)
	}
}

func TestSLOServiceID(t *testing.T) {
	mk := func(slo *routingv1.SLOSpec, svc string) *routingv1.ModelRoute {
		return &routingv1.ModelRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: "kimi"},
			Spec:       routingv1.ModelRouteSpec{Discovery: routingv1.Discovery{Service: svc}, SLO: slo},
		}
	}
	// 只读显式字段。**这个用例的重点是"不推导"**:曾经这里会把 kimi/kimi-k25-leader
	// 截成 kimi-k25,现在给了 Service 名也必须原样为空 —— 否则就是推导又被加回来了。
	if got := sloServiceID(mk(&routingv1.SLOSpec{}, "kimi/kimi-k25-leader")); got != "" {
		t.Errorf("serviceId 空时不该从 Service 名推导,得到 %q", got)
	}
	if got := sloServiceID(mk(&routingv1.SLOSpec{ServiceID: "x"}, "kimi/kimi-k25-leader")); got != "x" {
		t.Errorf("应原样返回显式 serviceId,得到 %q", got)
	}
	// spec.slo == nil 不该 panic
	if got := sloServiceID(mk(nil, "kimi/kimi-k25-leader")); got != "" {
		t.Errorf("slo==nil 应返回空,得到 %q", got)
	}
}
