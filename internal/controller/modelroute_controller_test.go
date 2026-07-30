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

	routingv1 "autoconfig/api/v1alpha1"
)

func boolp(b bool) *bool { return &b }

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
			Openresty: routingv1.OpenrestySpec{
				Route: "glm", Listen: 18083, OutputConfigMap: "openresty/openresty-conf",
				Values: map[string]string{"ttft_limit_ms": "60000"},
				Sources: []routingv1.RouteSource{
					{Use: "cart", Priority: 1, MaxConcurrency: 180},
					{Use: "backend", Priority: 0},
				},
			},
			Monitor: &routingv1.MonitorSpec{
				OutputConfigMap: "monitor/monitor-conf", Model: "glm-5.1-fp8", GPUType: "H100",
			},
		},
	}
	// chart 预建的 cart-config:底稿(server port 6700)+ 占位 workers;autoconfig 剥掉 workers 段重填
	base := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cart-config", Namespace: "glm"},
		Data:       map[string]string{"config.yaml": "server: { host: \"0.0.0.0\", port: 6700 }\nworkers: []"},
	}
	cl := ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rb, base).WithStatusSubresource(&routingv1.ModelRoute{}).Build()
	cs := k8sfake.NewSimpleClientset(
		epslice("glm-leader-1", "glm-leader", "glm", "10.1.0.1", "10.1.0.2"),
		epslice("cart-glm-1", "cart-glm", "glm", "10.9.0.1"),
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
	wantCart := `{ "10.9.0.1", 8071, "cart-0", 1, 180 },`
	wantBe := `{ "10.1.0.1", 8050, "backend-0" },`
	for _, w := range []string{wantCart, wantBe, "listen 18083", "ttft_limit_ms = 60000"} {
		if !strings.Contains(conf, w) {
			t.Errorf("openresty conf missing %q\n%s", w, conf)
		}
	}
	if strings.Index(conf, wantCart) > strings.Index(conf, wantBe) {
		t.Errorf("CART peer 应在后端 peer 之前\n%s", conf)
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
