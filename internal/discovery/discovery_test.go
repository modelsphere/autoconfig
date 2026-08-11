package discovery

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"autoconfig/internal/config"
)

func svcObj(name, ns, clusterIP string, ports ...int32) *corev1.Service {
	var sp []corev1.ServicePort
	for _, p := range ports {
		sp = append(sp, corev1.ServicePort{Port: p})
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       corev1.ServiceSpec{ClusterIP: clusterIP, Ports: sp},
	}
}

// ServiceClusterIP:返回 Service 的 ClusterIP(VIP)单 peer;headless/无 VIP 报错;端口按 frontend 取/校验。
func TestServiceClusterIP(t *testing.T) {
	cs := fake.NewSimpleClientset(
		svcObj("cart-glm", "glm", "10.96.0.71", 8071),
		svcObj("headless", "glm", corev1.ClusterIPNone, 8050),
		svcObj("multi", "glm", "10.96.0.9", 8071, 9090),
	)
	ctx := context.Background()
	// 单端口:自动取 frontend 端口
	peers, err := ServiceClusterIP(ctx, cs, config.Target{Namespace: "glm", Service: "cart-glm"})
	if err != nil || len(peers) != 1 || peers[0].IP != "10.96.0.71" || peers[0].Port != 8071 {
		t.Fatalf("single-port: got %v err %v", peers, err)
	}
	// headless(ClusterIP=None)→ 报错(没有可兜底的 VIP)
	if _, err := ServiceClusterIP(ctx, cs, config.Target{Namespace: "glm", Service: "headless"}); err == nil {
		t.Errorf("headless 应报错(无 ClusterIP)")
	}
	// 多端口未指定 port → 报错(要求显式)
	if _, err := ServiceClusterIP(ctx, cs, config.Target{Namespace: "glm", Service: "multi"}); err == nil {
		t.Errorf("多端口未指定 port 应报错")
	}
	// 多端口 + hint 命中 frontend → OK
	if peers, err := ServiceClusterIP(ctx, cs, config.Target{Namespace: "glm", Service: "multi", Port: 9090}); err != nil || peers[0].Port != 9090 {
		t.Fatalf("multi-port hint: got %v err %v", peers, err)
	}
	// hint 不是 frontend 端口(误传 targetPort)→ 报错
	if _, err := ServiceClusterIP(ctx, cs, config.Target{Namespace: "glm", Service: "cart-glm", Port: 6700}); err == nil {
		t.Errorf("非 frontend 端口应报错")
	}
}

func boolp(b bool) *bool { return &b }

func slice(name, svc string, eps ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Name: name, Namespace: "glm", Labels: map[string]string{serviceNameLabel: svc}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   eps,
	}
}

// EndpointSlice 发现:只取就绪端点(Ready=nil 视作就绪);未就绪/排空中(Ready=false)排除;多分片聚合。
func TestDiscoverEndpointSlices(t *testing.T) {
	cs := fake.NewSimpleClientset(
		slice("glm-leader-1", "glm-leader",
			discoveryv1.Endpoint{Addresses: []string{"10.1.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: boolp(true)}},
			discoveryv1.Endpoint{Addresses: []string{"10.1.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: boolp(false)}},                                                 // 未就绪
			discoveryv1.Endpoint{Addresses: []string{"10.1.0.3"}, Conditions: discoveryv1.EndpointConditions{}},                                                                    // Ready=nil → 就绪
			discoveryv1.Endpoint{Addresses: []string{"10.1.0.4"}, Conditions: discoveryv1.EndpointConditions{Ready: boolp(false), Serving: boolp(true), Terminating: boolp(true)}}, // 排空中 → 排除
		),
		slice("glm-leader-2", "glm-leader", // 分片 2(sharding)聚合
			discoveryv1.Endpoint{Addresses: []string{"10.1.0.5"}, Conditions: discoveryv1.EndpointConditions{Ready: boolp(true)}},
		),
	)
	tgt := config.Target{Namespace: "glm", Service: "glm-leader", Port: 8050}

	peers, err := Discover(context.Background(), cs, tgt)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	got := ips(peers)
	want := []string{"10.1.0.1", "10.1.0.3", "10.1.0.5"} // 就绪 + Ready=nil + 分片2;排除 .2/.4
	if !eq(got, want) {
		t.Fatalf("ready-only endpoints: got %v want %v", got, want)
	}
	for _, p := range peers {
		if p.Port != 8050 {
			t.Errorf("port: got %d want 8050", p.Port)
		}
	}

	// IncludeNotReady=true → 全带上
	tgt.IncludeNotReady = true
	peers, _ = Discover(context.Background(), cs, tgt)
	if got, want := ips(peers), []string{"10.1.0.1", "10.1.0.2", "10.1.0.3", "10.1.0.4", "10.1.0.5"}; !eq(got, want) {
		t.Fatalf("includeNotReady: got %v want %v", got, want)
	}
}

func ips(ps []config.Peer) []string {
	var s []string
	for _, p := range ps {
		s = append(s, p.IP)
	}
	return s
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 端口自动推导:Target.Port==0 时从 EndpointSlice 的 ports 取(唯一端口);多端口报错。
func TestDerivePortFromSlice(t *testing.T) {
	i32 := func(v int32) *int32 { return &v }
	withPorts := func(s *discoveryv1.EndpointSlice, ports ...int32) *discoveryv1.EndpointSlice {
		for _, p := range ports {
			s.Ports = append(s.Ports, discoveryv1.EndpointPort{Port: i32(p)})
		}
		return s
	}
	ep := discoveryv1.Endpoint{Addresses: []string{"10.2.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: boolp(true)}}

	// 单端口 → 自动取 8050
	cs := fake.NewSimpleClientset(withPorts(slice("k-1", "k", ep), 8050))
	peers, err := Discover(context.Background(), cs, config.Target{Namespace: "glm", Service: "k"}) // Port 省略
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(peers) != 1 || peers[0].Port != 8050 {
		t.Fatalf("auto port: got %+v want port 8050", peers)
	}

	// 显式 port 覆盖自动推导
	peers, _ = Discover(context.Background(), cs, config.Target{Namespace: "glm", Service: "k", Port: 9000})
	if peers[0].Port != 9000 {
		t.Errorf("explicit port override: got %d want 9000", peers[0].Port)
	}

	// 多端口且未显式配 → 报错
	cs2 := fake.NewSimpleClientset(withPorts(slice("k-1", "k", ep), 8050, 8060))
	if _, err := Discover(context.Background(), cs2, config.Target{Namespace: "glm", Service: "k"}); err == nil {
		t.Error("多端口未配 port 应报错")
	}
}

// normalizeGPUProduct:GFD 值取型号短名。
func TestNormalizeGPUProduct(t *testing.T) {
	cases := map[string]string{
		"NVIDIA-A100-SXM4-80GB": "A100",
		"NVIDIA-H100-80GB-HBM3": "H100",
		"NVIDIA-H200":           "H200",
		"NVIDIA-A800-80GB":      "A800",
		"Tesla-V100-SXM2-16GB":  "V100",
		"":                      "",
		"weird":                 "weird",
	}
	for in, want := range cases {
		if got := normalizeGPUProduct(in); got != want {
			t.Errorf("normalizeGPUProduct(%q)=%q want %q", in, got, want)
		}
	}
}

// nodeObj:带 GFD gpu.product label 的节点。
func nodeObj(name, product string) *corev1.Node {
	l := map[string]string{}
	if product != "" {
		l[gpuProductLabel] = product
	}
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: l}}
}

// AnnotateGPUTypes:【逐 peer】按各自节点标 GPU 型号 —— 混布场景每个 peer 各自正确
// (旧实现取第一个端点的型号套给全部 peer,混布时会把其余标错)。
func TestAnnotateGPUTypesMixed(t *testing.T) {
	cs := fake.NewSimpleClientset(
		nodeObj("n-h100", "NVIDIA-H100-80GB-HBM3"),
		nodeObj("n-a100", "NVIDIA-A100-SXM4-80GB"),
		nodeObj("n-bare", ""), // 无 GPU label
	)
	peers := []config.Peer{
		{IP: "10.1.0.1", Port: 8050, Node: "n-h100"},
		{IP: "10.1.0.2", Port: 8050, Node: "n-a100"},
		{IP: "10.1.0.3", Port: 8050, Node: "n-bare"},
		{IP: "10.1.0.4", Port: 8050, Node: "n-missing"}, // 节点不存在
		{IP: "10.1.0.5", Port: 8050},                    // 无 node 名
	}
	AnnotateGPUTypes(context.Background(), cs, peers)

	want := []string{"H100", "A100", "", "", ""}
	for i, w := range want {
		if peers[i].GPU != w {
			t.Errorf("peers[%d].GPU=%q want %q", i, peers[i].GPU, w)
		}
	}
}

// 同节点的 label 只查一次(node → 型号 缓存),避免 peer 多时反复打 API server。
func TestAnnotateGPUTypesCachesPerNode(t *testing.T) {
	cs := fake.NewSimpleClientset(nodeObj("n1", "NVIDIA-H200"))
	gets := 0
	cs.PrependReactor("get", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		gets++
		return false, nil, nil // 继续走默认 tracker
	})
	peers := []config.Peer{
		{IP: "10.1.0.1", Node: "n1"}, {IP: "10.1.0.2", Node: "n1"}, {IP: "10.1.0.3", Node: "n1"},
	}
	AnnotateGPUTypes(context.Background(), cs, peers)
	if gets != 1 {
		t.Errorf("同节点应只查 1 次 Node,实际 %d 次", gets)
	}
	for i := range peers {
		if peers[i].GPU != "H200" {
			t.Errorf("peers[%d].GPU=%q want H200", i, peers[i].GPU)
		}
	}
}
