package discovery

import (
	"context"
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"autoconfig/internal/config"
)

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
			discoveryv1.Endpoint{Addresses: []string{"10.1.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: boolp(false)}},         // 未就绪
			discoveryv1.Endpoint{Addresses: []string{"10.1.0.3"}, Conditions: discoveryv1.EndpointConditions{}},                             // Ready=nil → 就绪
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
