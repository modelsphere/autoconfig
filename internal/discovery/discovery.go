// Package discovery lists a target's backends per target and returns their endpoints.
// 两条发现路径(按 Target 配置二选一):
//   - Service 非空 → EndpointSlice(discovery.k8s.io/v1):拿 Service 背后端点,用原生 Ready/Terminating 语义。
//   - Selector 非空 → pod label 发现(兜底:没建 Service 的单机/单卡)。
//
// 端口(Target.Port):>0 显式指定;==0 则自动推导——Service 路径从 EndpointSlice 的端口取、
// selector 路径从 pod 的 containerPort 取(仅单端口时可推;多端口需显式配 port)。
package discovery

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"autoconfig/internal/config"
)

// serviceNameLabel:EndpointSlice 靠这个 label 归属到 Service(单 Service >100 端点会分多片,全按此聚合)。
const serviceNameLabel = "kubernetes.io/service-name"

// Discover returns the current endpoints for one target (sorted, deduped).
func Discover(ctx context.Context, cs kubernetes.Interface, t config.Target) ([]config.Peer, error) {
	if t.Service != "" {
		return discoverEndpointSlices(ctx, cs, t)
	}
	return discoverPods(ctx, cs, t)
}

// discoverEndpointSlices 聚合某 Service 的全部 EndpointSlice,取就绪端点(pod IP + 端口)。
// t.Port==0 时端口从 EndpointSlice 自动取(单端口)。
func discoverEndpointSlices(ctx context.Context, cs kubernetes.Interface, t config.Target) ([]config.Peer, error) {
	slices, err := cs.DiscoveryV1().EndpointSlices(t.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: serviceNameLabel + "=" + t.Service,
	})
	if err != nil {
		return nil, err
	}
	port := t.Port
	if port == 0 { // 未显式配 → 从 EndpointSlice 的端口推导
		port, err = derivePortFromSlices(slices.Items)
		if err != nil {
			return nil, fmt.Errorf("service %s/%s: %w", t.Namespace, t.Service, err)
		}
	}
	seen := map[string]bool{}
	var out []config.Peer
	for i := range slices.Items {
		for _, ep := range slices.Items[i].Endpoints {
			// Ready 为 nil 按 k8s 约定视作就绪;IncludeNotReady 时不过滤(排空中端点 Ready=false 会被默认排除)。
			ready := ep.Conditions.Ready == nil || *ep.Conditions.Ready
			if !t.IncludeNotReady && !ready {
				continue
			}
			for _, addr := range ep.Addresses {
				if addr == "" || seen[addr] {
					continue
				}
				seen[addr] = true
				out = append(out, config.Peer{IP: addr, Port: port})
			}
		}
	}
	sortPeers(out)
	return out, nil
}

// derivePortFromSlices 收集所有 EndpointSlice 里的端口,唯一时返回它;0 个或多个则要求显式配。
func derivePortFromSlices(items []discoveryv1.EndpointSlice) (int, error) {
	set := map[int32]bool{}
	for i := range items {
		for _, p := range items[i].Ports {
			if p.Port != nil && *p.Port > 0 {
				set[*p.Port] = true
			}
		}
	}
	return uniquePort(set, "EndpointSlice")
}

// discoverPods:pod label 发现(兜底)。t.Port==0 时端口从 pod containerPort 推导(单端口)。
func discoverPods(ctx context.Context, cs kubernetes.Interface, t config.Target) ([]config.Peer, error) {
	pods, err := cs.CoreV1().Pods(t.Namespace).List(ctx, metav1.ListOptions{LabelSelector: t.Selector})
	if err != nil {
		return nil, err
	}
	port := t.Port
	if port == 0 {
		port, err = derivePortFromPods(pods.Items)
		if err != nil {
			return nil, fmt.Errorf("selector %q: %w", t.Selector, err)
		}
	}
	seen := map[string]bool{}
	var out []config.Peer
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp != nil { // terminating
			continue
		}
		if p.Status.Phase != corev1.PodRunning || p.Status.PodIP == "" {
			continue
		}
		if !t.IncludeNotReady && !isReady(p) {
			continue
		}
		if seen[p.Status.PodIP] {
			continue
		}
		seen[p.Status.PodIP] = true
		out = append(out, config.Peer{IP: p.Status.PodIP, Port: port})
	}
	sortPeers(out)
	return out, nil
}

// derivePortFromPods 收集所有 pod 各容器的 containerPort,唯一时返回它。
func derivePortFromPods(items []corev1.Pod) (int, error) {
	set := map[int32]bool{}
	for i := range items {
		for _, c := range items[i].Spec.Containers {
			for _, cp := range c.Ports {
				if cp.ContainerPort > 0 {
					set[cp.ContainerPort] = true
				}
			}
		}
	}
	return uniquePort(set, "pod containerPort")
}

func uniquePort(set map[int32]bool, src string) (int, error) {
	switch len(set) {
	case 1:
		for p := range set {
			return int(p), nil
		}
	case 0:
		return 0, fmt.Errorf("%s 未声明端口,请显式配 port", src)
	}
	return 0, fmt.Errorf("%s 有多个端口,请显式配 port 指定后端端口", src)
}

func sortPeers(out []config.Peer) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].IP != out[j].IP {
			return out[i].IP < out[j].IP
		}
		return out[i].Port < out[j].Port
	})
}

func isReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
