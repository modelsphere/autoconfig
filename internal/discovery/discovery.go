// Package discovery lists a target's backends per target and returns their endpoints.
// 两条发现路径(按 Target 配置二选一):
//   - Service 非空 → EndpointSlice(discovery.k8s.io/v1):拿 Service 背后端点,用原生 Ready/Terminating 语义。
//   - Selector 非空 → pod label 发现(兜底:没建 Service 的单机/单卡)。
package discovery

import (
	"context"
	"sort"

	corev1 "k8s.io/api/core/v1"
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

// discoverEndpointSlices 聚合某 Service 的全部 EndpointSlice,取就绪端点(pod IP + t.Port)。
func discoverEndpointSlices(ctx context.Context, cs kubernetes.Interface, t config.Target) ([]config.Peer, error) {
	slices, err := cs.DiscoveryV1().EndpointSlices(t.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: serviceNameLabel + "=" + t.Service,
	})
	if err != nil {
		return nil, err
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
				out = append(out, config.Peer{IP: addr, Port: t.Port})
			}
		}
	}
	sortPeers(out)
	return out, nil
}

// discoverPods:pod label 发现(兜底)。
func discoverPods(ctx context.Context, cs kubernetes.Interface, t config.Target) ([]config.Peer, error) {
	pods, err := cs.CoreV1().Pods(t.Namespace).List(ctx, metav1.ListOptions{LabelSelector: t.Selector})
	if err != nil {
		return nil, err
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
		out = append(out, config.Peer{IP: p.Status.PodIP, Port: t.Port})
	}
	sortPeers(out)
	return out, nil
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
