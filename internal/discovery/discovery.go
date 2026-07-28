// Package discovery lists backend pods per target (client-go) and returns their endpoints.
package discovery

import (
	"context"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"autoconfig/internal/config"
)

// Discover returns the current endpoints for one target (sorted, deduped).
func Discover(ctx context.Context, cs kubernetes.Interface, t config.Target) ([]config.Peer, error) {
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
	sort.Slice(out, func(i, j int) bool {
		if out[i].IP != out[j].IP {
			return out[i].IP < out[j].IP
		}
		return out[i].Port < out[j].Port
	})
	return out, nil
}

func isReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
