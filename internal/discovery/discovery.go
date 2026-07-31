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
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"autoconfig/internal/config"
)

// serviceNameLabel:EndpointSlice 靠这个 label 归属到 Service(单 Service >100 端点会分多片,全按此聚合)。
const serviceNameLabel = "kubernetes.io/service-name"

// gpuProductLabel:NVIDIA GPU Feature Discovery(GFD)给节点打的 GPU 型号 label,
// 值如 "NVIDIA-A100-SXM4-80GB" / "NVIDIA-H100-80GB-HBM3" / "NVIDIA-H200"。
const gpuProductLabel = "nvidia.com/gpu.product"

// GPUType 推导 target 后端所在节点的 GPU 型号:取第一个就绪端点的 nodeName → 读 node 的 gpu.product label →
// 归一化成短名(A100/H100/H200…)。推不出(无端点/节点无 GPU label/无 node 读权限)返回 ""。
func GPUType(ctx context.Context, cs kubernetes.Interface, t config.Target) string {
	node := firstNodeName(ctx, cs, t)
	if node == "" {
		return ""
	}
	n, err := cs.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return normalizeGPUProduct(n.Labels[gpuProductLabel])
}

// firstNodeName 取 target 第一个就绪端点所在的 nodeName(Service 走 EndpointSlice.nodeName,selector 走 pod.spec.nodeName)。
func firstNodeName(ctx context.Context, cs kubernetes.Interface, t config.Target) string {
	if t.Service != "" {
		slices, err := cs.DiscoveryV1().EndpointSlices(t.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: serviceNameLabel + "=" + t.Service,
		})
		if err != nil {
			return ""
		}
		for i := range slices.Items {
			for _, ep := range slices.Items[i].Endpoints {
				ready := ep.Conditions.Ready == nil || *ep.Conditions.Ready
				if ready && ep.NodeName != nil && *ep.NodeName != "" {
					return *ep.NodeName
				}
			}
		}
		return ""
	}
	pods, err := cs.CoreV1().Pods(t.Namespace).List(ctx, metav1.ListOptions{LabelSelector: t.Selector})
	if err != nil {
		return ""
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning && p.Spec.NodeName != "" {
			return p.Spec.NodeName
		}
	}
	return ""
}

// normalizeGPUProduct 把 GFD 的 gpu.product 值取出型号短名:"NVIDIA-A100-SXM4-80GB" → "A100"、
// "NVIDIA-H100-80GB-HBM3" → "H100"、"NVIDIA-H200" → "H200"。GFD 值格式 = <厂商>-<型号>-<形态>-<显存>,
// 型号恒在厂商前缀之后。认不出格式则原样返回(至少保留信息)。
func normalizeGPUProduct(product string) string {
	if product == "" {
		return ""
	}
	parts := strings.Split(product, "-")
	i := 0
	if len(parts) > 1 && (strings.EqualFold(parts[0], "NVIDIA") || strings.EqualFold(parts[0], "Tesla")) {
		i = 1 // 跳过厂商前缀,型号是下一段
	}
	if i < len(parts) && parts[i] != "" {
		return parts[i]
	}
	return product
}

// ServiceClusterIP 返回某 Service 的 ClusterIP(VIP)作为单个 peer —— 用于「稳定兜底」:VIP 终生不变、
// 由 kube-proxy 维护到 pod 的映射,故后端 rollout / autoconfig(operator)宕 都不影响它可达(kube-proxy
// 是平台组件、一直在)。代价:走 VIP 无 per-pod 亲和/least_conn(降级)。故只当最低优先级兜底 peer,
// 平时用 pod-IP 层。headless(ClusterIP=None)/ 未分配 VIP 的 Service 报错(它没有可兜底的 VIP)。
// 端口:t.Port>0 时必须匹配 Service 的某个 frontend 端口(否则报错,防误用 targetPort);否则取唯一 frontend 端口。
func ServiceClusterIP(ctx context.Context, cs kubernetes.Interface, t config.Target) ([]config.Peer, error) {
	svc, err := cs.CoreV1().Services(t.Namespace).Get(ctx, t.Service, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	vip := svc.Spec.ClusterIP
	if vip == "" || vip == corev1.ClusterIPNone {
		return nil, fmt.Errorf("service %s/%s 无 ClusterIP(headless 或未分配)——不能作 VIP 兜底", t.Namespace, t.Service)
	}
	port, err := serviceFrontendPort(svc, t.Port)
	if err != nil {
		return nil, fmt.Errorf("service %s/%s: %w", t.Namespace, t.Service, err)
	}
	return []config.Peer{{IP: vip, Port: port}}, nil
}

// serviceFrontendPort 取 Service 的 frontend 端口(spec.ports[].port,= VIP 对外端口)。
// hint>0:必须是其中之一(挡住误传 targetPort);hint==0:唯一端口推导(多端口要求显式)。
func serviceFrontendPort(svc *corev1.Service, hint int) (int, error) {
	set := map[int32]bool{}
	for _, p := range svc.Spec.Ports {
		if p.Port > 0 {
			set[p.Port] = true
		}
	}
	if hint > 0 {
		if set[int32(hint)] {
			return hint, nil
		}
		return 0, fmt.Errorf("端口 %d 不是 Service 的 frontend 端口(spec.ports[].port)", hint)
	}
	return uniquePort(set, "Service frontend")
}

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
	if port == 0 {
		if t.PortName != "" { // 按名取(多端口 Service 消歧,如 openresty 的 dispatch/health)
			port, err = portByName(slices.Items, t.PortName)
		} else { // 从 EndpointSlice 的端口推导(唯一端口)
			port, err = derivePortFromSlices(slices.Items)
		}
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

// portByName 从 EndpointSlice 里取名为 name 的端口(Service 端口名会镜像到 EndpointSlice)。
func portByName(items []discoveryv1.EndpointSlice, name string) (int, error) {
	for i := range items {
		for _, p := range items[i].Ports {
			if p.Name != nil && *p.Name == name && p.Port != nil && *p.Port > 0 {
				return int(*p.Port), nil
			}
		}
	}
	return 0, fmt.Errorf("EndpointSlice 无名为 %q 的端口", name)
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
