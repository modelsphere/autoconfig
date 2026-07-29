package sink

import (
	"fmt"
	"strings"

	"autoconfig/internal/config"
)

// RenderMonitor 把发现的后端渲染成 monitor.conf 的 service 行(每个后端实例一行)。
// 格式:service: <name> | <url> | <model> | <gpu_type>(与现有 monitor.conf 约定一致)。
// name 用 "<mrName>-<i>";model 为空用 mrName;gpuType 可空。
func RenderMonitor(mrName, model, gpuType string, peers []config.Peer) string {
	if model == "" {
		model = mrName
	}
	var b strings.Builder
	for i, p := range peers {
		fmt.Fprintf(&b, "service: %s-%d | http://%s:%d | %s | %s\n", mrName, i, p.IP, p.Port, model, gpuType)
	}
	return b.String()
}
