package sink

import (
	"fmt"
	"strings"

	"autoconfig/internal/config"
)

// RenderMonitor 渲染一个模型在 monitor.conf 里的块(与现有 monitor.conf 约定一致):
//   service: <mrName>-<i> | http://ip:port | <model> | <gpu_type>   —— 后端(每实例一行)
//   nginx:   <nginxName>-<i> | http://ip:port                        —— openresty 入口(base url;monitor 探 /_active_conns)
//   router:  <mrName>-router-<i> | http://ip:port/workers            —— CART(Router tab;url 为 /workers 端点)
// model 为空用 mrName;nginxPeers/routerPeers 为空则不出对应行。
func RenderMonitor(mrName, model, gpuType, nginxName string, backends, nginxPeers, routerPeers []config.Peer) string {
	if model == "" {
		model = mrName
	}
	var b strings.Builder
	for i, p := range backends {
		fmt.Fprintf(&b, "service: %s-%d | http://%s:%d | %s | %s\n", mrName, i, p.IP, p.Port, model, gpuType)
	}
	for i, p := range nginxPeers {
		fmt.Fprintf(&b, "nginx: %s-%d | http://%s:%d\n", nginxName, i, p.IP, p.Port)
	}
	for i, p := range routerPeers {
		fmt.Fprintf(&b, "router: %s-router-%d | http://%s:%d/workers\n", mrName, i, p.IP, p.Port)
	}
	return b.String()
}
