package sink

import (
	"fmt"
	"strings"

	"autoconfig/internal/config"
)

// RenderMonitor 渲染一个模型在 monitor.conf 里的块(与现有 monitor.conf 约定一致):
//
//	service: <mrName>-<i> | http://ip:port | <model> | <gpu_type>   —— 后端(每实例一行)
//	nginx:   <nginxName>-nginx-<i> | http://ip:<port>/<route>       —— openresty 入口(路径路由:port=8080 dispatch、
//	                                                                   带 /<route> 路径;monitor 探 /<route>/_active_conns 等,per-model 探活/503 照旧)
//	router:  <mrName>-router-<i> | http://ip:port/workers            —— CART(Router tab;url 为 /workers 端点)
//
// model 为空用 mrName;route 为 nginx 行的路径 key;nginxPeers/routerPeers 为空则不出对应行。
// gpuType:CRD 显式配置(整条 route 覆盖);为空时逐 peer 用 Peer.GPU(自动推导,支持混布)。
func RenderMonitor(mrName, model, gpuType, nginxName, route string, backends, nginxPeers, routerPeers []config.Peer) string {
	if model == "" {
		model = mrName
	}
	var b strings.Builder
	for i, p := range backends {
		// gpuType(来自 CRD 显式配置)优先;否则用该 peer 自己节点推导出的型号 —— 混布时各行各自正确。
		g := gpuType
		if g == "" {
			g = p.GPU
		}
		fmt.Fprintf(&b, "service: %s-%d | http://%s:%d | %s | %s\n", mrName, i, p.IP, p.Port, model, g)
	}
	for i, p := range nginxPeers {
		// 加 -nginx- 判别段(与 router 的 -router- 平行):否则当 model==mrName 时,nginx 行名 <model>-<i>
		// 会和 service 行名 <mrName>-<i> 撞车 → monitor 校验「nginx name conflicts with service name」拒绝启动。
		fmt.Fprintf(&b, "nginx: %s-nginx-%d | http://%s:%d/%s\n", nginxName, i, p.IP, p.Port, route)
	}
	for i, p := range routerPeers {
		fmt.Fprintf(&b, "router: %s-router-%d | http://%s:%d/workers\n", mrName, i, p.IP, p.Port)
	}
	return b.String()
}
