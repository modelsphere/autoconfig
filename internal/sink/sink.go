// Package sink renders discovered peers into each consumer's config format
// (pure functions RenderCart / RenderRoute / ResolveSources, called by the controller).
package sink

import (
	"fmt"

	"autoconfig/internal/config"
)

// nameUnnamed 给没有名字的 peer 生成名字。
// 【优先用所在节点名】(Peer.Node,发现时从 EndpointSlice.nodeName / pod.spec.nodeName 取):
// 节点名是稳定的物理标识,便于从路由日志/响应头直接定位到机器;而原先的 "<target>-N"
// 用的是发现结果【数组下标】,而 peers 按 IP 排序 —— pod 重建换 IP 后同一后端的编号会变,
// 名字不稳定指向同一台机器。
//
// 同节点多 pod 时【不再追加 "#2"/"#3" 序号】,一律用裸节点名。理由:name 全程只作展示,
// 从不作键 —— openresty 的 name_by_key/gpu_by_key 以 "ip:port" 为键(route.lua),
// X-Routed-Peer 回显 "peer|gpu|name" 里 peer 仍是 ip:port,openresty_peer_* 指标的
// series 唯一性也由 peer 标签保证,bodylog 记的 peer 同样是 ip:port。故重名不会撞、
// 不丢分辨率;而裸节点名让「按物理机聚合」(sum by(name))直接可用,序号反而碍事。
//
// 拿不到节点名(如 Service VIP 兜底 peer)时回落原 "<target>-N"。
func nameUnnamed(peers []config.Peer, target string) []config.Peer {
	out := make([]config.Peer, len(peers))
	for i, p := range peers {
		if p.Name == "" {
			if p.Node != "" {
				p.Name = p.Node
			} else {
				p.Name = fmt.Sprintf("%s-%d", target, i)
			}
		}
		out[i] = p
	}
	return out
}
