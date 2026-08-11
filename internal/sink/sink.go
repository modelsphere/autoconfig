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
// 同一节点上有多个 pod 时(下标 >0 的重复)追加序号去重:"<node>#2"、"<node>#3"…
// 拿不到节点名(如 Service VIP 兜底 peer)时回落原 "<target>-N"。
func nameUnnamed(peers []config.Peer, target string) []config.Peer {
	out := make([]config.Peer, len(peers))
	seen := map[string]int{}
	for i, p := range peers {
		if p.Name == "" {
			if p.Node != "" {
				seen[p.Node]++
				if n := seen[p.Node]; n == 1 {
					p.Name = p.Node
				} else {
					p.Name = fmt.Sprintf("%s#%d", p.Node, n)
				}
			} else {
				p.Name = fmt.Sprintf("%s-%d", target, i)
			}
		}
		out[i] = p
	}
	return out
}
