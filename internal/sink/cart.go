package sink

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"autoconfig/internal/config"
)

// RenderCart 把 base(目标 key 里现有的内容)解析成 YAML,覆盖其中的 workers 键 = 发现的后端,再序列化回去。
// 用 YAML 解析而非字符串拼接 → base 里键的顺序/嵌套/是否已有 workers 都无所谓,只替换 workers,
// 其余原样保留(不受「workers 必须放最后」约束)。maxLoad<=0 用默认 20。
//
// base 为空 = 那个 key 还没有内容(chart 把底稿放在别的 key、workers 这个 key 由首轮写出),
// 从空文档起渲染,输出只有 workers —— 没有任何东西会被覆盖。
//
// ⚠️「读不到就别写」那道 fail-safe 在调用方:controller 区分【ConfigMap 读不到】(缺失 / 瞬时失败 →
// 跳过本轮)与【ConfigMap 正常、key 还没内容】(照写)。若拿读失败当空 base 渲染,会把 live 的
// server/proxy/health 段 clobber 成只剩 workers → cart 只接受「仅 workers 变化」的 reload → 整包拒绝 →
// workers 永久冻结在启动集合(2026-08-13 mf-fallback 踩坑)。
func RenderCart(base string, peers []config.Peer, maxLoad int) (string, error) {
	if maxLoad <= 0 {
		maxLoad = 20
	}
	doc := map[string]interface{}{}
	if err := yaml.Unmarshal([]byte(base), &doc); err != nil {
		return "", fmt.Errorf("parse cart base config.yaml: %w", err)
	}
	if doc == nil {
		doc = map[string]interface{}{}
	}
	doc["workers"] = workerList(peers, maxLoad) // 覆盖(base 里若有 workers 占位一并替换)
	out, err := yaml.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("marshal cart config.yaml: %w", err)
	}
	return string(out), nil
}

// workerList 把发现的 peer 转成 CART 的 workers 条目;p.MaxLoad 为 0 时用 maxLoad 兜底。
func workerList(peers []config.Peer, maxLoad int) []map[string]interface{} {
	workers := make([]map[string]interface{}, 0, len(peers))
	for _, p := range peers {
		ml := p.MaxLoad
		if ml == 0 {
			ml = maxLoad
		}
		workers = append(workers, map[string]interface{}{
			"url":      fmt.Sprintf("http://%s:%d", p.IP, p.Port),
			"max_load": ml,
		})
	}
	return workers
}
