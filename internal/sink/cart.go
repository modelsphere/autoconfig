package sink

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"autoconfig/internal/config"
)

// RenderCart 把底稿 config.yaml(chart 的 values.baseConfig 建在 cart-config 里)解析成 YAML,
// 覆盖其中的 workers 键 = 发现的后端,再序列化回去。用 YAML 解析而非字符串拼接 →
// 底稿里键的顺序/嵌套/是否已有 workers 都无所谓,只替换 workers,其余原样保留(不受「workers 必须放最后」约束)。
// maxLoad<=0 用默认 20。
//
// base 为空【报错】,绝不 fallback 到内置最小默认:那份默认只有 server.port=6700、没有 chart 的
// proxy/health/cache,写下去会把运行中 cart 的 base 段 clobber 掉 → cart 拒绝非-workers-only 的 reload →
// workers 永久冻结(2026-08-13 mf-fallback 踩坑)。调用方(controller)已在 base=="" 时跳过写入、保留旧配置;
// 这里再兜一道底,防止别的调用路径误传空 base。
func RenderCart(base string, peers []config.Peer, maxLoad int) (string, error) {
	if base == "" {
		return "", fmt.Errorf("cart base config.yaml is empty; refusing to render (would clobber chart baseConfig server/proxy/health)")
	}
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
	doc["workers"] = workers // 覆盖(底稿里若有 workers 占位一并替换)
	out, err := yaml.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("marshal cart config.yaml: %w", err)
	}
	return string(out), nil
}
