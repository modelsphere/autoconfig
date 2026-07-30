package sink

import (
	"bytes"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"autoconfig/internal/config"
)

// luaVal 把 values 的字符串安全渲染成 lua 字面量:数字/布尔/nil 原样出,其余当字符串加引号 + 转义
// (防止值里含 , } 换行 " 破坏 conf 或注入)。
func luaVal(s string) string {
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return s
	}
	if s == "true" || s == "false" || s == "nil" {
		return s
	}
	s = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", " ", "\r", " ").Replace(s)
	return `"` + s + `"`
}

// defaultRouteTmpl 内置路由模板(route.tmpl 编进二进制,controller 无需挂载模板文件)。
//
//go:embed route.tmpl
var defaultRouteTmpl string

// RouteData 喂给 route.tmpl。Route/Listen 是结构;Extra 是任意调优项(原样渲染进 lua 返回表,
// text/template range map 按 key 排序 → 确定性);Peers 是发现结果。
type RouteData struct {
	Route  string
	Listen int
	Extra  map[string]string
	Peers  []config.Peer
}

func RenderRoute(d RouteData) (string, error) {
	tmpl, err := template.New("route").Funcs(template.FuncMap{"luaVal": luaVal}).Parse(defaultRouteTmpl)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, d); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// ResolveSources builds a route's ordered peer list. If sources is set, it concatenates each
// source's discovered peers (applying that source's priority/maxConcurrency override) in order —
// e.g. CART(priority 1) then backends(priority 0). Otherwise it falls back to the single target.
// Names are assigned per source ("<target>-N"), so peers stay uniquely named across sources.
func ResolveSources(peersByTarget map[string][]config.Peer, sources []config.RouteSource, singleTarget string) []config.Peer {
	if len(sources) == 0 {
		return nameUnnamed(peersByTarget[singleTarget], singleTarget)
	}
	var all []config.Peer
	for _, src := range sources {
		for _, p := range nameUnnamed(peersByTarget[src.Target], src.Target) {
			if src.Priority != 0 {
				p.Priority = src.Priority
			}
			if src.MaxConcurrency != 0 {
				p.MaxConcurrency = src.MaxConcurrency
			}
			all = append(all, p)
		}
	}
	return all
}
