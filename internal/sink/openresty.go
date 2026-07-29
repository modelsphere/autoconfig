package sink

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"

	"autoconfig/internal/config"
)

// defaultRouteTmpl 内置路由模板(= deploy/openresty-route.tmpl 的副本),controller 无需挂载模板文件。
//
//go:embed route.tmpl
var defaultRouteTmpl string

// RenderRoute 用模板给一条 openresty route 生成整个 conf(dicts + server + register_route + peers)。
// tmplContent 为空用内置模板。
func RenderRoute(tmplContent string, values map[string]interface{}, peers []config.Peer) (string, error) {
	if tmplContent == "" {
		tmplContent = defaultRouteTmpl
	}
	tmpl, err := template.New("route").Parse(tmplContent)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	data := struct {
		Values map[string]interface{}
		Peers  []config.Peer
	}{Values: values, Peers: peers}
	if err := tmpl.Execute(&buf, data); err != nil {
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
