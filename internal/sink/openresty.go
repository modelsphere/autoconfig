package sink

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"autoconfig/internal/config"
)

// defaultRouteTmpl 内置路由模板(= deploy/openresty-route.tmpl 的副本)。
// Template 路径为空时用它 —— CRD controller 无需挂载模板文件。
//
//go:embed route.tmpl
var defaultRouteTmpl string

// OpenrestySink 生成 openresty 各路由的 conf。两种模式(可并用):
//   (A) rewrite:读 BaseConfigDir 的现成 conf,按 RouteByTarget 改写 peers 块(其余透传)。
//   (B) template:用 Template 给每条 Routes 生成整个 conf(dicts+server+register_route+peers)。
type OpenrestySink struct{ s config.Sink }

func (o *OpenrestySink) Kind() string          { return "openresty" }
func (o *OpenrestySink) Name() string          { return "openresty:" + o.s.OutputConfigMap }
func (o *OpenrestySink) ConfigMapNS() string   { ns, _ := splitNSName(o.s.OutputConfigMap); return ns }
func (o *OpenrestySink) ConfigMapName() string { _, n := splitNSName(o.s.OutputConfigMap); return n }

func (o *OpenrestySink) Render(peersByTarget map[string][]config.Peer) (Rendered, error) {
	out := Rendered{}

	// (A) rewrite 模式:透传 baseConfigDir + 改写指定 conf 的 peers 块
	if o.s.BaseConfigDir != "" {
		entries, err := os.ReadDir(o.s.BaseConfigDir)
		if err != nil {
			return nil, fmt.Errorf("read baseConfigDir %s: %w", o.s.BaseConfigDir, err)
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".") || e.IsDir() { // 跳过 ConfigMap 内部条目 ..data/..时间戳
				continue
			}
			b, err := os.ReadFile(filepath.Join(o.s.BaseConfigDir, e.Name()))
			if err != nil {
				return nil, err
			}
			out[e.Name()] = string(b)
		}
		for target, confFile := range o.s.RouteByTarget {
			content, ok := out[confFile]
			if !ok {
				return nil, fmt.Errorf("routeByTarget: conf %q not in baseConfigDir", confFile)
			}
			newContent, err := rewritePeersBlock(content, nameUnnamed(peersByTarget[target], target))
			if err != nil {
				return nil, fmt.Errorf("rewrite peers in %s: %w", confFile, err)
			}
			out[confFile] = newContent
		}
	}

	// (B) template 模式:每条 route 用模板生成整个 conf(Routes 非空即启用;Template 为空用内置模板)
	if len(o.s.Routes) > 0 {
		tmplContent := defaultRouteTmpl
		if o.s.Template != "" {
			tb, err := os.ReadFile(o.s.Template)
			if err != nil {
				return nil, fmt.Errorf("read template %s: %w", o.s.Template, err)
			}
			tmplContent = string(tb)
		}
		for _, r := range o.s.Routes {
			if r.File == "" {
				return nil, fmt.Errorf("route (target %q): file required", r.Target)
			}
			conf, err := RenderRoute(tmplContent, r.Values, ResolveSources(peersByTarget, r.Sources, r.Target))
			if err != nil {
				return nil, fmt.Errorf("render route %s: %w", r.File, err)
			}
			out[r.File] = conf
		}
	}
	return out, nil
}

// RenderRoute 用模板给一条 route 生成整个 conf。tmplContent 为空用内置模板。
// ConfigMap-agent 与 CRD controller 共用。
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

// rewritePeersBlock locates the first uncommented `peers = {` or `_G.PEERS = {` block and
// replaces its contents with the given peers (positional lua form). Brace-counted end detection.
func rewritePeersBlock(text string, peers []config.Peer) (string, error) {
	lines := strings.Split(text, "\n")
	start := -1
	for i, ln := range lines {
		t := strings.TrimSpace(ln)
		if strings.HasPrefix(t, "--") {
			continue // commented line
		}
		if strings.Contains(ln, "_G.PEERS = {") || strings.Contains(ln, "peers = {") {
			start = i
			break
		}
	}
	if start < 0 {
		return "", fmt.Errorf("peers block (`peers = {` / `_G.PEERS = {`) not found")
	}
	braceIdx := strings.Index(lines[start], "{")
	prefix := lines[start][:braceIdx] // e.g. "    peers = " or "        _G.PEERS = "
	indent := leadingSpaces(lines[start])

	depth := 0
	end := -1
	for j := start; j < len(lines); j++ {
		depth += strings.Count(lines[j], "{") - strings.Count(lines[j], "}")
		if depth == 0 && j > start {
			end = j
			break
		}
	}
	if end < 0 {
		return "", fmt.Errorf("peers block braces unbalanced")
	}

	var b strings.Builder
	b.WriteString(prefix + "{\n")
	for _, p := range peers {
		b.WriteString(indent + "    {" + p.LuaTuple() + "},\n")
	}
	b.WriteString(indent + "}")

	newLines := make([]string, 0, len(lines))
	newLines = append(newLines, lines[:start]...)
	newLines = append(newLines, b.String())
	newLines = append(newLines, lines[end+1:]...)
	return strings.Join(newLines, "\n"), nil
}

func leadingSpaces(s string) string {
	return s[:len(s)-len(strings.TrimLeft(s, " \t"))]
}
