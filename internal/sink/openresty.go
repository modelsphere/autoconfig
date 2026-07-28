package sink

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"autoconfig/internal/config"
)

// OpenrestySink rewrites the peers block of each route's session_route_<model>.conf.
// It emits every flat file in BaseConfigDir as a ConfigMap key (verbatim, except the rewritten confs).
// (lua/ subdir stays baked in the image or delivered separately; not handled here.)
type OpenrestySink struct{ s config.Sink }

func (o *OpenrestySink) Kind() string          { return "openresty" }
func (o *OpenrestySink) Name() string          { return "openresty:" + o.s.OutputConfigMap }
func (o *OpenrestySink) ConfigMapNS() string   { ns, _ := splitNSName(o.s.OutputConfigMap); return ns }
func (o *OpenrestySink) ConfigMapName() string { _, n := splitNSName(o.s.OutputConfigMap); return n }

func (o *OpenrestySink) Render(peersByTarget map[string][]config.Peer) (Rendered, error) {
	out := Rendered{}
	entries, err := os.ReadDir(o.s.BaseConfigDir)
	if err != nil {
		return nil, fmt.Errorf("read baseConfigDir %s: %w", o.s.BaseConfigDir, err)
	}
	for _, e := range entries {
		// 跳过 ConfigMap 挂载的内部条目(..data 符号链接、..2026_ 时间戳目录)和子目录
		if strings.HasPrefix(e.Name(), ".") || e.IsDir() {
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
			return nil, fmt.Errorf("routeByTarget: conf %q not found in baseConfigDir", confFile)
		}
		peers := nameUnnamed(peersByTarget[target], target)
		newContent, err := rewritePeersBlock(content, peers)
		if err != nil {
			return nil, fmt.Errorf("rewrite peers in %s: %w", confFile, err)
		}
		out[confFile] = newContent
	}
	return out, nil
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
		fields := fmt.Sprintf("%q, %d, %q", p.IP, p.Port, p.Name)
		if p.Priority != 0 || p.MaxConcurrency != 0 {
			fields += fmt.Sprintf(", %d", p.Priority)
			if p.MaxConcurrency != 0 {
				fields += fmt.Sprintf(", %d", p.MaxConcurrency)
			}
		}
		b.WriteString(indent + "    {" + fields + "},\n")
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
