// Package sink renders discovered peers into each consumer's config format.
package sink

import (
	"fmt"
	"strings"

	"autoconfig/internal/config"
)

// Rendered is the ConfigMap data (key -> file content) a sink produces.
type Rendered map[string]string

// Sink renders one consumer's ConfigMap from discovered peers, keyed by target name.
type Sink interface {
	Kind() string
	Name() string                // for logging
	ConfigMapNS() string         // target ConfigMap namespace
	ConfigMapName() string       // target ConfigMap name
	Render(peersByTarget map[string][]config.Peer) (Rendered, error)
}

// New builds a Sink from config.
func New(s config.Sink) (Sink, error) {
	switch s.Kind {
	case "cart":
		return &CartSink{s: s}, nil
	case "openresty":
		return &OpenrestySink{s: s}, nil
	default:
		return nil, fmt.Errorf("unknown sink kind %q", s.Kind)
	}
}

func splitNSName(v string) (ns, name string) {
	parts := strings.SplitN(v, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "default", v
}

// nameUnnamed assigns generated names to peers that have none (discovered peers).
// staticPeers already carry names from config. The agent has already prepended static peers.
func nameUnnamed(peers []config.Peer, target string) []config.Peer {
	out := make([]config.Peer, len(peers))
	for i, p := range peers {
		if p.Name == "" {
			p.Name = fmt.Sprintf("%s-%d", target, i)
		}
		out[i] = p
	}
	return out
}
