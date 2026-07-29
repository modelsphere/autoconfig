// Package sink renders discovered peers into each consumer's config format
// (pure functions RenderCart / RenderRoute / ResolveSources, called by the controller).
package sink

import (
	"fmt"

	"autoconfig/internal/config"
)

// nameUnnamed assigns generated names ("<target>-N") to discovered peers that have none.
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
