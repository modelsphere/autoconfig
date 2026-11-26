package sink

import (
	"fmt"
	"sort"
)

// KeyAuthConsumer is one entry of key-auth's defaultConfig.consumers: a caller, the
// credentials it may present, and where in the request to look for them.
//
// The shape is the plugin's, not ours, and it is checked at the gateway rather than by the
// API server — a renamed field here applies cleanly and then fails when the plugin loads,
// which stalls the config push for every plugin on that gateway. It matches what the chart
// renders today, field for field.
type KeyAuthConsumer struct {
	Name        string   `json:"name"`
	Credentials []string `json:"credentials"`
	// Keys: which header or query parameter carries the credential. Per consumer, because
	// key-auth's global `keys` is a placeholder on these gateways (a dummy header) precisely
	// so that extraction is decided per consumer.
	Keys     []string `json:"keys"`
	InHeader bool     `json:"in_header"`
	InQuery  bool     `json:"in_query"`
}

// credentialHeader is where every consumer's credential is read from. Fixed rather than a CRD
// field: it is what the chart renders for every consumer today, and a per-consumer choice is
// a thing to add when something actually needs it, not before.
const credentialHeader = "Authorization"

// ConsumerView is one APIConsumer flattened for rendering: the name it claims and the keys
// read out of its Secret.
type ConsumerView struct {
	// Name as it appears in the gateway's consumer list and in a fleet's authConsumers.
	Name string
	// Credentials, in the order they should be written. Several means a rotation is in
	// flight and both the old and the new key work.
	Credentials []string
	// Source identifies the object this came from, for error messages that name the CR
	// rather than the consumer.
	Source string
}

// RenderConsumers builds key-auth's defaultConfig.consumers from every APIConsumer on one
// gateway.
//
// Sorted by name so the rendered list is stable: an unstable order rewrites the WasmPlugin on
// every resync, and every rewrite is an ECDS push of that plugin to the whole data plane.
//
// A consumer with no credentials is an error rather than an entry with an empty list: an
// empty credentials array is accepted by the plugin and then matches nothing, so that tenant
// gets 401 on every request while the CR says it was programmed.
func RenderConsumers(views []ConsumerView) ([]KeyAuthConsumer, error) {
	out := make([]KeyAuthConsumer, 0, len(views))
	seen := map[string]string{}
	for _, v := range views {
		if v.Name == "" {
			return nil, fmt.Errorf("%s: empty consumer name", v.Source)
		}
		if len(v.Credentials) == 0 {
			return nil, fmt.Errorf("%s: consumer %q has no credentials", v.Source, v.Name)
		}
		// Two objects claiming one consumer name would each render an entry, the plugin would
		// take one of them, and which one is not defined. Refusing names both objects, which
		// is the only way the operator can tell what happened.
		if prev, dup := seen[v.Name]; dup {
			return nil, fmt.Errorf("consumer name %q is claimed by both %s and %s", v.Name, prev, v.Source)
		}
		seen[v.Name] = v.Source
		creds := append([]string(nil), v.Credentials...)
		sort.Strings(creds)
		out = append(out, KeyAuthConsumer{
			Name:        v.Name,
			Credentials: creds,
			Keys:        []string{credentialHeader},
			InHeader:    true,
			InQuery:     false,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
