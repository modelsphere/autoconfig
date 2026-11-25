package sink

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// weightTotal is what every tier's effective weights are renormalised to sum to.
//
// The plugin treats weights as relative, so any total would route the same. A fixed total is
// for the humans and for decision-gen: it makes status.effectiveWeight readable as basis
// points, and it makes "ascend died, its 20% went to h100" visible as a number that changed
// rather than as a member that quietly vanished from the table.
const weightTotal = 10000

// Decision-table defaults. These are plugin-level behaviour, not per-model policy, so they are
// not CRD fields — the doc's reasoning for leaving out overflowOn applies to all of them: the
// plugin applies one setting to the whole table, so a per-fleet knob would be a lie.
var (
	// defaultRetryCodes: what counts as "this member is full / broken, try the next tier".
	defaultRetryCodes = []int{429, 502, 503, 504}
	// maxRequestBodyBytes: the router buffers the body to read `model` out of it.
	maxRequestBodyBytes = 16 * 1024 * 1024
)

// FleetMemberView is one member flattened for rendering: the CR's intent joined with what was
// discovered about the member's backing ModelRoute. Keeping the join in the controller and the
// arithmetic here is what makes the weight maths testable without a cluster.
type FleetMemberView struct {
	// Name is the fleetBackend id in the decision table.
	Name string
	// Path is the gateway path prefix the router rewrites to, e.g. "/glm-53-h100".
	Path string
	// Tier and Weight are the intent from trafficPolicy.
	Tier   int32
	Weight int32
	// Ready: the member has at least one ready fleetBackend. An unready member is left out of the
	// table entirely rather than shipped with weight 0 — a zero-weight entry is still a
	// legal overflow fleetTarget for the plugin, and overflowing into a member with no backends
	// turns a degraded fleet into a failing one.
	Ready bool
	// ReadyReplicas / MaxConcurrency feed status.totalCapacity.
	ReadyReplicas  int32
	MaxConcurrency int32
}

// EffectiveWeights renormalises each tier's weights over the members that are actually ready,
// and returns a map from member name to effective weight. Members that are not ready, or whose
// tier has no weight at all, get 0.
//
// Renormalising per tier, not globally: tiers are an ordered fallback chain, so moving weight
// across a tier boundary would change which hardware is "the mains" — exactly the thing the
// tier is there to pin down.
//
// Largest-remainder apportionment, so the surviving weights sum to exactly weightTotal; naive
// rounding drifts, and a tier that sums to 9998 is a tier that reads as "someone is missing".
func EffectiveWeights(members []FleetMemberView) map[string]int32 {
	out := make(map[string]int32, len(members))
	byTier := map[int32][]FleetMemberView{}
	for _, m := range members {
		out[m.Name] = 0
		if m.Ready && m.Weight > 0 {
			byTier[m.Tier] = append(byTier[m.Tier], m)
		}
	}
	for _, group := range byTier {
		var sum int64
		for _, m := range group {
			sum += int64(m.Weight)
		}
		if sum <= 0 {
			continue
		}
		// Floor each share, then hand the remaining units to the largest remainders.
		type rem struct {
			name string
			r    int64
		}
		var left int64 = weightTotal
		rems := make([]rem, 0, len(group))
		for _, m := range group {
			num := int64(m.Weight) * weightTotal
			q := num / sum
			out[m.Name] = int32(q)
			left -= q
			rems = append(rems, rem{m.Name, num % sum})
		}
		// Ties broken by name so the output is stable: an unstable table rewrites the
		// WasmPlugin on every reconcile, and every rewrite is a config push to the gateway.
		sort.Slice(rems, func(i, j int) bool {
			if rems[i].r != rems[j].r {
				return rems[i].r > rems[j].r
			}
			return rems[i].name < rems[j].name
		})
		for i := 0; left > 0 && i < len(rems); i++ {
			out[rems[i].name]++
			left--
		}
	}
	return out
}

// DecisionTable is the ai-fleetBackend-router matchRule config. Rendered as a typed struct rather
// than a free map so the field names are checked at compile time: the plugin refuses a table
// with an unknown field outright, and it refuses it at the gateway — `kubectl apply` succeeds
// and the breakage surfaces later, as a config push that stops updating every plugin.
type DecisionTable struct {
	Decisions fleetDecisions `json:"decisions"`
	Limits    fleetLimits    `json:"limits"`
	Retry     fleetRetry     `json:"retry"`
}

type fleetDecisions struct {
	Static fleetStatic `json:"static"`
}

type fleetStatic struct {
	Models  map[string]fleetModelEntry `json:"models"`
	Version string                     `json:"version"`
}

type fleetModelEntry struct {
	Backends []fleetBackend `json:"backends"`
	Default  string         `json:"default"`
}

type fleetBackend struct {
	ID     string      `json:"id"`
	Target fleetTarget `json:"target"`
	Tier   int32       `json:"tier"`
	Weight int32       `json:"weight"`
}

type fleetTarget struct {
	Path string `json:"path"`
}

type fleetLimits struct {
	MaxRequestBodyBytes int `json:"maxRequestBodyBytes"`
}

type fleetRetry struct {
	Codes []int `json:"codes"`
	// Exclude: "tier" means a fleetRetry after one of Codes goes to a DIFFERENT tier, i.e. the
	// overflow behaviour. Retrying inside the same tier would just hit the next equally full
	// member of the same hardware.
	Exclude string `json:"exclude"`
}

// RenderDecisionTable builds the router's table for one fleet.
//
// defaultMember is required by the plugin and is emitted even when it is not ready: it is the
// landing spot for "nothing is selectable", and leaving it out would make that case a 500 at
// the gateway rather than a request to a degraded fleetBackend.
func RenderDecisionTable(modelName, defaultMember string, members []FleetMemberView, revision int64) (*DecisionTable, error) {
	if modelName == "" {
		return nil, fmt.Errorf("render decision table: empty model name")
	}
	eff := EffectiveWeights(members)
	byName := map[string]FleetMemberView{}
	for _, m := range members {
		byName[m.Name] = m
	}
	def, ok := byName[defaultMember]
	if !ok {
		return nil, fmt.Errorf("render decision table: defaultMember %q is not a member", defaultMember)
	}
	if def.Path == "" {
		return nil, fmt.Errorf("render decision table: defaultMember %q has no path", defaultMember)
	}

	backends := make([]fleetBackend, 0, len(members))
	for _, m := range members {
		w := eff[m.Name]
		// The default member is always in the table, even at weight 0, because the plugin
		// resolves `default` by id against this list.
		if w == 0 && m.Name != defaultMember {
			continue
		}
		if m.Path == "" {
			return nil, fmt.Errorf("render decision table: member %q has no path", m.Name)
		}
		backends = append(backends, fleetBackend{
			ID:     m.Name,
			Target: fleetTarget{Path: m.Path},
			Tier:   m.Tier,
			Weight: w,
		})
	}
	sort.Slice(backends, func(i, j int) bool {
		if backends[i].Tier != backends[j].Tier {
			return backends[i].Tier < backends[j].Tier
		}
		return backends[i].ID < backends[j].ID
	})

	t := &DecisionTable{
		Decisions: fleetDecisions{Static: fleetStatic{
			Models: map[string]fleetModelEntry{modelName: {Backends: backends, Default: defaultMember}},
		}},
		Limits: fleetLimits{MaxRequestBodyBytes: maxRequestBodyBytes},
		Retry:  fleetRetry{Codes: defaultRetryCodes, Exclude: "tier"},
	}
	// version is an audit label read off the live gateway: it ties a table back to the
	// decision that produced it, and it changes iff the table changes (the hash covers the
	// rendered backends), so a stale push is visible without diffing the whole config.
	t.Decisions.Static.Version = fmt.Sprintf("rev%d-%s", revision, hashOf(backends, defaultMember))
	return t, nil
}

func hashOf(v ...any) string {
	b, err := json.Marshal(v)
	if err != nil {
		// json.Marshal on these types cannot fail; fall back rather than panic in a controller.
		b = []byte(fmt.Sprint(v...))
	}
	return fmt.Sprintf("%x", sha256.Sum256(b))[:12]
}

// ToMap converts a rendered config to the generic map the WasmPlugin CR stores, which is an
// unstructured blob (x-kubernetes-preserve-unknown-fields).
func ToMap(v any) (map[string]any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// KeyAuthAllow is the key-auth matchRule config: which consumers may use this route.
// The consumer list and its credentials stay in the gateway chart's defaultConfig — this is
// only the allow-list, so no key passes through the CR or this renderer.
type KeyAuthAllow struct {
	Allow []string `json:"allow"`
}

// RenderKeyAuth builds the allow-list for one fleet's route.
func RenderKeyAuth(consumers []string) *KeyAuthAllow {
	out := append([]string(nil), consumers...)
	sort.Strings(out)
	return &KeyAuthAllow{Allow: out}
}

// AIProxy is the ai-proxy matchRule config: the credential the gateway presents to openresty.
type AIProxy struct {
	Provider fleetProvider `json:"provider"`
}

type fleetProvider struct {
	Type      string   `json:"type"`
	APITokens []string `json:"apiTokens"`
}

// RenderAIProxy builds the upstream-auth config. The token comes from a Secret read by the
// controller, never from the CR.
func RenderAIProxy(apiToken string) (*AIProxy, error) {
	if strings.TrimSpace(apiToken) == "" {
		return nil, fmt.Errorf("render ai-proxy: empty upstream api token")
	}
	return &AIProxy{Provider: fleetProvider{Type: "openai", APITokens: []string{apiToken}}}, nil
}

// AIStatistics is the ai-statistics matchRule config: what to put in the access log.
type AIStatistics struct {
	Attributes []fleetStatAttr `json:"attributes"`
}

type fleetStatAttr struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	ValueSource string `json:"value_source"`
	ApplyToLog  bool   `json:"apply_to_log"`
}

// RenderAIStatistics builds the token-accounting config for one fleet's route.
func RenderAIStatistics() *AIStatistics {
	return &AIStatistics{Attributes: []fleetStatAttr{{
		Key:         "requested_model",
		Value:       "model",
		ValueSource: "request_body",
		ApplyToLog:  true,
	}}}
}
