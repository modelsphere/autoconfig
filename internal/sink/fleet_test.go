package sink

import (
	"encoding/json"
	"testing"
)

func mem(name string, tier, weight int32, ready bool) FleetMemberView {
	return FleetMemberView{Name: name, Path: "/" + name, Tier: tier, Weight: weight, Ready: ready}
}

// Every tier's effective weights must sum to exactly weightTotal, so a reader can treat them as
// basis points and so a dead member's share is visibly redistributed rather than lost.
func TestEffectiveWeightsRenormalisePerTier(t *testing.T) {
	got := EffectiveWeights([]FleetMemberView{
		mem("h100", 0, 8000, true),
		mem("ascend", 0, 2000, true),
		mem("rtx6000d", 1, 10000, true),
	})
	if got["h100"] != 8000 || got["ascend"] != 2000 {
		t.Fatalf("tier 0 split changed: %v", got)
	}
	// A tier with a single member gets the whole total, whatever its raw weight was.
	if got["rtx6000d"] != weightTotal {
		t.Fatalf("tier 1 sole member = %d, want %d", got["rtx6000d"], weightTotal)
	}
}

func TestEffectiveWeightsRedistributeWhenMemberUnready(t *testing.T) {
	got := EffectiveWeights([]FleetMemberView{
		mem("h100", 0, 8000, true),
		mem("ascend", 0, 2000, false), // dead
		mem("rtx6000d", 1, 10000, true),
	})
	if got["ascend"] != 0 {
		t.Fatalf("unready member kept weight %d", got["ascend"])
	}
	// h100 is now alone in tier 0: it must absorb ascend's share, not keep 8000 and leak 2000.
	if got["h100"] != weightTotal {
		t.Fatalf("h100 = %d, want %d (ascend's share redistributed)", got["h100"], weightTotal)
	}
}

// Thirds do not divide evenly; largest-remainder must still land on the exact total.
func TestEffectiveWeightsSumExactly(t *testing.T) {
	got := EffectiveWeights([]FleetMemberView{
		mem("a", 0, 1, true), mem("b", 0, 1, true), mem("c", 0, 1, true),
	})
	sum := got["a"] + got["b"] + got["c"]
	if sum != weightTotal {
		t.Fatalf("sum = %d, want %d (%v)", sum, weightTotal, got)
	}
}

// Zero weight means "take me out", and that must not be confused with "unset".
func TestEffectiveWeightsZeroIsDrained(t *testing.T) {
	got := EffectiveWeights([]FleetMemberView{
		mem("h100", 0, 10000, true), mem("ascend", 0, 0, true),
	})
	if got["ascend"] != 0 || got["h100"] != weightTotal {
		t.Fatalf("zero-weight member not drained: %v", got)
	}
}

// With no policy at all every weight is 0, so the table carries only the default member. This
// is the safety property behind "a fleet with no decision yet does not spray traffic".
func TestRenderDecisionTableNoPolicyKeepsOnlyDefault(t *testing.T) {
	tbl, err := RenderDecisionTable("glm-5.3", "h100", []FleetMemberView{
		mem("h100", 0, 0, true), mem("ascend", 0, 0, true),
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	be := tbl.Decisions.Static.Models["glm-5.3"].Backends
	if len(be) != 1 || be[0].ID != "h100" {
		t.Fatalf("backends = %+v, want only the default member", be)
	}
}

// The default member stays in the table even when it is unready: the plugin resolves `default`
// by id against this list, and dropping it turns "nothing selectable" into a gateway error.
func TestRenderDecisionTableKeepsUnreadyDefault(t *testing.T) {
	tbl, err := RenderDecisionTable("glm-5.3", "h100", []FleetMemberView{
		mem("h100", 0, 8000, false), mem("ascend", 0, 2000, true),
	}, 7)
	if err != nil {
		t.Fatal(err)
	}
	m := tbl.Decisions.Static.Models["glm-5.3"]
	if m.Default != "h100" {
		t.Fatalf("default = %q", m.Default)
	}
	var sawH100 bool
	for _, b := range m.Backends {
		if b.ID == "h100" {
			sawH100 = true
			if b.Weight != 0 {
				t.Fatalf("unready default has weight %d, want 0", b.Weight)
			}
		}
	}
	if !sawH100 {
		t.Fatal("default member dropped from the table")
	}
}

// An unready non-default member must be absent, not present at weight 0: a zero-weight entry is
// still a legal overflow target, and overflowing into a member with no backends turns a
// degraded fleet into a failing one.
func TestRenderDecisionTableDropsUnreadyMember(t *testing.T) {
	tbl, _ := RenderDecisionTable("glm-5.3", "h100", []FleetMemberView{
		mem("h100", 0, 8000, true), mem("ascend", 1, 2000, false),
	}, 0)
	for _, b := range tbl.Decisions.Static.Models["glm-5.3"].Backends {
		if b.ID == "ascend" {
			t.Fatal("unready member present in the table")
		}
	}
}

func TestRenderDecisionTableRejectsUnknownDefault(t *testing.T) {
	if _, err := RenderDecisionTable("glm-5.3", "nope", []FleetMemberView{mem("h100", 0, 1, true)}, 0); err == nil {
		t.Fatal("want error for a defaultMember that is not a member")
	}
}

// The rendered JSON must match the shape the plugin accepts, field for field: it validates at
// the gateway, not at the API server, so a typo here is a config push that stops updating
// every plugin on that gateway rather than a rejected apply.
func TestRenderDecisionTableWireShape(t *testing.T) {
	tbl, err := RenderDecisionTable("qwen-trial", "qwen-trial", []FleetMemberView{
		{Name: "qwen-trial", Path: "/qwen-trial", Tier: 0, Weight: 100, Ready: true},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(tbl)
	var got map[string]any
	_ = json.Unmarshal(b, &got)

	models := got["decisions"].(map[string]any)["static"].(map[string]any)["models"].(map[string]any)
	entry := models["qwen-trial"].(map[string]any)
	backend := entry["backends"].([]any)[0].(map[string]any)
	if backend["id"] != "qwen-trial" {
		t.Fatalf("backend id: %v", backend["id"])
	}
	if backend["target"].(map[string]any)["path"] != "/qwen-trial" {
		t.Fatalf("target.path: %v", backend["target"])
	}
	if entry["default"] != "qwen-trial" {
		t.Fatalf("default: %v", entry["default"])
	}
	if _, ok := got["limits"].(map[string]any)["maxRequestBodyBytes"]; !ok {
		t.Fatal("limits.maxRequestBodyBytes missing")
	}
	if got["retry"].(map[string]any)["exclude"] != "tier" {
		t.Fatalf("retry.exclude: %v", got["retry"])
	}
}

// Identical input must render byte-identical output, or every resync rewrites the WasmPlugin
// and every rewrite is a config push across the data plane.
func TestRenderDecisionTableStable(t *testing.T) {
	in := []FleetMemberView{mem("b", 0, 1, true), mem("a", 0, 1, true), mem("c", 1, 5, true)}
	a, _ := RenderDecisionTable("m", "a", in, 3)
	b, _ := RenderDecisionTable("m", "a", in, 3)
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	if string(ab) != string(bb) {
		t.Fatalf("unstable render:\n%s\n%s", ab, bb)
	}
}

// ...and a changed table must change the version string, so a stale push is visible on the
// live gateway without diffing the whole config.
func TestDecisionTableVersionTracksContent(t *testing.T) {
	a, _ := RenderDecisionTable("m", "a", []FleetMemberView{mem("a", 0, 1, true)}, 1)
	b, _ := RenderDecisionTable("m", "a", []FleetMemberView{mem("a", 0, 2, true), mem("x", 1, 1, true)}, 1)
	if a.Decisions.Static.Version == b.Decisions.Static.Version {
		t.Fatal("version did not change with the table")
	}
}

func TestRenderAIProxyRejectsEmptyToken(t *testing.T) {
	if _, err := RenderAIProxy("  "); err == nil {
		t.Fatal("want error for an empty upstream token")
	}
}

func TestRenderKeyAuthSorts(t *testing.T) {
	got := RenderKeyAuth([]string{"ops", "tenant-a"})
	if got.Allow[0] != "ops" || got.Allow[1] != "tenant-a" {
		t.Fatalf("allow = %v", got.Allow)
	}
}
