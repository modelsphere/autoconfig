package sink

import (
	"encoding/json"
	"strings"
	"testing"
)

func view(name string, creds ...string) ConsumerView {
	return ConsumerView{Name: name, Credentials: creds, Source: "llm-route/" + name}
}

// The shape is key-auth's, and it is validated at the gateway rather than by the API server:
// a renamed field applies cleanly and then fails when the plugin loads, which stalls the
// config push for every plugin on that gateway. Checked field for field against what the
// chart renders today.
func TestRenderConsumersWireShape(t *testing.T) {
	got, err := RenderConsumers([]ConsumerView{view("tenant-a", "sk-a")})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	const want = `[{"name":"tenant-a","credentials":["sk-a"],"keys":["Authorization"],` +
		`"in_header":true,"in_query":false}]`
	if string(b) != want {
		t.Fatalf("rendered %s\nwant     %s", b, want)
	}
}

// Stable output, or every resync rewrites the plugin and every rewrite is an ECDS push of
// key-auth to the whole data plane.
func TestRenderConsumersIsSorted(t *testing.T) {
	got, err := RenderConsumers([]ConsumerView{
		view("tenant-b", "sk-b"), view("ops", "sk-2", "sk-1"), view("tenant-a", "sk-a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Name != "ops" || got[1].Name != "tenant-a" || got[2].Name != "tenant-b" {
		t.Fatalf("order = %s/%s/%s", got[0].Name, got[1].Name, got[2].Name)
	}
	// ...credentials too: a Secret whose keys are read in map order would otherwise churn.
	if got[0].Credentials[0] != "sk-1" || got[0].Credentials[1] != "sk-2" {
		t.Fatalf("credentials unsorted: %v", got[0].Credentials)
	}
}

// A consumer with no credentials is an error, not an entry with an empty list: the plugin
// accepts an empty credentials array and then matches nothing, so that tenant gets 401 on
// every request while the CR reports it was programmed.
func TestRenderConsumersRejectsAnEmptyCredentialList(t *testing.T) {
	if _, err := RenderConsumers([]ConsumerView{view("tenant-a")}); err == nil {
		t.Fatal("a consumer with no credentials was rendered")
	}
}

// Two objects claiming one name would each render an entry and the plugin would take one of
// them, with no way to tell which. The error has to name both, or the operator cannot find
// the other one.
func TestRenderConsumersRefusesDuplicateNames(t *testing.T) {
	_, err := RenderConsumers([]ConsumerView{
		{Name: "shared", Credentials: []string{"sk-a"}, Source: "llm-route/team-a"},
		{Name: "shared", Credentials: []string{"sk-b"}, Source: "llm-route/team-b"},
	})
	if err == nil {
		t.Fatal("two consumers were allowed to share a name")
	}
	if !strings.Contains(err.Error(), "team-a") || !strings.Contains(err.Error(), "team-b") {
		t.Fatalf("the error names only one claimant: %v", err)
	}
}

// Nothing to render is not an error here - whether an empty list may be WRITTEN is the
// controller's decision, and it depends on who owns the gateway's list.
func TestRenderConsumersEmptyInputIsEmptyOutput(t *testing.T) {
	got, err := RenderConsumers(nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
}
