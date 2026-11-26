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
	// Byte for byte what the chart renders today, including the Bearer prefix on the
	// credential - the plugin compares the whole Authorization header value.
	const want = `[{"name":"tenant-a","credentials":["Bearer sk-a"],"keys":["Authorization"],` +
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
	if got[0].Credentials[0] != "Bearer sk-1" || got[0].Credentials[1] != "Bearer sk-2" {
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

// key_auth compares the ENTIRE Authorization header value against its credentials set, so a
// consumer configured with a bare key refuses every standard client - all of which send
// `Authorization: Bearer <key>` - while the object that configured it reports success. The
// Secret holds the key, so the prefix is added here.
func TestRenderConsumersAddsTheBearerPrefix(t *testing.T) {
	out, err := RenderConsumers([]ConsumerView{
		{Name: "a", Credentials: []string{"sk-plain"}, Source: "llm-route/a"},
		// One that already carries it must not get a second one.
		{Name: "b", Credentials: []string{"Bearer sk-already"}, Source: "llm-route/b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := out[0].Credentials[0]; got != "Bearer sk-plain" {
		t.Fatalf("credential = %q, want the Bearer prefix added", got)
	}
	if got := out[1].Credentials[0]; got != "Bearer sk-already" {
		t.Fatalf("credential = %q, want the existing prefix left alone", got)
	}
}

// Two consumers sharing a key is not "one of them wins": key_auth logs `duplicate consumer
// credential`, returns false, and the whole defaultConfig fails to parse - with the plugin
// FAIL_CLOSE, that denies every caller on every route of that gateway.
func TestRenderConsumersRefusesASharedCredential(t *testing.T) {
	_, err := RenderConsumers([]ConsumerView{
		{Name: "a", Credentials: []string{"sk-shared"}, Source: "llm-route/a"},
		{Name: "b", Credentials: []string{"sk-shared"}, Source: "llm-route/b"},
	})
	if err == nil {
		t.Fatal("rendered a configuration that key-auth refuses to parse at all")
	}
	// The failure names both consumers and what it would have caused...
	for _, want := range []string{`"b"`, "llm-route/a", "every caller on every route"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the error does not mention %s: %v", want, err)
		}
	}
	// ...but never the key itself. This text reaches a CR, the logs, and whatever ships them.
	if strings.Contains(err.Error(), "sk-shared") {
		t.Fatalf("the error leaks the credential: %v", err)
	}
	// The same key twice under ONE consumer is the same parse failure.
	if _, err := RenderConsumers([]ConsumerView{
		{Name: "a", Credentials: []string{"sk-dup", "sk-dup"}, Source: "llm-route/a"},
	}); err == nil {
		t.Fatal("accepted the same credential twice in one consumer")
	}
	// A bare key and the same key with the prefix collapse to one value, so they collide too
	// - and silently, if the prefix were added after the duplicate check rather than before.
	if _, err := RenderConsumers([]ConsumerView{
		{Name: "a", Credentials: []string{"sk-x"}, Source: "llm-route/a"},
		{Name: "b", Credentials: []string{"Bearer sk-x"}, Source: "llm-route/b"},
	}); err == nil {
		t.Fatal("accepted the same key written two ways under two consumers")
	}
}
