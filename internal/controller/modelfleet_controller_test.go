package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	routingv1 "autoconfig/api/v1alpha1"
)

func fleetScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = routingv1.AddToScheme(s)
	// WasmPlugin is handled unstructured, so the fake client needs the GVK registered but not
	// a Go type — the same way the controller treats it.
	s.AddKnownTypeWithName(wasmPluginGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(wasmPluginGVK.GroupVersion().WithKind("WasmPluginList"), &unstructured.UnstructuredList{})
	return s
}

func wasmPlugin(name string, rules ...any) *unstructured.Unstructured {
	o := &unstructured.Unstructured{}
	o.SetGroupVersionKind(wasmPluginGVK)
	o.SetName(name)
	o.SetNamespace("higress-system")
	if rules == nil {
		rules = []any{}
	}
	_ = unstructured.SetNestedSlice(o.Object, rules, "spec", "matchRules")
	return o
}

func getPlugin(t *testing.T, c client.Client, name string) []any {
	t.Helper()
	o := &unstructured.Unstructured{}
	o.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "higress-system", Name: name}, o); err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	rules, _, _ := unstructured.NestedSlice(o.Object, "spec", "matchRules")
	return rules
}

func modelRoute(name, ns, route string, backends int, ready bool) *routingv1.ModelRoute {
	return &routingv1.ModelRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: routingv1.ModelRouteSpec{
			Discovery: routingv1.Discovery{Service: name + "-leader", Port: 8050},
			Nginx: routingv1.NginxSpec{
				Route: route, OutputConfigMap: ns + "/openresty-conf",
				Peers: []routingv1.RoutePeer{{Use: "backend"}},
			},
		},
		Status: routingv1.ModelRouteStatus{Backends: backends, Ready: ready},
	}
}

func fleetFixture() *routingv1.ModelFleet {
	return &routingv1.ModelFleet{
		ObjectMeta: metav1.ObjectMeta{Name: "glm-5.3", Namespace: "llm-route", Generation: 3,
			Finalizers: []string{fleetFinalizer}},
		Spec: routingv1.ModelFleetSpec{
			ModelName:     "glm-5.3",
			DefaultMember: "h100",
			Gateway: routingv1.GatewaySpec{
				ExternalURL:   "http://172.28.44.17:31080",
				AuthConsumers: []string{"ops"},
				// Opt out by default so each test states its own intent about the upstream
				// credential; the tests that care about it set this themselves.
				UpstreamAuthSecretRef: "none",
			},
			Members: []routingv1.FleetMember{
				{Name: "h100", Hardware: routingv1.Hardware{Vendor: "nvidia", Model: "H100", Replicas: 2},
					Backend:      routingv1.MemberBackend{ModelRouteRef: "glm-53-h100"},
					Capabilities: &routingv1.Capabilities{MaxConcurrency: 16}},
				{Name: "ascend", Hardware: routingv1.Hardware{Vendor: "huawei", Model: "910B4", Replicas: 1},
					Backend:      routingv1.MemberBackend{ModelRouteRef: "glm-53-ascend"},
					Capabilities: &routingv1.Capabilities{MaxConcurrency: 8}},
			},
			TrafficPolicy: &routingv1.TrafficPolicy{
				Revision: 42,
				Members: []routingv1.PolicyMember{
					{Name: "h100", Tier: 0, Weight: 8000},
					{Name: "ascend", Tier: 0, Weight: 2000},
				},
			},
		},
	}
}

func newFleetReconciler(t *testing.T, objs ...client.Object) (*ModelFleetReconciler, client.Client) {
	t.Helper()
	s := fleetScheme(t)
	c := ctrlfake.NewClientBuilder().WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&routingv1.ModelFleet{}).
		Build()
	return &ModelFleetReconciler{Client: c, Scheme: s}, c
}

func reconcileFleet(t *testing.T, r *ModelFleetReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

// End to end on a fake cluster: both members ready → one Ingress matching the model header,
// a decision table carrying both backends at their intended split, and a status the console
// can render without reading anything else.
func TestFleetReconcileHappyPath(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPlugin("ai-statistics"),
	)
	reconcileFleet(t, r)

	var ing networkingv1.Ingress
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err != nil {
		t.Fatalf("ingress: %v", err)
	}
	if got := ing.Annotations["higress.io/exact-match-header-"+modelHeader]; got != "glm-5.3" {
		t.Fatalf("model header match = %q", got)
	}
	// No rewrite-path: the path is the router plugin's to pick per member. An Ingress-level
	// rewrite would pin every member to one path and silently defeat the split.
	if _, ok := ing.Annotations["higress.io/rewrite-path"]; ok {
		t.Fatal("ingress must not set rewrite-path")
	}

	rules := getPlugin(t, c, "ai-backend-router")
	if len(rules) != 1 {
		t.Fatalf("match rules = %d, want 1", len(rules))
	}
	rule := rules[0].(map[string]any)
	ings, _, _ := unstructured.NestedStringSlice(rule, "ingress")
	if len(ings) != 1 || ings[0] != "llm-route/llm-fleet-glm-5.3" {
		t.Fatalf("rule bound to %v", ings)
	}
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	if len(backends) != 2 {
		t.Fatalf("backends = %d, want 2", len(backends))
	}

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if fleet.Status.ObservedGeneration != 3 {
		t.Fatalf("observedGeneration = %d, want 3", fleet.Status.ObservedGeneration)
	}
	if fleet.Status.Endpoint != "http://172.28.44.17:31080/v1" {
		t.Fatalf("endpoint = %q", fleet.Status.Endpoint)
	}
	if fleet.Status.TotalCapacity.MaxConcurrency != 24 || fleet.Status.TotalCapacity.ReadyReplicas != 3 {
		t.Fatalf("capacity = %+v, want 24/3", fleet.Status.TotalCapacity)
	}
	for _, m := range fleet.Status.Members {
		if m.Phase != routingv1.MemberReady {
			t.Fatalf("member %s phase %s", m.Name, m.Phase)
		}
		if m.Path == "" {
			t.Fatalf("member %s has no path", m.Name)
		}
	}
	if !condTrue(fleet.Status.Conditions, condReady) || !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("conditions = %+v", fleet.Status.Conditions)
	}
}

// A member whose backends all went away must drop out of the table and have its share
// redistributed, and the fleet must stay Ready on the survivor rather than going dark.
func TestFleetReconcileMemberUnready(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 0, false),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	if len(backends) != 1 {
		t.Fatalf("backends = %d, want only the ready member", len(backends))
	}

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	byName := map[string]routingv1.MemberStatus{}
	for _, m := range fleet.Status.Members {
		byName[m.Name] = m
	}
	if byName["ascend"].Phase != routingv1.MemberDegraded {
		t.Fatalf("ascend phase = %s, want Degraded", byName["ascend"].Phase)
	}
	// h100 absorbs ascend's 2000: decision-gen reads this, not its own last write.
	if byName["h100"].EffectiveWeight != 10000 {
		t.Fatalf("h100 effectiveWeight = %d, want 10000", byName["h100"].EffectiveWeight)
	}
	if byName["ascend"].EffectiveWeight != 0 {
		t.Fatalf("ascend effectiveWeight = %d, want 0", byName["ascend"].EffectiveWeight)
	}
	if fleet.Status.TotalCapacity.MaxConcurrency != 16 {
		t.Fatalf("capacity counts a degraded member: %+v", fleet.Status.TotalCapacity)
	}
	if !condTrue(fleet.Status.Conditions, condReady) {
		t.Fatal("fleet went un-Ready although one member still serves")
	}
}

// Rules written by anyone else (the gateway chart's own routes) must survive our edit:
// matchRules is an atomic array, so a careless writer replaces the whole list.
func TestFleetReconcilePreservesForeignMatchRules(t *testing.T) {
	foreign := map[string]any{
		"ingress":       []any{"llm-route/llm-model-qwen-trial"},
		"configDisable": false,
		"config":        map[string]any{"decisions": map[string]any{}},
	}
	r, c := newFleetReconciler(t,
		fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router", foreign),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rules := getPlugin(t, c, "ai-backend-router")
	if len(rules) != 2 {
		t.Fatalf("rules = %d, want the foreign rule kept plus ours", len(rules))
	}
	var sawForeign bool
	for _, raw := range rules {
		ings, _, _ := unstructured.NestedStringSlice(raw.(map[string]any), "ingress")
		if len(ings) == 1 && ings[0] == "llm-route/llm-model-qwen-trial" {
			sawForeign = true
		}
	}
	if !sawForeign {
		t.Fatal("foreign match rule was dropped")
	}
}

// Reconciling twice must not rewrite the plugin: every write is a config push to the gateway,
// and a 30s resync that rewrites an identical object is 30s config churn on the data plane.
func TestFleetReconcileIdempotent(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"),
	)
	reconcileFleet(t, r)
	first := resourceVersionOf(t, c, "ai-backend-router")
	reconcileFleet(t, r)
	if second := resourceVersionOf(t, c, "ai-backend-router"); second != first {
		t.Fatalf("plugin rewritten on an unchanged reconcile: %s -> %s", first, second)
	}
}

// A policy entry naming a member the console already deleted is reported, never rejected:
// refusing it would deadlock the delete against decision-gen's stale policy.
func TestFleetReconcileOrphanPolicyMemberIsReportedNotFatal(t *testing.T) {
	f := fleetFixture()
	f.Spec.TrafficPolicy.Members = append(f.Spec.TrafficPolicy.Members,
		routingv1.PolicyMember{Name: "gone", Tier: 1, Weight: 5000})
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condPolicyApplied) {
		t.Fatal("orphan reference not reported on PolicyApplied")
	}
	// ...and the rest of the policy still went out.
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("orphan reference blocked the gateway sync")
	}
}

// A member pointing at a ModelRoute that does not exist is NotFound, and the fleet still
// serves on the members that do exist.
func TestFleetReconcileMissingModelRoute(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	for _, m := range fleet.Status.Members {
		if m.Name == "ascend" && m.Phase != routingv1.MemberNotFound {
			t.Fatalf("ascend phase = %s, want NotFound", m.Phase)
		}
	}
	if !condTrue(fleet.Status.Conditions, condReady) {
		t.Fatal("fleet un-Ready although h100 serves")
	}
}

// Deleting the fleet must withdraw its rules from every plugin: they live in another
// namespace, in objects we do not own, so nothing garbage-collects them for us.
func TestFleetDeleteWithdrawsMatchRules(t *testing.T) {
	f := fleetFixture()
	now := metav1.Now()
	f.DeletionTimestamp = &now
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		wasmPlugin("ai-backend-router", map[string]any{
			"ingress":       []any{"llm-route/llm-fleet-glm-5.3"},
			"configDisable": false,
			"config":        map[string]any{},
		}),
	)
	reconcileFleet(t, r)
	if rules := getPlugin(t, c, "ai-backend-router"); len(rules) != 0 {
		t.Fatalf("rules left behind after delete: %v", rules)
	}
}

// The upstream credential comes from a Secret and must never need to appear in the CR.
func TestFleetUpstreamKeyFromSecret(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.UpstreamAuthSecretRef = "openresty-keys:gateway"
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), wasmPlugin("ai-proxy"),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "openresty-keys", Namespace: "llm-route"},
			Data:       map[string][]byte{"gateway": []byte("sk-test-key\n")},
		},
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-proxy")[0].(map[string]any)
	tokens, _, _ := unstructured.NestedStringSlice(rule, "config", "provider", "apiTokens")
	if len(tokens) != 1 || tokens[0] != "sk-test-key" {
		t.Fatalf("apiTokens = %v (whitespace must be trimmed)", tokens)
	}
}

// A plugin this cluster does not use, that we had nothing to write to anyway, must not stop
// the rest from being programmed.
func TestFleetMissingPluginWithNothingToWriteIsNotFatal(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.AuthConsumers = nil // nothing to put in an allow-list
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), // no key-auth / ai-proxy / ai-statistics here
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("missing unused plugin failed the sync: %+v", fleet.Status.Conditions)
	}
}

// ...but a fleet that ASKED for an allow-list and found no key-auth must fail the sync.
// Skipping it silently would publish an unauthenticated route, which is the one outcome
// nobody would notice until it mattered.
func TestFleetRequestedAuthWithoutKeyAuthPluginFailsSync(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(), // AuthConsumers: [ops]
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), // key-auth absent
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("authConsumers requested but key-auth missing, and the sync reported success")
	}
	if condTrue(fleet.Status.Conditions, condReady) {
		t.Fatal("fleet Ready with an allow-list that was never programmed")
	}
}

// ai-statistics is the one plugin whose absence is only a loss of metrics, so it stays
// tolerated even though we had a rule to write.
func TestFleetMissingStatisticsPluginIsNotFatal(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}), // no ai-statistics
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("missing ai-statistics failed the sync: %+v", fleet.Status.Conditions)
	}
}

// The router itself is NOT optional: without its table nothing can be split, so that must
// surface as a failed sync rather than as a fleet that quietly serves one member.
func TestFleetMissingRouterPluginFailsSync(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("sync reported true with no ai-backend-router present")
	}
	if condTrue(fleet.Status.Conditions, condReady) {
		t.Fatal("fleet Ready with an unprogrammed gateway")
	}
}

func condTrue(conds []metav1.Condition, t string) bool {
	for _, c := range conds {
		if c.Type == t {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}

func resourceVersionOf(t *testing.T, c client.Client, name string) string {
	t.Helper()
	o := &unstructured.Unstructured{}
	o.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "higress-system", Name: name}, o); err != nil {
		t.Fatal(err)
	}
	return o.GetResourceVersion()
}

// A plugin that exists with no match rules at all, and a fleet with nothing to write to it,
// must not be written. nil and [] are the same empty list; marshalling them differently made
// every 30s resync push an identical config to the gateway.
func TestFleetUnchangedEmptyPluginIsNotRewritten(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.AuthConsumers = nil // nothing to write into key-auth
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)
	first := resourceVersionOf(t, c, "key-auth")
	reconcileFleet(t, r)
	if second := resourceVersionOf(t, c, "key-auth"); second != first {
		t.Fatalf("empty plugin rewritten with nothing to write: %s -> %s", first, second)
	}
}

// If the default member's ModelRoute is gone there is no safe table to ship, so the existing
// one is left alone and the failure is reported. Overwriting it would leave the fleet with no
// landing spot for a fallback; picking another default silently would move those requests to
// hardware nobody chose.
func TestFleetMissingDefaultMemberRouteFreezesTableAndReports(t *testing.T) {
	f := fleetFixture() // defaultMember: h100
	existing := map[string]any{
		"ingress":       []any{"llm-route/llm-fleet-glm-5.3"},
		"configDisable": false,
		"config":        map[string]any{"marker": "last-good"},
	}
	r, c := newFleetReconciler(t, f,
		// h100's ModelRoute is absent; ascend's is healthy.
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router", existing), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rules := getPlugin(t, c, "ai-backend-router")
	if len(rules) != 1 {
		t.Fatalf("rules = %d, want the previous one kept", len(rules))
	}
	marker, _, _ := unstructured.NestedString(rules[0].(map[string]any), "config", "marker")
	if marker != "last-good" {
		t.Fatalf("the last good table was overwritten: %v", rules[0])
	}

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("a frozen table was reported as synced")
	}
	var msg string
	for _, cd := range fleet.Status.Conditions {
		if cd.Type == condGatewaySynced {
			msg = cd.Message
		}
	}
	if !strings.Contains(msg, "frozen") || !strings.Contains(msg, "h100") {
		t.Fatalf("GatewaySynced message does not say what froze or why: %q", msg)
	}
}

// keyAuthPlugin builds a key-auth WasmPlugin whose defaultConfig defines consumers, the way
// the gateway chart renders it.
func keyAuthPlugin(consumers map[string]string) *unstructured.Unstructured {
	o := wasmPlugin("key-auth")
	var list []any
	for name, cred := range consumers {
		list = append(list, map[string]any{
			"name":        name,
			"credentials": []any{"Bearer " + cred},
		})
	}
	_ = unstructured.SetNestedSlice(o.Object, list, "spec", "defaultConfig", "consumers")
	return o
}

// The route must not be published before the rules that guard it. Writing the Ingress first
// means a failure part way through the plugins leaves a live, unauthenticated route behind.
func TestFleetIngressIsNotPublishedWhenPluginRulesFail(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(), // asks for authConsumers: [ops]
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), // key-auth absent -> writing its rule must fail
	)
	reconcileFleet(t, r)

	var ing networkingv1.Ingress
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing)
	if err == nil {
		t.Fatal("the route was published although its key-auth rule could not be written")
	}
}

// An allow-list naming a consumer key-auth does not define would publish a route that denies
// everyone with 403 while the CR claims a healthy sync. Catch it before publishing anything.
func TestFleetUnknownAuthConsumerIsRefusedBeforePublishing(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.AuthConsumers = []string{"opss"} // typo
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("an allow-list naming an undefined consumer reported a healthy sync")
	}
	var msg string
	for _, cd := range fleet.Status.Conditions {
		if cd.Type == condGatewaySynced {
			msg = cd.Message
		}
	}
	if !strings.Contains(msg, "opss") {
		t.Fatalf("the error does not name the offending consumer: %q", msg)
	}
	var ing networkingv1.Ingress
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err == nil {
		t.Fatal("the route was published despite an unusable allow-list")
	}
}

// A known consumer passes, and the route is published.
func TestFleetKnownAuthConsumerPasses(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(), // authConsumers: [ops]
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"),
		keyAuthPlugin(map[string]string{"ops": "sk-ops", "tenant-a": "sk-a"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("a valid allow-list failed the sync: %+v", fleet.Status.Conditions)
	}
	var ing networkingv1.Ingress
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err != nil {
		t.Fatalf("route not published: %v", err)
	}
}

// The credential the gateway presents to openresty must not also be a caller credential at
// the gateway: one leak would then open both doors.
func TestFleetUpstreamKeyMustNotBeAConsumerCredential(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.UpstreamAuthSecretRef = "keys:gateway"
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), wasmPlugin("ai-proxy"),
		keyAuthPlugin(map[string]string{"ops": "sk-shared"}),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "keys", Namespace: "llm-route"},
			Data:       map[string][]byte{"gateway": []byte("sk-shared")},
		},
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("an upstream key that doubles as a consumer credential was accepted")
	}
}

// The upstream credential defaults to where that key already lives on these clusters, so a
// fleet does not have to restate one cluster-wide fact.
func TestFleetUpstreamKeyDefaultsToOpenrestySecret(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.UpstreamAuthSecretRef = "" // not stated
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), wasmPlugin("ai-proxy"),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "openresty-api-keys", Namespace: "llm-route"},
			Data:       map[string][]byte{"keys": []byte("sk-default\n")},
		},
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-proxy")[0].(map[string]any)
	tokens, _, _ := unstructured.NestedStringSlice(rule, "config", "provider", "apiTokens")
	if len(tokens) != 1 || tokens[0] != "sk-default" {
		t.Fatalf("apiTokens = %v, want the default secret's key", tokens)
	}
}

// With no Secret to default to, the route must not be published: openresty would answer 401
// on the first real request, long after this reconcile looked healthy.
func TestFleetMissingDefaultUpstreamSecretFailsSync(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.UpstreamAuthSecretRef = ""
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), wasmPlugin("ai-proxy"),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		// no openresty-api-keys Secret
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("no upstream credential available, yet the sync reported success")
	}
	var msg string
	for _, cd := range fleet.Status.Conditions {
		if cd.Type == condGatewaySynced {
			msg = cd.Message
		}
	}
	// The message has to say how to get out of it, or the only way forward is reading source.
	if !strings.Contains(msg, "none") {
		t.Fatalf("the error does not mention the opt-out: %q", msg)
	}
	var ing networkingv1.Ingress
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err == nil {
		t.Fatal("route published without the credential it needs")
	}
}

// "none" is the explicit opt-out for an openresty that asks for no key.
func TestFleetUpstreamAuthNoneSkipsAIProxy(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.UpstreamAuthSecretRef = "none"
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), wasmPlugin("ai-proxy"),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("explicit opt-out failed the sync: %+v", fleet.Status.Conditions)
	}
	if rules := getPlugin(t, c, "ai-proxy"); len(rules) != 0 {
		t.Fatalf("ai-proxy rule written despite the opt-out: %v", rules)
	}
}

func rawJSON(t *testing.T, v any) apiextensionsv1.JSON {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return apiextensionsv1.JSON{Raw: b}
}

// A plugin this controller knows nothing about is bound to the fleet's route with its config
// passed through unchanged - that is the whole point: adding a plugin should not need a new
// field, a new renderer and a new release.
func TestFleetExtraPluginIsPassedThroughVerbatim(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.ExtraPlugins = map[string]apiextensionsv1.JSON{
		"cluster-key-rate-limit": rawJSON(t, map[string]any{
			"rule_name":       "glm-53",
			"limit_by_header": "x-mse-consumer",
			"limit_keys":      []any{map[string]any{"key": "tenant-a", "query_per_minute": 600}},
		}),
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPlugin("cluster-key-rate-limit"),
	)
	reconcileFleet(t, r)

	rules := getPlugin(t, c, "cluster-key-rate-limit")
	if len(rules) != 1 {
		t.Fatalf("rules = %d, want 1", len(rules))
	}
	rule := rules[0].(map[string]any)
	ings, _, _ := unstructured.NestedStringSlice(rule, "ingress")
	if len(ings) != 1 || ings[0] != "llm-route/llm-fleet-glm-5.3" {
		t.Fatalf("rule bound to %v", ings)
	}
	name, _, _ := unstructured.NestedString(rule, "config", "rule_name")
	hdr, _, _ := unstructured.NestedString(rule, "config", "limit_by_header")
	if name != "glm-53" || hdr != "x-mse-consumer" {
		t.Fatalf("config was not passed through verbatim: %v", rule["config"])
	}
}

// The fleet asked for this config, so a plugin that is not deployed means the route would go
// live without something it was told to have.
func TestFleetMissingExtraPluginFailsSync(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.ExtraPlugins = map[string]apiextensionsv1.JSON{
		"cluster-key-rate-limit": rawJSON(t, map[string]any{"rule_name": "x"}),
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		// no cluster-key-rate-limit
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("a declared extra plugin was missing, yet the sync reported success")
	}
}

// Passing through a plugin the controller renders itself would put two writers on one rule.
// The CRD refuses it at apply time; this is the controller's own guard for an older cluster
// whose CRD has not been updated.
func TestFleetExtraPluginCannotOverrideARenderedOne(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.ExtraPlugins = map[string]apiextensionsv1.JSON{
		"ai-backend-router": rawJSON(t, map[string]any{"decisions": map[string]any{}}),
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("extraPlugins was allowed to override a rendered plugin")
	}
	// ...and the real table was still written, not replaced by the passthrough blob.
	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	if _, found, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends"); !found {
		t.Fatalf("the computed decision table was clobbered: %v", rule["config"])
	}
}

// Deleting the fleet has to withdraw the passthrough rules too: we wrote them, nothing else
// will take them back.
func TestFleetDeleteWithdrawsExtraPluginRules(t *testing.T) {
	f := fleetFixture()
	now := metav1.Now()
	f.DeletionTimestamp = &now
	f.Spec.Gateway.ExtraPlugins = map[string]apiextensionsv1.JSON{
		"cluster-key-rate-limit": rawJSON(t, map[string]any{"rule_name": "x"}),
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		wasmPlugin("cluster-key-rate-limit", map[string]any{
			"ingress":       []any{"llm-route/llm-fleet-glm-5.3"},
			"configDisable": false,
			"config":        map[string]any{"rule_name": "x"},
		}),
	)
	reconcileFleet(t, r)
	if rules := getPlugin(t, c, "cluster-key-rate-limit"); len(rules) != 0 {
		t.Fatalf("passthrough rule left behind after delete: %v", rules)
	}
}

// observedGeneration is the convergence signal console and decision-gen poll. Advancing it
// on a failed reconcile tells them the new split is live while nothing was written.
func TestFleetObservedGenerationOnlyAdvancesOnSuccess(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(), // wants an allow-list
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), // key-auth missing -> sync fails
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if fleet.Status.ObservedGeneration == fleet.Generation {
		t.Fatalf("observedGeneration = generation = %d after a failed sync", fleet.Generation)
	}
}

// An address that was removed must disappear from status, not linger: status.endpoint is the
// one address the console shows.
func TestFleetEndpointClearedWhenExternalURLRemoved(t *testing.T) {
	r, c := newFleetReconciler(t,
		fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	nn := types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}
	_ = c.Get(context.Background(), nn, &fleet)
	if fleet.Status.Endpoint == "" {
		t.Fatal("precondition: endpoint should have been set")
	}
	fleet.Spec.Gateway.ExternalURL = ""
	if err := c.Update(context.Background(), &fleet); err != nil {
		t.Fatal(err)
	}
	reconcileFleet(t, r)
	_ = c.Get(context.Background(), nn, &fleet)
	if fleet.Status.Endpoint != "" {
		t.Fatalf("stale endpoint kept: %q", fleet.Status.Endpoint)
	}
}

// An overridable plugin passed through must be written once, by the fleet - not written by
// the controller's default first and then overwritten, twice per resync forever.
func TestFleetOverriddenStatisticsIsWrittenOnce(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.ExtraPlugins = map[string]apiextensionsv1.JSON{
		"ai-statistics": rawJSON(t, map[string]any{"attributes": []any{
			map[string]any{"key": "mine", "value": "model", "value_source": "request_body", "apply_to_log": true},
		}}),
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPlugin("ai-statistics"),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-statistics")[0].(map[string]any)
	attrs, _, _ := unstructured.NestedSlice(rule, "config", "attributes")
	key, _, _ := unstructured.NestedString(attrs[0].(map[string]any), "key")
	if key != "mine" {
		t.Fatalf("the fleet's config did not win: %v", rule["config"])
	}
	// ...and a second reconcile writes nothing at all.
	before := resourceVersionOf(t, c, "ai-statistics")
	reconcileFleet(t, r)
	if after := resourceVersionOf(t, c, "ai-statistics"); after != before {
		t.Fatalf("overridden plugin rewritten on an unchanged reconcile: %s -> %s", before, after)
	}
}

// Taking a key out of extraPlugins has to withdraw the rule it wrote. Nothing else will: the
// reconciler only ever sees the spec as it is now.
func TestFleetRemovedExtraPluginRuleIsWithdrawn(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.ExtraPlugins = map[string]apiextensionsv1.JSON{
		"cluster-key-rate-limit": rawJSON(t, map[string]any{"rule_name": "x"}),
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPlugin("cluster-key-rate-limit"),
	)
	reconcileFleet(t, r)
	if n := len(getPlugin(t, c, "cluster-key-rate-limit")); n != 1 {
		t.Fatalf("precondition: rules = %d, want 1", n)
	}

	var fleet routingv1.ModelFleet
	nn := types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}
	_ = c.Get(context.Background(), nn, &fleet)
	if len(fleet.Status.AppliedExtraPlugins) != 1 {
		t.Fatalf("status did not record what was written: %v", fleet.Status.AppliedExtraPlugins)
	}
	fleet.Spec.Gateway.ExtraPlugins = nil
	if err := c.Update(context.Background(), &fleet); err != nil {
		t.Fatal(err)
	}
	reconcileFleet(t, r)

	if rules := getPlugin(t, c, "cluster-key-rate-limit"); len(rules) != 0 {
		t.Fatalf("rule still enforced on the live route after its key was removed: %v", rules)
	}
	_ = c.Get(context.Background(), nn, &fleet)
	if len(fleet.Status.AppliedExtraPlugins) != 0 {
		t.Fatalf("status still claims %v", fleet.Status.AppliedExtraPlugins)
	}
}

// `plugin: null` must be refused, not treated as "withdraw": an emptied entry would quietly
// take a plugin off a live route while reporting a healthy sync.
func TestFleetNullExtraPluginConfigIsRefused(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.ExtraPlugins = map[string]apiextensionsv1.JSON{
		"cluster-key-rate-limit": {Raw: []byte("null")},
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPlugin("cluster-key-rate-limit"),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("a null plugin config reported a healthy sync")
	}
}

// A cluster that never deployed ai-proxy and never named a Secret takes no key from the
// gateway; defaulting the credential must not turn that into a fleet that refuses to publish.
func TestFleetDefaultedCredentialToleratesMissingAIProxy(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.UpstreamAuthSecretRef = "" // defaulted
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "openresty-api-keys", Namespace: "llm-route"},
			Data:       map[string][]byte{"keys": []byte("sk-default")},
		},
		// no ai-proxy WasmPlugin
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("a defaulted credential with no ai-proxy failed the sync: %+v", fleet.Status.Conditions)
	}
}

// ...but naming a Secret is an instruction, so a missing ai-proxy then IS a failure.
func TestFleetExplicitCredentialRequiresAIProxy(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.UpstreamAuthSecretRef = "mine:gateway"
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mine", Namespace: "llm-route"},
			Data:       map[string][]byte{"gateway": []byte("sk-explicit")},
		},
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("an explicitly named credential was silently skipped when ai-proxy was absent")
	}
}
