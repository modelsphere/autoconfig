package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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
		wasmPlugin("ai-backend-router"), wasmPlugin("key-auth"),
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
		wasmPlugin("ai-backend-router"), wasmPlugin("key-auth"),
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
		wasmPlugin("ai-backend-router"), wasmPlugin("key-auth"),
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
		wasmPlugin("ai-backend-router"), wasmPlugin("key-auth"),
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
		wasmPlugin("ai-backend-router"), wasmPlugin("key-auth"), // no ai-statistics
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
		wasmPlugin("ai-backend-router"), wasmPlugin("key-auth"),
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
		wasmPlugin("ai-backend-router", existing), wasmPlugin("key-auth"),
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
