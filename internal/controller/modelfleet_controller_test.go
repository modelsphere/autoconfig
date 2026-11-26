package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	return getPluginIn(t, c, "higress-system", name)
}

func getPluginIn(t *testing.T, c client.Client, ns, name string) []any {
	t.Helper()
	o := &unstructured.Unstructured{}
	o.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, o); err != nil {
		t.Fatalf("get %s/%s: %v", ns, name, err)
	}
	rules, _, _ := unstructured.NestedSlice(o.Object, "spec", "matchRules")
	return rules
}

func wasmPluginIn(ns, name string, rules ...any) *unstructured.Unstructured {
	o := wasmPlugin(name, rules...)
	o.SetNamespace(ns)
	return o
}

func keyAuthPluginIn(ns string, consumers map[string]string) *unstructured.Unstructured {
	o := keyAuthPlugin(consumers)
	o.SetNamespace(ns)
	return o
}

func modelRoute(name, ns, route string, backends int, ready bool) *routingv1.ModelRoute {
	return &routingv1.ModelRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: routingv1.ModelRouteSpec{
			Discovery: routingv1.Discovery{Service: name + "-leader", Port: 8050},
			Nginx: routingv1.NginxSpec{
				// Routes from many namespaces legitimately write into ONE openresty - on the
				// test cluster half a dozen do - so the helper's default is the fleet's own
				// openresty rather than its own namespace's.
				Route: route, OutputConfigMap: "llm-route/openresty-conf",
				// 8 per instance: the fleet reads its capacity from here, so the helper has
				// to carry one — a route with no ceiling legitimately reports 0.
				Peers: []routingv1.RoutePeer{{Use: "backend", MaxConcurrency: 8}},
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
					Backend: routingv1.MemberBackend{ModelRouteRef: "glm-53-h100"}},
				{Name: "ascend", Hardware: routingv1.Hardware{Vendor: "huawei", Model: "910B4", Replicas: 1},
					Backend: routingv1.MemberBackend{ModelRouteRef: "glm-53-ascend"}},
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
	f.Spec.Gateway.AuthConsumers = []string{"*"} // a deliberately public route: no rule to write
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

// condMessage: a condition's message, for tests that check a failure NAMES what went wrong
// rather than only that something did.
func condMessage(conds []metav1.Condition, typ string) string {
	for i := range conds {
		if conds[i].Type == typ {
			return conds[i].Message
		}
	}
	return ""
}

func condTrue(conds []metav1.Condition, t string) bool {
	for _, c := range conds {
		if c.Type == t {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}

// Two fleets sharing one gateway must settle. Each fleet rewrites one rule inside the same
// WasmPlugin, and the unchanged-check compares matchRules in order, so a fleet that drops its
// own rule and appends it moves itself to the end of the list - which the other fleet then
// sees as a change and answers by moving itself to the end, every resync, forever. Each swap
// is an ECDS push of that plugin to the whole data plane. Measured on a test cluster before
// the fix: the two rules traded places about every 35s with nothing changing.
func TestTwoFleetsSharingAPluginSettle(t *testing.T) {
	a := fleetFixture()
	b := fleetFixture()
	b.Name = "qwen-7b"
	b.Spec.ModelName = "qwen-7b"

	r, c := newFleetReconciler(t, a, b,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPlugin("ai-statistics"),
	)
	reconcile := func(name string) {
		t.Helper()
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: "llm-route", Name: name},
		}); err != nil {
			t.Fatalf("reconcile %s: %v", name, err)
		}
	}
	order := func() []string {
		t.Helper()
		var out []string
		for _, item := range getPlugin(t, c, "ai-statistics") {
			ings, _, _ := unstructured.NestedStringSlice(item.(map[string]any), "ingress")
			out = append(out, ings...)
		}
		return out
	}

	reconcile("glm-5.3")
	reconcile("qwen-7b")
	first := order()
	if len(first) != 2 {
		t.Fatalf("rules = %v, want one per fleet", first)
	}

	// Now resync both, repeatedly, with nothing changed. Neither the order nor the object may
	// move again.
	rv := resourceVersionOf(t, c, "ai-statistics")
	for i := 0; i < 3; i++ {
		reconcile("glm-5.3")
		reconcile("qwen-7b")
	}
	if got := order(); !reflect.DeepEqual(got, first) {
		t.Fatalf("rule order churns: %v -> %v", first, got)
	}
	if after := resourceVersionOf(t, c, "ai-statistics"); after != rv {
		t.Fatalf("ai-statistics rewritten on resync with nothing changed: %s -> %s", rv, after)
	}
}

// ai-proxy carries the upstream credential, resolved from a Secret precisely so it never
// lands in the CR. Passing it through extraPlugins would put a credential in plaintext in the
// object AND leave the Secret-derived one unprogrammed, with a healthy-looking sync. There is
// no CEL rule to fall back on, so this refusal is the only guard - and it was briefly lost
// when the overridable set was derived from "has no tolerateMissing" instead of being stated.
func TestFleetExtraPluginCannotOverrideAIProxy(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.UpstreamAuthSecretRef = "gw-upstream:token"
	f.Spec.Gateway.ExtraPlugins = map[string]apiextensionsv1.JSON{
		"ai-proxy": rawJSON(t, map[string]any{
			"provider": map[string]any{"type": "openai", "apiTokens": []any{"sk-from-the-cr"}},
		}),
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), wasmPlugin("ai-proxy"),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "gw-upstream", Namespace: "llm-route"},
			Data:       map[string][]byte{"token": []byte("sk-from-the-secret")},
		},
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("extraPlugins was allowed to supply the upstream credential")
	}
	// ...and the credential actually programmed is still the Secret's, not the CR's.
	rule := getPlugin(t, c, "ai-proxy")[0].(map[string]any)
	tokens, _, _ := unstructured.NestedStringSlice(rule, "config", "provider", "apiTokens")
	if len(tokens) != 1 || tokens[0] != "sk-from-the-secret" {
		t.Fatalf("apiTokens = %v, want the Secret's credential", tokens)
	}
}

// status.appliedExtraPlugins is written on the failure paths too, so it must say what is
// believed to be ON the gateway, not what this round managed to write. Forgetting a live rule
// means a later key removal never withdraws it, and the rule keeps being enforced on a live
// route with nothing in the CR to say so.
func TestFleetAppliedExtraPluginsSurvivesAFailedRound(t *testing.T) {
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
	nn := types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}
	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), nn, &fleet)
	if len(fleet.Status.AppliedExtraPlugins) != 1 {
		t.Fatalf("precondition: %v", fleet.Status.AppliedExtraPlugins)
	}

	// A round that fails to reach the plugin, with the rule still on the gateway. Deleting
	// the WasmPlugin stands in for any write error: the Get fails, the key is not written.
	pl := &unstructured.Unstructured{}
	pl.SetGroupVersionKind(wasmPluginGVK)
	pl.SetNamespace("higress-system")
	pl.SetName("cluster-key-rate-limit")
	if err := c.Delete(context.Background(), pl); err != nil {
		t.Fatal(err)
	}
	_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}) // expected to fail
	_ = c.Get(context.Background(), nn, &fleet)
	if len(fleet.Status.AppliedExtraPlugins) != 1 {
		t.Fatalf("a failed round forgot a live rule: %v", fleet.Status.AppliedExtraPlugins)
	}
}

// A cluster with no Higress has no extensions.higress.io CRD at all, and then a Get on a
// WasmPlugin fails with *meta.NoKindMatchError rather than a NotFound - a different type
// entirely, which apierrors.IsNotFound does not recognise. The reachable consequence is
// deletion: the finalizer withdraws this fleet's rules and treats "not there" as done, so
// classifying that error as a real failure leaves the object stuck Terminating forever, with
// no way out but patching the finalizer off by hand. Uninstalling Higress from a cluster that
// still has fleets is enough to get there.
func TestIsAbsent(t *testing.T) {
	notFound := apierrors.NewNotFound(schema.GroupResource{
		Group: "extensions.higress.io", Resource: "wasmplugins"}, "ai-statistics")
	noKind := &meta.NoKindMatchError{GroupKind: wasmPluginGVK.GroupKind()}
	noResource := &meta.NoResourceMatchError{PartialResource: wasmPluginGVK.GroupVersion().
		WithResource("wasmplugins")}
	for _, err := range []error{notFound, noKind, noResource} {
		if !isAbsent(err) {
			t.Errorf("isAbsent(%T) = false, want true (%v)", err, err)
		}
	}
	// A conflict or a permission problem is NOT absence: those must fail the sync loudly.
	for _, err := range []error{
		apierrors.NewConflict(schema.GroupResource{Resource: "wasmplugins"}, "x", nil),
		apierrors.NewForbidden(schema.GroupResource{Resource: "wasmplugins"}, "x", nil),
	} {
		if isAbsent(err) {
			t.Errorf("isAbsent(%T) = true, want false (%v)", err, err)
		}
	}
}

// gateway.service is documented as "ns/name:port". Every malformed part has to fail the sync:
// an Ingress pointing at the wrong port is published without complaint and then 502s on the
// first real request. "8o80" used to parse as port 8, because %d stops at the first
// non-digit and still reports success.
func TestUpstreamService(t *testing.T) {
	fleetWith := func(svc string) *routingv1.ModelFleet {
		f := fleetFixture()
		f.Spec.Gateway.Service = svc
		return f
	}
	for _, tc := range []struct {
		svc  string
		name string
		port int32
	}{
		{"", "openresty", 8080},
		{"openresty", "openresty", 8080},
		{"openresty:8080", "openresty", 8080},
		{"llm-route/openresty:9090", "openresty", 9090},
	} {
		n, p, err := upstreamService(fleetWith(tc.svc))
		if err != nil || n != tc.name || p != tc.port {
			t.Errorf("upstreamService(%q) = %q, %d, %v; want %q, %d", tc.svc, n, p, err, tc.name, tc.port)
		}
	}
	for _, svc := range []string{
		"openresty:8o80",                // silently became port 8
		"openresty:0",                   // not a port
		"openresty:99999",               // not a port
		"openresty:",                    // no port at all
		"higress-system/openresty:8080", // an Ingress cannot reach another namespace
	} {
		if n, p, err := upstreamService(fleetWith(svc)); err == nil {
			t.Errorf("upstreamService(%q) = %q, %d, nil; want an error", svc, n, p)
		}
	}
}

// gateway.upstreamAuthSecretRef must not reach outside the fleet's own namespace. The
// controller holds a cluster-wide `secrets: get`, the value it reads is written in plain text
// into the ai-proxy WasmPlugin, and anyone who can write a ModelFleet could otherwise name
// kube-system/any-secret:any-key and read it back - "write a ModelFleet" would mean "read
// every Secret in the cluster".
func TestFleetUpstreamSecretRefCannotCrossNamespaces(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.UpstreamAuthSecretRef = "kube-system/stolen:token"
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), wasmPlugin("ai-proxy"),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "stolen", Namespace: "kube-system"},
			Data:       map[string][]byte{"token": []byte("sk-not-yours")},
		},
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("a Secret in another namespace was resolved")
	}
	// Nothing may be written at all, least of all the value.
	for _, item := range getPlugin(t, c, "ai-proxy") {
		if strings.Contains(fmt.Sprint(item), "sk-not-yours") {
			t.Fatalf("the foreign Secret's value reached the gateway: %v", item)
		}
	}
	// A ref in the fleet's own namespace still works, with or without the prefix.
	for _, ref := range []string{"llm-route/mine:token", "mine:token"} {
		if _, err := r.readSecretKey(context.Background(), ref, "llm-route"); err == nil ||
			!strings.Contains(err.Error(), "not found") {
			// The Secret does not exist here, so "not found" is the expected failure - what
			// matters is that it was not refused for being in another namespace.
			if err != nil && strings.Contains(err.Error(), "own namespace") {
				t.Errorf("%q was refused as cross-namespace", ref)
			}
		}
	}
}

// Two fleets claiming one model name publish two Ingresses matching the same header, and
// which one a request lands on is undefined; they also fight over one key in the decision
// table. The older one keeps serving and the newcomer reports the conflict - the other way
// round, a typo in a new fleet would take a route that already carries traffic off the air.
func TestTwoFleetsCannotClaimOneModelName(t *testing.T) {
	old := fleetFixture()
	old.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	newer := fleetFixture()
	newer.Name = "glm-53-copy"
	newer.Spec.ModelName = "glm-5.3" // the same model as the incumbent
	newer.CreationTimestamp = metav1.NewTime(time.Now())

	r, c := newFleetReconciler(t, old, newer,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcile := func(name string) {
		t.Helper()
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: "llm-route", Name: name},
		}); err != nil {
			t.Fatalf("reconcile %s: %v", name, err)
		}
	}
	reconcile("glm-5.3")
	reconcile("glm-53-copy")

	var incumbent, copy routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &incumbent)
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-53-copy"}, &copy)
	if !condTrue(incumbent.Status.Conditions, condGatewaySynced) {
		t.Fatalf("the incumbent stopped serving over a newcomer's name clash: %+v", incumbent.Status.Conditions)
	}
	if condTrue(copy.Status.Conditions, condGatewaySynced) {
		t.Fatal("two fleets were allowed to claim one model name")
	}
	// ...and the second fleet published no Ingress of its own.
	var ing networkingv1.Ingress
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "llm-fleet-glm-53-copy"}, &ing)
	if err == nil {
		t.Fatal("the clashing fleet published an Ingress matching the same model header")
	}
}

// Teardown is the mirror of creation: the route goes first, the rules after. Withdrawing
// key-auth's rule while the Ingress is still live leaves the route reachable and
// unauthenticated (the gateway chart's globalAuth is false, so a route with no rule is open),
// and because cleanup carries on past errors, one failed withdrawal would hold that state
// through every retry.
func TestFleetDeleteTakesTheRouteDownBeforeItsGuards(t *testing.T) {
	f := fleetFixture()
	now := metav1.Now()
	f.DeletionTimestamp = &now
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		wasmPlugin("ai-backend-router"),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}, map[string]any{
			"ingress":       []any{"llm-route/llm-fleet-glm-5.3"},
			"configDisable": false,
			"config":        map[string]any{"allow": []any{"ops"}},
		}),
		&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{
			Name: "llm-fleet-glm-5.3", Namespace: "llm-route"}},
	)
	// First pass: the Ingress is deleted and the pass returns without touching any rule,
	// because the route must be gone before the rule that keeps it closed is.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var ing networkingv1.Ingress
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err == nil {
		t.Fatal("the Ingress is still live after the first teardown pass")
	}
	if n := len(getPlugin(t, c, "key-auth")); n != 1 {
		t.Fatalf("key-auth rules = %d: the guard was withdrawn while the route was still live", n)
	}
	// Second pass: the route is gone, so now the rules go.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n := len(getPlugin(t, c, "key-auth")); n != 0 {
		t.Fatalf("key-auth rules = %d after the route was gone, want 0", n)
	}
}

// Omitting authConsumers must close the route, not open it. The two cases are different
// WRITES, not different values: `allow: []` is a rule the plugin reads as "nobody is
// allowed" (403), while NO rule at all means the plugin never runs for this route, and with
// global_auth=false that is open. Omission therefore has to produce the empty rule, and only
// ["*"] produces no rule.
func TestFleetEmptyAuthConsumersDeniesEveryone(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.AuthConsumers = nil
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	nn := types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}
	_ = c.Get(context.Background(), nn, &fleet)
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("a closed route failed to publish: %+v", fleet.Status.Conditions)
	}
	rules := getPlugin(t, c, "key-auth")
	if len(rules) != 1 {
		t.Fatalf("key-auth rules = %d, want 1 (an empty allow-list is still a rule)", len(rules))
	}
	allow, found, _ := unstructured.NestedSlice(rules[0].(map[string]any), "config", "allow")
	if !found {
		t.Fatalf("no allow key in the rule: %v", rules[0])
	}
	if len(allow) != 0 {
		t.Fatalf("allow = %v, want empty", allow)
	}
	// allow: null would fail the plugin's array parse, and a rule it cannot parse denies
	// nobody - so the empty list has to survive a round trip as [].
	b, err := json.Marshal(rules[0].(map[string]any)["config"])
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"allow":[]}` {
		t.Fatalf("rendered config = %s, want {\"allow\":[]}", b)
	}

	// ...and the sentinel is the one case that writes no rule, which is what open looks like.
	fleet.Spec.Gateway.AuthConsumers = []string{"*"}
	if err := c.Update(context.Background(), &fleet); err != nil {
		t.Fatal(err)
	}
	reconcileFleet(t, r)
	_ = c.Get(context.Background(), nn, &fleet)
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf(`authConsumers: ["*"] did not publish: %+v`, fleet.Status.Conditions)
	}
	if n := len(getPlugin(t, c, "key-auth")); n != 0 {
		t.Fatalf("key-auth rules = %d for a public route, want 0", n)
	}
}

// A member in another namespace is fine — what matters is which openresty serves its path.
// Routes from several namespaces routinely write into one openresty (on the test cluster,
// minimax-h3, modelforge-01-glm, pp-strip-test and vllm-mock all write into
// llm-route/openresty-conf), and a fleet made of those is a legitimate setup. An earlier
// version of this check refused any cross-namespace ref, on the assumption that an openresty
// serves only its own namespace's routes. It does not.
func TestFleetMemberFromAnotherNamespaceIsFineWhenItSharesTheOpenresty(t *testing.T) {
	f := fleetFixture()
	f.Spec.Members[1].Backend.ModelRouteRef = "other-ns/glm-53-ascend"
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "other-ns", "glm-53-ascend", 1, true), // writes into llm-route's openresty
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("a member sharing this fleet's openresty was refused: %+v", fleet.Status.Conditions)
	}
	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	if len(backends) != 2 {
		t.Fatalf("backends = %v, want both members", backends)
	}
}

// ...and the case that actually breaks: a member whose route is served by a DIFFERENT
// openresty. Its path is not on the openresty this fleet's Ingress points at, so a request
// routed there would 404 while the fleet reports the member healthy. The namespace of the
// ModelRoute says nothing about this — a same-namespace route can write elsewhere, which the
// old check missed entirely.
func TestFleetMemberServedByAnotherOpenrestyIsRefused(t *testing.T) {
	stray := modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true)
	stray.Spec.Nginx.OutputConfigMap = "other-gw/openresty-conf"
	r, c := newFleetReconciler(t, fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		stray,
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("a member served by a different openresty was accepted")
	}
	msg := condMessage(fleet.Status.Conditions, condGatewaySynced)
	if !strings.Contains(msg, "other-gw/openresty-conf") {
		t.Fatalf("the message does not name the openresty it would have needed: %q", msg)
	}
}

// Rules are written into WasmPlugins in spec.gateway.namespace, and the reconciler only sees
// the namespace that is there now. Changing the field would otherwise leave a full set of
// rules behind, bound to this fleet's Ingress reference, that nothing would ever remove.
func TestFleetMovingGatewayNamespaceWithdrawsTheOldRules(t *testing.T) {
	f := fleetFixture()
	f.Spec.Gateway.Namespace = "higress-system"
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPluginIn("gw-two", "ai-backend-router"), keyAuthPluginIn("gw-two", map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)
	if n := len(getPluginIn(t, c, "higress-system", "ai-backend-router")); n != 1 {
		t.Fatalf("precondition: rules in the first namespace = %d", n)
	}
	var fleet routingv1.ModelFleet
	nn := types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}
	_ = c.Get(context.Background(), nn, &fleet)
	if got := fleet.Status.AppliedGatewayNamespace; got != "higress-system" {
		t.Fatalf("status.appliedGatewayNamespace = %q", got)
	}

	fleet.Spec.Gateway.Namespace = "gw-two"
	if err := c.Update(context.Background(), &fleet); err != nil {
		t.Fatal(err)
	}
	reconcileFleet(t, r)

	if n := len(getPluginIn(t, c, "gw-two", "ai-backend-router")); n != 1 {
		t.Fatalf("rules in the new namespace = %d, want 1", n)
	}
	if n := len(getPluginIn(t, c, "higress-system", "ai-backend-router")); n != 0 {
		t.Fatalf("rules left behind in the old namespace = %d, want 0", n)
	}
	_ = c.Get(context.Background(), nn, &fleet)
	if got := fleet.Status.AppliedGatewayNamespace; got != "gw-two" {
		t.Fatalf("status.appliedGatewayNamespace = %q, want gw-two", got)
	}
}

// The Members print column is fed by a field, because a print column's JSONPath is evaluated
// by the API server and cannot count a list - pointing it at .status.members.length() renders
// an empty column, which is worse than the wrong-but-populated one it replaced.
func TestFleetStatusCountsItsMembers(t *testing.T) {
	r, c := newFleetReconciler(t, fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if got, want := fleet.Status.MemberCount, int32(len(fleet.Status.Members)); got != want {
		t.Fatalf("status.memberCount = %d, want %d", got, want)
	}
	if fleet.Status.MemberCount != 2 {
		t.Fatalf("status.memberCount = %d, want 2", fleet.Status.MemberCount)
	}
	// ...and it is not the replica count, which is the number it used to show.
	if fleet.Status.TotalCapacity != nil && fleet.Status.MemberCount == fleet.Status.TotalCapacity.ReadyReplicas {
		t.Fatalf("memberCount %d equals readyReplicas: the fixture no longer distinguishes them",
			fleet.Status.MemberCount)
	}
}

// A fleet with no traffic policy yet must use all of its members, split evenly. Leaving
// everything on the default member would mean a console that adds hardware to a fleet sees
// nothing happen until decision-gen writes a split - and the added member sits idle while
// reporting Ready. Nothing covered this branch before: every fixture here has a policy.
func TestFleetWithNoPolicySplitsEvenly(t *testing.T) {
	f := fleetFixture()
	f.Spec.TrafficPolicy = nil
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	got := map[string]int64{}
	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	for _, b := range backends {
		m := b.(map[string]any)
		id, _, _ := unstructured.NestedString(m, "id")
		w, _, _ := unstructured.NestedInt64(m, "weight")
		tier, _, _ := unstructured.NestedInt64(m, "tier")
		if tier != 0 {
			t.Fatalf("member %s landed in tier %d with no policy", id, tier)
		}
		got[id] = w
	}
	if len(got) != 2 {
		t.Fatalf("backends = %v, want both members", got)
	}
	if got["h100"] != 5000 || got["ascend"] != 5000 {
		t.Fatalf("weights = %v, want an even split", got)
	}

	// ...and status says the same thing, so the console is not told something else.
	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	for _, m := range fleet.Status.Members {
		if m.EffectiveWeight != 5000 {
			t.Fatalf("status member %s effectiveWeight = %d, want 5000", m.Name, m.EffectiveWeight)
		}
	}
}

// The even split is over members that can actually serve. A member that is not up yet must
// not hold a share that goes nowhere.
func TestFleetWithNoPolicySplitsOverReadyMembersOnly(t *testing.T) {
	f := fleetFixture()
	f.Spec.TrafficPolicy = nil
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 0, false), // not up
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	if len(backends) != 1 {
		t.Fatalf("backends = %v, want only the ready member", backends)
	}
	m := backends[0].(map[string]any)
	id, _, _ := unstructured.NestedString(m, "id")
	w, _, _ := unstructured.NestedInt64(m, "weight")
	if id != "h100" || w != 10000 {
		t.Fatalf("backend = %s at %d, want h100 at 10000", id, w)
	}
}

// An all-zero policy is a decision, not an absence: it means "drain this fleet onto its
// default member", which is the only way to take every member out of rotation without
// deleting the object. It must NOT be mistaken for "no policy" and turned into an even split.
func TestFleetExplicitlyZeroedPolicyIsNotAnEvenSplit(t *testing.T) {
	f := fleetFixture()
	f.Spec.TrafficPolicy.Members = []routingv1.PolicyMember{
		{Name: "h100", Tier: 0, Weight: 0},
		{Name: "ascend", Tier: 0, Weight: 0},
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	if len(backends) != 1 {
		t.Fatalf("backends = %v, want only the default member", backends)
	}
	if id, _, _ := unstructured.NestedString(backends[0].(map[string]any), "id"); id != "h100" {
		t.Fatalf("backend = %s, want the default member h100", id)
	}
}

// A policy that exists is authoritative, so a member it does not mention gets no traffic.
// That is the right rule - otherwise nothing could ever take a member out - but a writer that
// lists only the members it has measurements for would retire the rest without meaning to.
// The fleet keeps serving; it says so on PolicyApplied rather than only in the gateway's
// counters.
func TestFleetReportsMembersAPolicyLeavesOut(t *testing.T) {
	f := fleetFixture()
	f.Spec.TrafficPolicy.Members = []routingv1.PolicyMember{{Name: "h100", Tier: 0, Weight: 10000}}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("the fleet stopped syncing over an incomplete policy: %+v", fleet.Status.Conditions)
	}
	var policy *metav1.Condition
	for i := range fleet.Status.Conditions {
		if fleet.Status.Conditions[i].Type == condPolicyApplied {
			policy = &fleet.Status.Conditions[i]
		}
	}
	if policy == nil || policy.Reason != "MembersNotInPolicy" {
		t.Fatalf("PolicyApplied = %+v, want MembersNotInPolicy", policy)
	}
	if !strings.Contains(policy.Message, "ascend") {
		t.Fatalf("the omitted member is not named: %q", policy.Message)
	}
	// ...and the member really is out of rotation, which is what the report is about.
	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	for _, b := range backends {
		if id, _, _ := unstructured.NestedString(b.(map[string]any), "id"); id == "ascend" {
			t.Fatal("a member absent from the policy still got traffic")
		}
	}
}

// The report is only for a policy that exists. With no policy every member is served, so
// there is nothing to warn about - and a warning there would be permanent noise on every
// fleet that has not been tuned yet.
func TestFleetWithNoPolicyReportsNothingUncovered(t *testing.T) {
	f := fleetFixture()
	f.Spec.TrafficPolicy = nil
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	for _, cond := range fleet.Status.Conditions {
		if cond.Type == condPolicyApplied && cond.Status != metav1.ConditionTrue {
			t.Fatalf("PolicyApplied=%s with no policy written: %s", cond.Status, cond.Message)
		}
	}
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
	f.Spec.Gateway.AuthConsumers = []string{"*"} // a deliberately public route: no rule to write
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
func keyAuthPlugin(consumers map[string]string, rules ...any) *unstructured.Unstructured {
	o := wasmPlugin("key-auth", rules...)
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
// fleet does not have to restate one cluster-wide fact. That file is openresty's own, and its
// format is "key[:owner]" - sending the file's bytes as the bearer token sends "sk-...:admin",
// which openresty refuses with 401 on every request while the sync still reports success.
// Measured on a test cluster, where the key in the file was the right one and still 401'd.
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
			// The real shape of that file on these clusters, owner label and all.
			Data: map[string][]byte{"keys": []byte("sk-default:admin\n")},
		},
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-proxy")[0].(map[string]any)
	tokens, _, _ := unstructured.NestedStringSlice(rule, "config", "provider", "apiTokens")
	if len(tokens) != 1 || tokens[0] != "sk-default" {
		t.Fatalf("apiTokens = %v, want the key alone, without the owner label", tokens)
	}
}

// The file holds several keys while one is being rotated out, and openresty accepts all of
// them, so any single one is a working credential.
func TestFirstOpenrestyAPIKey(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"sk-one:admin", "sk-one"},
		{"sk-one", "sk-one"},                      // no owner label
		{" sk-one:admin , sk-two:ops ", "sk-one"}, // rotation in progress
		{",,sk-two:ops", "sk-two"},                // empty entries skipped
		{"sk-one:", "sk-one"},                     // colon, no owner
	} {
		got, err := firstOpenrestyAPIKey(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("firstOpenrestyAPIKey(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	// Nothing usable must be an error, not an empty token: an empty bearer is a 401 on every
	// request, reported as a healthy sync.
	for _, in := range []string{"", "   ", ",,", ":nobody"} {
		if got, err := firstOpenrestyAPIKey(in); err == nil {
			t.Errorf("firstOpenrestyAPIKey(%q) = %q, want an error", in, got)
		}
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
// This is the only guard against it: a CEL rule on extraPlugins cannot exist, because the
// API server leaves a map with x-kubernetes-preserve-unknown-fields values out of the CEL
// type system and would reject the whole CRD.
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

// Stopping an override of a plugin the controller also renders itself must NOT withdraw it:
// the built-in renderer has just put our default back on the route, and withdrawing it in the
// same pass would take token accounting off a live route moments after programming it.
func TestFleetStoppingAnOverrideRestoresTheDefaultRule(t *testing.T) {
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

	var fleet routingv1.ModelFleet
	nn := types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}
	_ = c.Get(context.Background(), nn, &fleet)
	if len(fleet.Status.AppliedExtraPlugins) != 1 {
		t.Fatalf("precondition: status = %v", fleet.Status.AppliedExtraPlugins)
	}
	fleet.Spec.Gateway.ExtraPlugins = nil
	if err := c.Update(context.Background(), &fleet); err != nil {
		t.Fatal(err)
	}
	reconcileFleet(t, r)

	rules := getPlugin(t, c, "ai-statistics")
	if len(rules) != 1 {
		t.Fatalf("ai-statistics rules = %d, want our default back (withdrawn by the override bookkeeping?)", len(rules))
	}
	rule := rules[0].(map[string]any)
	attrs, _, _ := unstructured.NestedSlice(rule, "config", "attributes")
	if len(attrs) != 1 {
		t.Fatalf("config = %v", rule["config"])
	}
	if key, _, _ := unstructured.NestedString(attrs[0].(map[string]any), "key"); key != "requested_model" {
		t.Fatalf("the override is still live after its key was removed: %v", rule["config"])
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

// ---------------------------------------------------------------------------
// The invariants the design rests on, and the object shapes the gateway reads.
// ---------------------------------------------------------------------------

// Plugin rules first, Ingress last. The Ingress is what makes the route reachable, so a run
// that fails half way through the plugins must NOT publish one: an Ingress whose key-auth rule
// was never written is a live, unauthenticated route answering 200, with nothing but a
// condition on the CR to say so. The existing tests check the condition; this checks the
// thing the condition is about.
func TestFleetPluginFailureLeavesNoIngress(t *testing.T) {
	r, c := newFleetReconciler(t, fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		// no ai-backend-router: there is nowhere to put the decision table
	)
	reconcileFleet(t, r)

	var ing networkingv1.Ingress
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing)
	if err == nil {
		t.Fatal("an Ingress was published although a plugin rule could not be written")
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error getting the Ingress: %v", err)
	}
}

// The Ingress is the one object the gateway reads to decide that a request belongs to this
// fleet. Every part of it is load-bearing, and none of it is checked anywhere else in Go.
func TestFleetIngressShape(t *testing.T) {
	r, c := newFleetReconciler(t, fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var ing networkingv1.Ingress
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err != nil {
		t.Fatal(err)
	}
	if ing.Spec.IngressClassName == nil || *ing.Spec.IngressClassName != "higress" {
		t.Fatalf("ingressClassName = %v", ing.Spec.IngressClassName)
	}
	a := ing.Annotations
	if got := a["higress.io/exact-match-header-x-higress-llm-model"]; got != "glm-5.3" {
		t.Fatalf("model header match = %q", got)
	}
	// No rewrite-path: the path is the router plugin's to choose per member, and an
	// Ingress-level rewrite would pin every member to one path.
	for k := range a {
		if strings.Contains(k, "rewrite-path") {
			t.Fatalf("annotation %q pins every member to one path", k)
		}
	}
	// Long-lived streaming responses: the defaults cut a generation short.
	for _, k := range []string{"higress.io/proxy-read-timeout", "higress.io/proxy-send-timeout"} {
		if a[k] != "3600" {
			t.Fatalf("%s = %q, want 3600", k, a[k])
		}
	}
	if n := len(ing.Spec.Rules); n != 1 {
		t.Fatalf("rules = %d, want 1", n)
	}
	paths := ing.Spec.Rules[0].HTTP.Paths
	if len(paths) != 1 {
		t.Fatalf("paths = %d, want 1", len(paths))
	}
	p := paths[0]
	if p.PathType == nil || *p.PathType != networkingv1.PathTypePrefix {
		t.Fatalf("pathType = %v, want Prefix", p.PathType)
	}
	if p.Path != "/" {
		t.Fatalf("path = %q, want / (the router rewrites per member)", p.Path)
	}
	svc := p.Backend.Service
	if svc == nil || svc.Name != "openresty" || svc.Port.Number != 8080 {
		t.Fatalf("backend = %+v, want openresty:8080", svc)
	}
	// Owned by the fleet, so deleting the fleet takes the route with it.
	if n := len(ing.OwnerReferences); n != 1 || ing.OwnerReferences[0].Kind != "ModelFleet" {
		t.Fatalf("ownerReferences = %+v", ing.OwnerReferences)
	}
}

// Every write to the Ingress is a config push to the gateway, so an unchanged fleet must not
// produce one. CreateOrUpdate only writes on a real diff - this pins that, because a mutate
// function that sets a field to a fresh value every time (a timestamp, a re-sorted list)
// would turn each resync into a push and nothing else would notice.
func TestFleetIngressIsNotRewrittenWhenUnchanged(t *testing.T) {
	r, c := newFleetReconciler(t, fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)
	nn := types.NamespacedName{Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}
	var first networkingv1.Ingress
	if err := c.Get(context.Background(), nn, &first); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		reconcileFleet(t, r)
	}
	var again networkingv1.Ingress
	if err := c.Get(context.Background(), nn, &again); err != nil {
		t.Fatal(err)
	}
	if again.ResourceVersion != first.ResourceVersion {
		t.Fatalf("the Ingress was rewritten on an unchanged reconcile: %s -> %s",
			first.ResourceVersion, again.ResourceVersion)
	}
}

// ...and it does follow a real change, or the gateway would keep sending this model's traffic
// to the Service the fleet used to name.
func TestFleetIngressFollowsTheGatewayService(t *testing.T) {
	f := fleetFixture()
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	nn := types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}
	_ = c.Get(context.Background(), nn, &fleet)
	fleet.Spec.Gateway.Service = "openresty-canary:9090"
	if err := c.Update(context.Background(), &fleet); err != nil {
		t.Fatal(err)
	}
	reconcileFleet(t, r)

	var ing networkingv1.Ingress
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err != nil {
		t.Fatal(err)
	}
	svc := ing.Spec.Rules[0].HTTP.Paths[0].Backend.Service
	if svc.Name != "openresty-canary" || svc.Port.Number != 9090 {
		t.Fatalf("backend = %+v, want openresty-canary:9090", svc)
	}
}

// Deleting a fleet must finish on a cluster that no longer serves WasmPlugin at all - after
// Higress is uninstalled, say. A Get on a kind with no RESTMapping fails with
// *meta.NoKindMatchError rather than NotFound, and treating that as a real failure would
// leave the object stuck Terminating with no way out but patching the finalizer off by hand.
func TestFleetDeleteCompletesWhenThePluginKindIsGone(t *testing.T) {
	f := fleetFixture()
	now := metav1.Now()
	f.DeletionTimestamp = &now
	s := fleetScheme(t)
	c := ctrlfake.NewClientBuilder().WithScheme(s).
		WithObjects(f, modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true)).
		WithStatusSubresource(&routingv1.ModelFleet{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == wasmPluginGVK {
					return &meta.NoKindMatchError{GroupKind: wasmPluginGVK.GroupKind()}
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	r := &ModelFleetReconciler{Client: c, Scheme: s}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"},
	}); err != nil {
		t.Fatalf("teardown failed on a cluster with no WasmPlugin kind: %v", err)
	}
	var fleet routingv1.ModelFleet
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if err == nil && len(fleet.Finalizers) > 0 {
		t.Fatalf("the finalizer is still on the object: %v", fleet.Finalizers)
	}
}

// The uniqueness check is per gateway, not global: two clusters' worth of gateways in one
// cluster (a canary namespace next to the real one) legitimately serve the same model name,
// and the Ingresses live in different namespaces with different plugins.
func TestTwoFleetsMayShareAModelNameOnDifferentGateways(t *testing.T) {
	a := fleetFixture()
	a.Spec.Gateway.Namespace = "higress-system"
	b := fleetFixture()
	b.Name = "glm-53-canary"
	b.Spec.ModelName = "glm-5.3"
	b.Spec.Gateway.Namespace = "higress-canary"

	r, c := newFleetReconciler(t, a, b,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPluginIn("higress-canary", "ai-backend-router"),
		keyAuthPluginIn("higress-canary", map[string]string{"ops": "sk-ops"}),
	)
	for _, name := range []string{"glm-5.3", "glm-53-canary"} {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: "llm-route", Name: name},
		}); err != nil {
			t.Fatalf("reconcile %s: %v", name, err)
		}
	}
	for _, name := range []string{"glm-5.3", "glm-53-canary"} {
		var fleet routingv1.ModelFleet
		_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: name}, &fleet)
		if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
			t.Fatalf("%s was refused although the other fleet is on a different gateway: %+v",
				name, fleet.Status.Conditions)
		}
	}
}

// totalCapacity is what the console shows as the fleet's capacity, so it must count only what
// can actually serve. Summing a member that is down would overstate the fleet exactly when
// someone is looking at it because it is degraded.
func TestFleetTotalCapacityCountsReadyMembersOnly(t *testing.T) {
	r, c := newFleetReconciler(t, fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 0, false), // down
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	tc := fleet.Status.TotalCapacity
	if tc == nil {
		t.Fatal("no totalCapacity")
	}
	// Read from the routes, not declared: h100 has 2 backends at 8 each, ascend is down.
	if tc.MaxConcurrency != 16 || tc.ReadyReplicas != 2 {
		t.Fatalf("totalCapacity = %+v, want only the ready member's 16/2", tc)
	}
}

// modelName is what the client sends as body.model. Defaulting it to the object's name is how
// a single-hardware fleet stays a one-liner, and it has to reach the Ingress match and the
// decision table, not just status.
func TestFleetModelNameDefaultsToTheObjectName(t *testing.T) {
	f := fleetFixture()
	f.Spec.ModelName = ""
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var ing networkingv1.Ingress
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err != nil {
		t.Fatal(err)
	}
	if got := ing.Annotations["higress.io/exact-match-header-x-higress-llm-model"]; got != "glm-5.3" {
		t.Fatalf("Ingress matches %q, want the object's name", got)
	}
	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	if _, found, _ := unstructured.NestedMap(rule, "config", "decisions", "static", "models", "glm-5.3"); !found {
		t.Fatalf("the table has no entry under the object's name: %v", rule["config"])
	}
	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if fleet.Status.ModelName != "glm-5.3" {
		t.Fatalf("status.modelName = %q", fleet.Status.ModelName)
	}
}

// Tiers are an ordered fallback chain, so weights renormalise WITHIN a tier and never across
// one: moving weight over a tier boundary would change which hardware is the mains, which is
// the one thing the tier exists to pin down.
func TestFleetWeightsRenormaliseWithinEachTier(t *testing.T) {
	f := fleetFixture()
	f.Spec.Members = append(f.Spec.Members, routingv1.FleetMember{
		Name:     "rtx6000d",
		Hardware: routingv1.Hardware{Vendor: "nvidia", Model: "RTX6000D", Replicas: 1},
		Backend:  routingv1.MemberBackend{ModelRouteRef: "glm-53-6000d"},
	})
	f.Spec.TrafficPolicy.Members = []routingv1.PolicyMember{
		{Name: "h100", Tier: 0, Weight: 3},
		{Name: "ascend", Tier: 0, Weight: 1},
		{Name: "rtx6000d", Tier: 1, Weight: 7}, // alone in its tier, whatever the number
	}
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		modelRoute("glm-53-6000d", "llm-route", "glm-53-6000d", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	perTier := map[int64]int64{}
	got := map[string]int64{}
	for _, b := range backends {
		m := b.(map[string]any)
		id, _, _ := unstructured.NestedString(m, "id")
		w, _, _ := unstructured.NestedInt64(m, "weight")
		tier, _, _ := unstructured.NestedInt64(m, "tier")
		got[id] = w
		perTier[tier] += w
	}
	if got["h100"] != 7500 || got["ascend"] != 2500 {
		t.Fatalf("tier 0 split = %v, want 7500/2500 from 3:1", got)
	}
	if got["rtx6000d"] != 10000 {
		t.Fatalf("the sole member of tier 1 got %d, want the whole tier", got["rtx6000d"])
	}
	for tier, sum := range perTier {
		if sum != 10000 {
			t.Fatalf("tier %d sums to %d, want exactly 10000", tier, sum)
		}
	}
}

// A one-member fleet is how every single-hardware model is published once "everything goes
// through ModelFleet" is the rule, so it has to be the boring case: one backend, everything.
func TestFleetWithOneMemberGetsEverything(t *testing.T) {
	f := fleetFixture()
	f.Spec.Members = f.Spec.Members[:1]
	f.Spec.TrafficPolicy = nil
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	if len(backends) != 1 {
		t.Fatalf("backends = %v, want exactly one", backends)
	}
	m := backends[0].(map[string]any)
	id, _, _ := unstructured.NestedString(m, "id")
	w, _, _ := unstructured.NestedInt64(m, "weight")
	if id != "h100" || w != 10000 {
		t.Fatalf("backend = %s at %d, want h100 at 10000", id, w)
	}
	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if fleet.Status.MemberCount != 1 {
		t.Fatalf("status.memberCount = %d", fleet.Status.MemberCount)
	}
}

// Capacity is READ from each member's ModelRoute, not declared on the fleet: "per-instance
// ceiling × discovered backends", the same rule the ModelRoute controller uses to size CART's
// dynamic concurrency. It therefore follows a member scaling with nobody editing a field —
// which is the whole reason the declared copy was removed.
func TestFleetCapacityFollowsTheRoute(t *testing.T) {
	r, c := newFleetReconciler(t, fleetFixture(),
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	capacity := func() (int32, map[string]int32) {
		var f routingv1.ModelFleet
		_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &f)
		per := map[string]int32{}
		for _, m := range f.Status.Members {
			per[m.Name] = m.MaxConcurrency
		}
		return f.Status.TotalCapacity.MaxConcurrency, per
	}
	total, per := capacity()
	// 8 per instance in the helper: h100 has 2 backends, ascend 1.
	if per["h100"] != 16 || per["ascend"] != 8 || total != 24 {
		t.Fatalf("capacity = %d, per member %v, want 16+8", total, per)
	}

	// The member scales out. Nothing in the fleet changes, and the capacity has to move.
	var mr routingv1.ModelRoute
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "glm-53-h100"}, &mr); err != nil {
		t.Fatal(err)
	}
	mr.Status.Backends = 5
	// A plain Update: only ModelFleet is registered as having a status subresource on this
	// fake client, so Status().Update on a ModelRoute fails with a confusing NotFound.
	if err := c.Update(context.Background(), &mr); err != nil {
		t.Fatal(err)
	}
	reconcileFleet(t, r)
	total, per = capacity()
	if per["h100"] != 40 || total != 48 {
		t.Fatalf("after scaling to 5 backends: capacity = %d, per member %v, want 40+8", total, per)
	}
}

// A route that declares no per-instance ceiling reports 0 rather than a guess: openresty
// falls back to the route's default_max there, and inventing a number here would put a
// figure on the console that nothing enforces.
func TestFleetCapacityIsZeroWhenTheRouteDeclaresNone(t *testing.T) {
	route := modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true)
	route.Spec.Nginx.Peers = []routingv1.RoutePeer{{Use: "backend"}} // no maxConcurrency
	f := fleetFixture()
	f.Spec.Members = f.Spec.Members[:1]
	f.Spec.TrafficPolicy = nil
	r, c := newFleetReconciler(t, f, route,
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if got := fleet.Status.TotalCapacity.MaxConcurrency; got != 0 {
		t.Fatalf("totalCapacity.maxConcurrency = %d, want 0", got)
	}
	if got := fleet.Status.Members[0].MaxConcurrency; got != 0 {
		t.Fatalf("member maxConcurrency = %d, want 0", got)
	}
	// ...and the fleet is otherwise perfectly healthy: no ceiling is not an error.
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("a route with no declared ceiling failed the sync: %+v", fleet.Status.Conditions)
	}
}

// defaultMember is optional: omitted means members[0]. The field cannot go away — the plugin
// requires `default` to name one of the model's backends and rejects the WHOLE decision table
// otherwise (not just that model), which then stalls the config push for every plugin on the
// gateway — but a single-hardware fleet should not have to state the obvious.
func TestFleetDefaultMemberDefaultsToTheFirstMember(t *testing.T) {
	f := fleetFixture()
	f.Spec.DefaultMember = ""
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	def, found, _ := unstructured.NestedString(rule, "config", "decisions", "static", "models", "glm-5.3", "default")
	if !found || def != "h100" {
		t.Fatalf("table default = %q (found=%v), want the first member", def, found)
	}
	// ...and the console is told which one it is, rather than having to re-derive it.
	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if fleet.Status.DefaultMember != "h100" {
		t.Fatalf("status.defaultMember = %q", fleet.Status.DefaultMember)
	}
}

// The defaulted member is subject to the same rule as a stated one: it stays in the table
// even when it is unready, because a table whose `default` names a backend that is not there
// is rejected whole — one member's outage would otherwise freeze the gateway's config.
func TestFleetDefaultedMemberStaysInTheTableWhenUnready(t *testing.T) {
	f := fleetFixture()
	f.Spec.DefaultMember = ""
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 0, false), // the default, down
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	var sawDefault bool
	for _, b := range backends {
		m := b.(map[string]any)
		if id, _, _ := unstructured.NestedString(m, "id"); id == "h100" {
			sawDefault = true
			if w, _, _ := unstructured.NestedInt64(m, "weight"); w != 0 {
				t.Fatalf("the unready default carries weight %d, want 0", w)
			}
		}
	}
	if !sawDefault {
		t.Fatal("the defaulted member was dropped from the table while unready - the plugin " +
			"would reject the whole table and freeze this gateway's config")
	}
}

// The manual kill switch, and the whole reason it is a field of its own rather than a zero
// weight: decision-gen recomputes trafficPolicy from metrics and would put the member back
// within the minute. A disabled member stays out however generous the policy is.
func TestFleetDisabledMemberIsOutOfRotationDespiteItsWeight(t *testing.T) {
	f := fleetFixture() // policy gives h100 8000, ascend 2000
	f.Spec.Members[1].Disabled = true
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true), // perfectly healthy
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	for _, b := range backends {
		if id, _, _ := unstructured.NestedString(b.(map[string]any), "id"); id == "ascend" {
			t.Fatal("a disabled member is still in the decision table - the kill switch did nothing")
		}
	}
	// ...and its share went to the rest of its tier rather than onto the floor.
	m := backends[0].(map[string]any)
	if w, _, _ := unstructured.NestedInt64(m, "weight"); w != 10000 {
		t.Fatalf("surviving member weight = %d, want the disabled member's share redistributed", w)
	}

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	for _, ms := range fleet.Status.Members {
		if ms.Name != "ascend" {
			continue
		}
		// Disabled, NOT Degraded: "someone took this out" and "this one is broken" call for
		// opposite reactions, and the route behind it is healthy.
		if ms.Phase != routingv1.MemberDisabled {
			t.Fatalf("phase = %q, want Disabled", ms.Phase)
		}
		if ms.EffectiveWeight != 0 {
			t.Fatalf("a disabled member reports effectiveWeight %d", ms.EffectiveWeight)
		}
	}
	// It must not count towards the capacity the console shows, either.
	if tc := fleet.Status.TotalCapacity; tc == nil || tc.MaxConcurrency != 16 {
		t.Fatalf("totalCapacity = %+v, want only the live member's 16", tc)
	}
}

// Omitted defaultMember skips disabled members: taking the first member out by hand must not
// quietly make it the fallback for everything.
func TestFleetDefaultMemberSkipsDisabledMembers(t *testing.T) {
	f := fleetFixture()
	f.Spec.DefaultMember = ""
	f.Spec.Members[0].Disabled = true // the one members[0] would otherwise pick
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	def, _, _ := unstructured.NestedString(rule, "config", "decisions", "static", "models", "glm-5.3", "default")
	if def != "ascend" {
		t.Fatalf("table default = %q, want the first member that is not disabled", def)
	}
}

// A disabled member that IS the default stays in the table. The plugin resolves `default` by
// id and rejects the entire table - every model - when it cannot find it, so dropping it
// would turn one operator's drain into a gateway-wide config freeze.
func TestFleetDisabledDefaultMemberStaysInTheTable(t *testing.T) {
	f := fleetFixture()
	f.Spec.Members[0].Disabled = true // h100 is also the stated defaultMember
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
	)
	reconcileFleet(t, r)

	rule := getPlugin(t, c, "ai-backend-router")[0].(map[string]any)
	backends, _, _ := unstructured.NestedSlice(rule, "config", "decisions", "static", "models", "glm-5.3", "backends")
	var seen bool
	for _, b := range backends {
		m := b.(map[string]any)
		if id, _, _ := unstructured.NestedString(m, "id"); id == "h100" {
			seen = true
			if w, _, _ := unstructured.NestedInt64(m, "weight"); w != 0 {
				t.Fatalf("the disabled default carries weight %d, want 0", w)
			}
		}
	}
	if !seen {
		t.Fatal("the disabled default member was dropped - the plugin would reject the whole table")
	}
}

// Disabling every member means "take this model offline", and the only way to mean it is to
// stop publishing the route. The plugin cannot express "serve nobody": with nothing
// selectable it routes to `default` unconditionally - not even enabled:false stops it - so a
// live route would send 100% of traffic to the member that was just drained. On a
// single-member fleet, which is how every single-hardware model is published, that made the
// kill switch do nothing at all.
func TestFleetAllMembersDisabledWithdrawsTheRoute(t *testing.T) {
	f := fleetFixture()
	f.Spec.Members[0].Disabled = true
	f.Spec.Members[1].Disabled = true
	r, c := newFleetReconciler(t, f,
		modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true),
		modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true),
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPlugin("ai-statistics"),
	)
	// First pass deletes the Ingress and requeues; second withdraws the rules.
	reconcileFleet(t, r)
	reconcileFleet(t, r)

	var ing networkingv1.Ingress
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err == nil {
		t.Fatal("the route is still published although every member is disabled")
	}
	for _, p := range []string{"ai-backend-router", "key-auth", "ai-statistics"} {
		if n := len(getPlugin(t, c, p)); n != 0 {
			t.Fatalf("%s still carries %d rule(s) for a withdrawn route", p, n)
		}
	}

	var fleet routingv1.ModelFleet
	nn := types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}
	_ = c.Get(context.Background(), nn, &fleet)
	// Synced is True - the gateway matches what the object asks for - but Ready says why it
	// is not serving, and says it was on purpose.
	if !condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatalf("GatewaySynced=False for a fleet that is exactly as asked: %+v", fleet.Status.Conditions)
	}
	if condTrue(fleet.Status.Conditions, condReady) {
		t.Fatal("Ready=True for a model whose route was withdrawn")
	}
	var readyCond *metav1.Condition
	for i := range fleet.Status.Conditions {
		if fleet.Status.Conditions[i].Type == condReady {
			readyCond = &fleet.Status.Conditions[i]
		}
	}
	// NoReadyMember would send someone hunting for a fault that is not there.
	if readyCond == nil || readyCond.Reason != "AllMembersDisabled" {
		t.Fatalf("Ready reason = %v, want AllMembersDisabled", readyCond)
	}

	// ...and re-enabling one brings the route back.
	_ = c.Get(context.Background(), nn, &fleet)
	fleet.Spec.Members[0].Disabled = false
	if err := c.Update(context.Background(), &fleet); err != nil {
		t.Fatal(err)
	}
	reconcileFleet(t, r)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "llm-fleet-glm-5.3"}, &ing); err != nil {
		t.Fatalf("re-enabling a member did not bring the route back: %v", err)
	}
}

// Two members whose routes land in different ConfigMaps are served by two different
// openrestys, and the Ingress can only point at one of them. Whichever members it does not
// reach 404 on a fleet that reports them healthy. A namespace-only comparison waved this
// through whenever both ConfigMaps happened to live in the fleet's namespace, which is what
// two openresty releases in one namespace look like.
func TestFleetMembersServedByTwoOpenrestysAreRefused(t *testing.T) {
	f := fleetFixture()
	h100 := modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 2, true)
	h100.Spec.Nginx.OutputConfigMap = "openresty-conf"
	ascend := modelRoute("glm-53-ascend", "llm-route", "glm-53-ascend", 1, true)
	ascend.Spec.Nginx.OutputConfigMap = "rel-b-openresty-conf" // same namespace, other openresty
	r, c := newFleetReconciler(t, f, h100, ascend,
		wasmPlugin("ai-backend-router"), keyAuthPlugin(map[string]string{"ops": "sk-ops"}),
		wasmPlugin("ai-statistics"))
	reconcileFleet(t, r)

	var fleet routingv1.ModelFleet
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "glm-5.3"}, &fleet)
	if condTrue(fleet.Status.Conditions, condGatewaySynced) {
		t.Fatal("accepted a fleet whose members are served by two different openrestys")
	}
	var msg string
	for _, c := range fleet.Status.Conditions {
		if c.Type == condGatewaySynced {
			msg = c.Message
		}
	}
	if !strings.Contains(msg, "rel-b-openresty-conf") || !strings.Contains(msg, "one openresty") {
		t.Fatalf("the message does not say what is wrong: %q", msg)
	}
	if n := len(getPlugin(t, c, "ai-backend-router")); n != 0 {
		t.Fatalf("wrote %d rule(s) for a fleet it refused", n)
	}
}

// A route fronted by CART with a fixed ceiling admits that many requests however many
// backends sit behind it, so multiplying the backends out reports capacity openresty will
// refuse to use. With maxConcurrencyFromBackend the cart peer's limit IS the backend sum, so
// there the backends answer.
func TestFleetCapacityUnderCartUsesTheCartCeiling(t *testing.T) {
	mr := modelRoute("glm-53-h100", "llm-route", "glm-53-h100", 4, true)
	mr.Spec.Nginx.Peers = []routingv1.RoutePeer{
		{Use: "cart", MaxConcurrency: 48},
		{Use: "backend", MaxConcurrency: 25},
	}
	if got := routeMaxConcurrency(mr); got != 48 {
		t.Fatalf("capacity = %d, want the cart ceiling 48 (not 4 backends x 25)", got)
	}
	// ...and the other direction, which taking the cart ceiling alone got wrong: a CART that
	// accepts more than the engines behind it can serve does not make the route bigger, it
	// queues the surplus inside CART.
	mr.Spec.Nginx.Peers[0].MaxConcurrency = 200
	if got := routeMaxConcurrency(mr); got != 100 {
		t.Fatalf("capacity = %d, want 4 x 25 = 100: the engines bind, not CART's 200", got)
	}
	mr.Spec.Nginx.Peers[0].MaxConcurrency = 48
	mr.Spec.Nginx.Peers[0].MaxConcurrencyFromBackend = true
	if got := routeMaxConcurrency(mr); got != 100 {
		t.Fatalf("capacity = %d, want 4 backends x 25 = 100 when the cart limit follows them", got)
	}
	// Two backend groups are two ceilings and no single answer; reporting one of them would
	// be a number nobody can trace back.
	mr.Spec.Nginx.Peers = []routingv1.RoutePeer{
		{Use: "backend", MaxConcurrency: 25},
		{Use: "backend", MaxConcurrency: 40},
	}
	if got := routeMaxConcurrency(mr); got != 0 {
		t.Fatalf("capacity = %d, want 0 (unknown) for a route with two backend ceilings", got)
	}
}
