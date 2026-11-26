package controller

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	routingv1 "autoconfig/api/v1alpha1"
)

func consumerFixture(name, secret string) *routingv1.APIConsumer {
	return &routingv1.APIConsumer{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "llm-route", Generation: 1,
			Finalizers: []string{consumerFinalizer},
		},
		Spec: routingv1.APIConsumerSpec{
			Gateway:              routingv1.ConsumerGateway{Namespace: "higress-system"},
			CredentialsSecretRef: secret,
		},
	}
}

func credSecret(name string, keys map[string]string) *corev1.Secret {
	data := map[string][]byte{}
	for k, v := range keys {
		data[k] = []byte(v)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "llm-route"},
		Data:       data,
	}
}

// keyAuthWith builds the gateway's key-auth plugin with an existing consumer list, the way
// the chart leaves it.
func keyAuthWith(consumers ...map[string]any) *unstructured.Unstructured {
	o := wasmPlugin("key-auth")
	list := make([]any, 0, len(consumers))
	for _, c := range consumers {
		list = append(list, c)
	}
	_ = unstructured.SetNestedSlice(o.Object, list, "spec", "defaultConfig", "consumers")
	_ = unstructured.SetNestedField(o.Object, false, "spec", "defaultConfig", "global_auth")
	_ = unstructured.SetNestedStringSlice(o.Object, []string{"x-higress-dummy-key"}, "spec", "defaultConfig", "keys")
	return o
}

// ...and the same key-auth with those entries recorded as this controller's own, which is
// what it looks like after a previous programming round. Entries with no such record are
// somebody else's (the gateway chart's, mid-migration) and are carried over untouched, so a
// test about revoking OUR entry has to say that it is ours.
func keyAuthManagedBy(names string, consumers ...map[string]any) *unstructured.Unstructured {
	o := keyAuthWith(consumers...)
	o.SetAnnotations(map[string]string{managedConsumersAnnotation: names})
	return o
}

func liveConsumer(name string, creds ...string) map[string]any {
	cs := make([]any, 0, len(creds))
	for _, c := range creds {
		cs = append(cs, c)
	}
	return map[string]any{
		"name": name, "credentials": cs, "keys": []any{"Authorization"},
		"in_header": true, "in_query": false,
	}
}

// appliedFields records every server-side-apply this controller made, per object. The fake
// client refuses apply patches outright ("apply patches are not supported in the fake
// client"), so they are translated into the merge patch that has the same effect on the
// stored object and counted here. Without the translation the ownership takeover - the one
// write that MUST be an apply - could not be exercised at all; without the counting, a test
// could not tell "took ownership once" from "re-applies on every pass", which is an ECDS
// push of key-auth to the whole data plane each time.
type appliedFields struct {
	byObject map[string][]string
}

func (a *appliedFields) count(name string) int { return len(a.byObject[name]) }

func newConsumerReconciler(t *testing.T, objs ...client.Object) (*APIConsumerReconciler, client.Client) {
	r, c, _ := newConsumerReconcilerRecording(t, objs...)
	return r, c
}

func newConsumerReconcilerRecording(t *testing.T, objs ...client.Object) (
	*APIConsumerReconciler, client.Client, *appliedFields) {
	t.Helper()
	s := fleetScheme(t)
	applies := &appliedFields{byObject: map[string][]string{}}
	c := ctrlfake.NewClientBuilder().WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&routingv1.APIConsumer{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object,
				patch client.Patch, opts ...client.PatchOption) error {
				if patch.Type() != types.ApplyPatchType {
					return cl.Patch(ctx, obj, patch, opts...)
				}
				data, err := patch.Data(obj)
				if err != nil {
					return err
				}
				key := obj.GetNamespace() + "/" + obj.GetName()
				applies.byObject[key] = append(applies.byObject[key], string(data))
				if err := cl.Patch(ctx, obj, client.RawPatch(types.MergePatchType, data),
					&client.PatchOptions{}); err != nil {
					return err
				}
				// The fake client keeps no managedFields, and the code under test asks
				// whether it already owns a field before claiming it. Without this, that
				// question answers "no" forever and the takeover repeats on every pass -
				// which is exactly what TestConsumerUnchangedListIsNotRewritten caught.
				return recordApplyOwnership(ctx, cl, obj, data, fieldManagerName)
			},
		}).
		Build()
	return &APIConsumerReconciler{Client: c, APIReader: c, Scheme: s}, c, applies
}

func reconcileConsumer(t *testing.T, r *APIConsumerReconciler, name string) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "llm-route", Name: name},
	})
	return err
}

func gatewayConsumers(t *testing.T, c client.Client, ns string) []any {
	t.Helper()
	o := &unstructured.Unstructured{}
	o.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "key-auth"}, o); err != nil {
		t.Fatalf("get key-auth in %s: %v", ns, err)
	}
	list, _, _ := unstructured.NestedSlice(o.Object, "spec", "defaultConfig", "consumers")
	return list
}

func consumerNames(list []any) []string {
	var out []string
	for _, c := range list {
		n, _, _ := unstructured.NestedString(c.(map[string]any), "name")
		out = append(out, n)
	}
	return out
}

// The ordinary case: a consumer with a Secret is programmed into the gateway, in the shape
// key-auth reads, and status says how many keys it has without ever carrying one.
func TestConsumerIsProgrammedIntoTheGateway(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-tenant-a"}),
		keyAuthWith(),
	)
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}

	list := gatewayConsumers(t, c, "higress-system")
	if len(list) != 1 {
		t.Fatalf("consumers = %v, want one", consumerNames(list))
	}
	got := list[0].(map[string]any)
	if n, _, _ := unstructured.NestedString(got, "name"); n != "tenant-a" {
		t.Fatalf("name = %q", n)
	}
	// With the Bearer prefix, because that is what key_auth compares against: it takes the
	// WHOLE Authorization header value and looks it up exactly. A bare key here would 401
	// every standard client while this CR reported success.
	creds, _, _ := unstructured.NestedStringSlice(got, "credentials")
	if len(creds) != 1 || creds[0] != "Bearer sk-tenant-a" {
		t.Fatalf("credentials = %v, want the key with the Bearer prefix", creds)
	}
	// The extraction config the plugin needs, which nothing else in Go checks.
	keys, _, _ := unstructured.NestedStringSlice(got, "keys")
	inHeader, _, _ := unstructured.NestedBool(got, "in_header")
	inQuery, _, _ := unstructured.NestedBool(got, "in_query")
	if len(keys) != 1 || keys[0] != "Authorization" || !inHeader || inQuery {
		t.Fatalf("extraction config = keys:%v in_header:%v in_query:%v", keys, inHeader, inQuery)
	}

	var con routingv1.APIConsumer
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-a"}, &con)
	if !condTrue(con.Status.Conditions, condProgrammed) {
		t.Fatalf("not programmed: %+v", con.Status.Conditions)
	}
	if con.Status.Credentials != 1 || con.Status.ConsumerName != "tenant-a" {
		t.Fatalf("status = %+v", con.Status)
	}
	if con.Status.ObservedGeneration != con.Generation {
		t.Fatalf("observedGeneration = %d, want %d", con.Status.ObservedGeneration, con.Generation)
	}
	// The key itself must not be anywhere in status: the whole point of the Secret is that it
	// lives in one place, and status is readable by anyone who can read the CR.
	if strings.Contains(strings.ToLower(con.Status.ConsumerName+con.Status.AppliedGatewayNamespace), "sk-") {
		t.Fatal("a credential leaked into status")
	}
}

// Every value in the Secret is accepted, which is what makes a rotation possible: add the new
// key, let both work, remove the old one. No window where neither does.
func TestConsumerAcceptsEveryKeyInTheSecret(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("ops", "ops-keys"),
		credSecret("ops-keys", map[string]string{"current": "sk-new", "previous": "sk-old"}),
		keyAuthWith(),
	)
	if err := reconcileConsumer(t, r, "ops"); err != nil {
		t.Fatal(err)
	}
	creds, _, _ := unstructured.NestedStringSlice(gatewayConsumers(t, c, "higress-system")[0].(map[string]any), "credentials")
	if len(creds) != 2 || creds[0] != "Bearer sk-new" || creds[1] != "Bearer sk-old" {
		t.Fatalf("credentials = %v, want both keys with the Bearer prefix, sorted", creds)
	}
}

// THE guard. An unreadable Secret must not quietly drop that consumer from the list: a
// consumer missing from key-auth is a caller that gets 401 on its next request. The gateway
// keeps its last good list and the failure is reported.
func TestConsumerUnreadableSecretDoesNotRevokeAnyone(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		consumerFixture("tenant-b", "missing-secret"), // Secret does not exist
		credSecret("tenant-a-keys", map[string]string{"key": "sk-tenant-a"}),
		keyAuthWith(liveConsumer("tenant-a", "sk-tenant-a"), liveConsumer("tenant-b", "sk-tenant-b")),
	)
	// Reconciling the healthy one must not publish a list that drops the broken one. The
	// round reports the failure on the object and requeues rather than returning an error -
	// returning one would hot-loop on a Secret that may be minutes from existing.
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var healthy routingv1.APIConsumer
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-a"}, &healthy)
	if condTrue(healthy.Status.Conditions, condProgrammed) {
		t.Fatal("the round reported success although one consumer could not be resolved")
	}
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if len(names) != 2 {
		t.Fatalf("the live consumer list was rewritten to %v - someone's key just stopped working", names)
	}
}

// The single most dangerous write this controller can make: an empty consumer list means
// every caller is refused on every route. It can only come from a bug or a half-finished
// migration, never from intent.
func TestConsumerRefusesToEmptyTheGateway(t *testing.T) {
	r, c := newConsumerReconciler(t,
		keyAuthWith(liveConsumer("tenant-a", "sk-a"), liveConsumer("ops", "sk-ops")),
	)
	err := r.programGateway(context.Background(), "higress-system", nil)
	if err == nil {
		t.Fatal("an empty consumer list was written over a live gateway")
	}
	if !strings.Contains(err.Error(), "deny every caller") {
		t.Fatalf("the error does not say what would have happened: %v", err)
	}
	if n := len(gatewayConsumers(t, c, "higress-system")); n != 2 {
		t.Fatalf("live consumers = %d, want both still there", n)
	}
}

// Deleting an APIConsumer has to actually revoke the key, and that happens while the object
// still exists - under its finalizer - because afterwards nothing remembers the credential
// was ever accepted.
func TestConsumerDeleteRevokesTheKey(t *testing.T) {
	now := metav1.Now()
	doomed := consumerFixture("tenant-b", "tenant-b-keys")
	doomed.DeletionTimestamp = &now
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		doomed,
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		credSecret("tenant-b-keys", map[string]string{"key": "sk-b"}),
		keyAuthManagedBy("tenant-a,tenant-b",
			liveConsumer("tenant-a", "sk-a"), liveConsumer("tenant-b", "sk-b")),
	)
	if err := reconcileConsumer(t, r, "tenant-b"); err != nil {
		t.Fatal(err)
	}
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if len(names) != 1 || names[0] != "tenant-a" {
		t.Fatalf("consumers = %v, want only tenant-a", names)
	}
	var con routingv1.APIConsumer
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-b"}, &con)
	if err == nil && len(con.Finalizers) > 0 {
		t.Fatalf("finalizer still on the deleted object: %v", con.Finalizers)
	}
}

// Every write here is an ECDS push of key-auth to the whole data plane, and the console calls
// this on every key edit. An unchanged list must produce no write at all.
func TestConsumerUnchangedListIsNotRewritten(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		keyAuthWith(),
	)
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	before := resourceVersionOf(t, c, "key-auth")
	for i := 0; i < 3; i++ {
		if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
			t.Fatal(err)
		}
	}
	if after := resourceVersionOf(t, c, "key-auth"); after != before {
		t.Fatalf("key-auth rewritten with nothing changed: %s -> %s (an ECDS push per resync)", before, after)
	}
}

// Two objects claiming one consumer name would each render an entry and the plugin would take
// one of them, with no way to tell which. Both names are reported rather than one silently
// winning.
func TestConsumerDuplicateNameIsRefused(t *testing.T) {
	a := consumerFixture("team-a", "a-keys")
	a.Spec.ConsumerName = "shared"
	b := consumerFixture("team-b", "b-keys")
	b.Spec.ConsumerName = "shared"
	r, c := newConsumerReconciler(t, a, b,
		credSecret("a-keys", map[string]string{"key": "sk-a"}),
		credSecret("b-keys", map[string]string{"key": "sk-b"}),
		keyAuthWith(),
	)
	if err := reconcileConsumer(t, r, "team-a"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var first routingv1.APIConsumer
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "team-a"}, &first)
	if condTrue(first.Status.Conditions, condProgrammed) {
		t.Fatal("two objects were allowed to claim one consumer name")
	}
	if !strings.Contains(condMessage(first.Status.Conditions, condProgrammed), "team-b") {
		t.Fatalf("the other claimant is not named: %q", condMessage(first.Status.Conditions, condProgrammed))
	}
	if n := len(gatewayConsumers(t, c, "higress-system")); n != 0 {
		t.Fatalf("something was written anyway: %d consumers", n)
	}
}

// credentialsSecretRef is a bare name on purpose: this controller holds a cluster-wide
// `secrets: get`, so a ref that could name another namespace would turn "can write an
// APIConsumer here" into "can read every Secret in the cluster".
func TestConsumerSecretRefCannotLeaveTheNamespace(t *testing.T) {
	bad := consumerFixture("tenant-a", "kube-system/stolen")
	r, c := newConsumerReconciler(t, bad,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "stolen", Namespace: "kube-system"},
			Data: map[string][]byte{"key": []byte("sk-not-yours")}},
		keyAuthWith(),
	)
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var con routingv1.APIConsumer
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-a"}, &con)
	if condTrue(con.Status.Conditions, condProgrammed) {
		t.Fatal("a cross-namespace Secret ref was accepted")
	}
	for _, item := range gatewayConsumers(t, c, "higress-system") {
		if strings.Contains(strings.ToLower(strings.Join(consumerNames([]any{item}), "")), "tenant-a") {
			t.Fatal("the consumer was programmed from a Secret in another namespace")
		}
	}
}

// A Secret with no usable value is an error, not an entry with an empty credentials list: an
// empty list is accepted by the plugin and then matches nothing, so that tenant gets 401 on
// every request while the CR reports success.
func TestConsumerEmptySecretIsAnError(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "empty-keys"),
		credSecret("empty-keys", map[string]string{"key": "   "}),
		keyAuthWith(),
	)
	_ = reconcileConsumer(t, r, "tenant-a")
	var con routingv1.APIConsumer
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-a"}, &con)
	if condTrue(con.Status.Conditions, condProgrammed) {
		t.Fatal("a consumer with no usable credential was reported as programmed")
	}
	if n := len(gatewayConsumers(t, c, "higress-system")); n != 0 {
		t.Fatalf("an entry was written anyway: %d", n)
	}
}

// Moving gateway.namespace has to take the credentials out of the gateway they used to be in,
// or that gateway keeps accepting them forever with nothing in the object pointing at it.
func TestConsumerMovingGatewayWithdrawsFromTheOldOne(t *testing.T) {
	con := consumerFixture("tenant-a", "tenant-a-keys")
	con.Status.AppliedGatewayNamespace = "higress-old"
	r, c := newConsumerReconciler(t, con,
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		keyAuthWith(),
		keyAuthInNamespace("higress-old", liveConsumer("tenant-a", "sk-a")),
	)
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if n := len(gatewayConsumers(t, c, "higress-system")); n != 1 {
		t.Fatalf("new gateway has %d consumers, want 1", n)
	}
	if n := len(gatewayConsumers(t, c, "higress-old")); n != 0 {
		t.Fatalf("the old gateway still accepts %d consumer(s)", n)
	}
}

// A failed list must never be read as "there are no consumers": that renders an empty list,
// and an empty list locks every caller out of every route.
func TestConsumerListFailureWritesNothing(t *testing.T) {
	s := fleetScheme(t)
	c := ctrlfake.NewClientBuilder().WithScheme(s).
		WithObjects(consumerFixture("tenant-a", "tenant-a-keys"),
			credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
			keyAuthWith(liveConsumer("tenant-a", "sk-a"))).
		WithStatusSubresource(&routingv1.APIConsumer{}).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*routingv1.APIConsumerList); ok {
					return context.DeadlineExceeded
				}
				return cl.List(ctx, list, opts...)
			},
		}).Build()
	r := &APIConsumerReconciler{Client: c, APIReader: c, Scheme: s}

	err := r.programGateway(context.Background(), "higress-system", nil)
	if err == nil {
		t.Fatal("a failed list was treated as an empty consumer set")
	}
	// It has to fail AS a list failure. The empty-list guard would also stop this write, so
	// an error alone proves nothing about the path taken - and that guard does not apply once
	// this controller owns the gateway's list, which is the state every migrated cluster is
	// in. Then only this check stands between a transient API error and a wiped consumer list.
	if !strings.Contains(err.Error(), "list APIConsumers") {
		t.Fatalf("the write was stopped by something other than the failed list: %v", err)
	}
	if n := len(gatewayConsumers(t, c, "higress-system")); n != 1 {
		t.Fatalf("live consumers = %d, want the list untouched", n)
	}
}

// keyAuthInNamespace: a second gateway, for the "moved to another namespace" case.
func keyAuthInNamespace(ns string, consumers ...map[string]any) *unstructured.Unstructured {
	o := keyAuthWith(consumers...)
	o.SetNamespace(ns)
	// Recorded as this controller's own: the consumer being moved was programmed into this
	// gateway by a previous round, which is the only reason withdrawing it is allowed to
	// remove the entry.
	names := make([]string, 0, len(consumers))
	for _, c := range consumers {
		names = append(names, c["name"].(string))
	}
	sort.Strings(names)
	if len(names) > 0 {
		o.SetAnnotations(map[string]string{managedConsumersAnnotation: strings.Join(names, ",")})
	}
	return o
}

// condMessage: a condition's message, for tests that check the failure NAMES what went wrong
// rather than just that something did.
func condMessage(conds []metav1.Condition, typ string) string {
	for i := range conds {
		if conds[i].Type == typ {
			return conds[i].Message
		}
	}
	return ""
}

// The migration case, and the first step the documentation tells people to take: the gateway
// chart renders the tenants, somebody creates ONE APIConsumer. A full replacement of the list
// would revoke everyone else - a 401 each, with nothing but a healthy-looking CR to show for
// it. The empty-list guard does not catch this, because the rendered list is not empty.
func TestConsumerDoesNotRevokeEntriesItDoesNotOwn(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-new", "tenant-new-keys"),
		credSecret("tenant-new-keys", map[string]string{"key": "sk-new"}),
		// Rendered by the chart: no managed-consumers record, so not ours.
		keyAuthWith(liveConsumer("tenant-a", "sk-a"), liveConsumer("tenant-b", "sk-b"),
			liveConsumer("ops", "sk-ops")),
	)
	if err := reconcileConsumer(t, r, "tenant-new"); err != nil {
		t.Fatal(err)
	}
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	want := []string{"ops", "tenant-a", "tenant-b", "tenant-new"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("consumers = %v, want %v - the chart's tenants must survive", names, want)
	}
	// Their credentials have to survive too, not just their names: an entry carried over
	// with an empty credential list is a 403 rather than a 401, which is not an improvement.
	for _, lc := range gatewayConsumers(t, c, "higress-system") {
		m := lc.(map[string]any)
		if creds, _ := m["credentials"].([]any); len(creds) == 0 {
			t.Fatalf("consumer %v was carried over without its credentials", m["name"])
		}
	}
	// ...and only the new one is recorded as ours, so deleting it later revokes exactly it.
	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "higress-system", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	if got := ka.GetAnnotations()[managedConsumersAnnotation]; got != "tenant-new" {
		t.Fatalf("managed-consumers = %q, want just tenant-new", got)
	}
}

// A list this controller cannot account for is not rewritten at all. Carrying entries over
// means reproducing them verbatim, and an entry that is not an object, or has no name, cannot
// be told apart from one of ours - so the safe move is to refuse rather than to guess.
func TestConsumerRefusesAnUnreadableLiveList(t *testing.T) {
	r, _ := newConsumerReconciler(t,
		consumerFixture("tenant-new", "tenant-new-keys"),
		credSecret("tenant-new-keys", map[string]string{"key": "sk-new"}),
		keyAuthWith(map[string]any{"credentials": []any{"sk-x"}}), // no name
	)
	err := r.programGateway(context.Background(), "higress-system", nil)
	if err == nil || !strings.Contains(err.Error(), "no name") {
		t.Fatalf("expected a refusal naming the problem, got %v", err)
	}
}

// Deleting a consumer must not be able to wedge the object. The rewrite that revokes the key
// goes through programGateway, which refuses the whole round if ANY consumer on that gateway
// has an unreadable Secret - so somebody else's broken Secret used to hold this object in
// Terminating for good, freeable only by hand-editing a finalizer.
func TestConsumerDeleteIsNotWedgedByAnotherConsumersSecret(t *testing.T) {
	stale := metav1.NewTime(time.Now().Add(-20 * time.Minute)) // past the revoke deadline
	doomed := consumerFixture("tenant-b", "tenant-b-keys")
	doomed.DeletionTimestamp = &stale
	broken := consumerFixture("tenant-a", "secret-that-does-not-exist")
	r, c := newConsumerReconciler(t, doomed, broken,
		credSecret("tenant-b-keys", map[string]string{"key": "sk-b"}),
		keyAuthManagedBy("tenant-a,tenant-b",
			liveConsumer("tenant-a", "sk-a"), liveConsumer("tenant-b", "sk-b")),
	)
	if err := reconcileConsumer(t, r, "tenant-b"); err != nil {
		t.Fatalf("deletion returned an error instead of letting the object go: %v", err)
	}
	var con routingv1.APIConsumer
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-b"}, &con)
	if err == nil && len(con.Finalizers) > 0 {
		t.Fatalf("object still held by %v after the revoke deadline", con.Finalizers)
	}
}

// ...but before the deadline it keeps trying, and says so. Giving up immediately would make
// "delete" mean "the key may or may not still work", which is worse than a slow delete.
func TestConsumerDeleteKeepsTryingBeforeTheDeadline(t *testing.T) {
	now := metav1.Now()
	doomed := consumerFixture("tenant-b", "tenant-b-keys")
	doomed.DeletionTimestamp = &now
	broken := consumerFixture("tenant-a", "secret-that-does-not-exist")
	r, c := newConsumerReconciler(t, doomed, broken,
		credSecret("tenant-b-keys", map[string]string{"key": "sk-b"}),
		keyAuthManagedBy("tenant-a,tenant-b",
			liveConsumer("tenant-a", "sk-a"), liveConsumer("tenant-b", "sk-b")),
	)
	if err := reconcileConsumer(t, r, "tenant-b"); err != nil {
		t.Fatal(err)
	}
	var con routingv1.APIConsumer
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "llm-route", Name: "tenant-b"}, &con); err != nil {
		t.Fatalf("the object was released although the key was never revoked: %v", err)
	}
	if len(con.Finalizers) == 0 {
		t.Fatal("the finalizer was dropped although the key was never revoked")
	}
	if msg := condMessage(con.Status.Conditions, condProgrammed); !strings.Contains(msg, "still retrying") {
		t.Fatalf("the condition does not say it is still trying: %q", msg)
	}
	// The key is still live, which is the honest state - and the reason the object is held.
	if n := len(consumerNames(gatewayConsumers(t, c, "higress-system"))); n != 2 {
		t.Fatalf("live consumers = %d, want both (nothing was written)", n)
	}
}

// A gateway that is gone, or whose CRD has been uninstalled, needs no rewrite: there is
// nothing left to revoke the key from, so the object goes immediately rather than waiting out
// a deadline for a write that can never succeed.
func TestConsumerDeleteReleasesWhenTheGatewayIsGone(t *testing.T) {
	now := metav1.Now()
	doomed := consumerFixture("tenant-b", "tenant-b-keys")
	doomed.DeletionTimestamp = &now
	r, c := newConsumerReconciler(t, doomed,
		credSecret("tenant-b-keys", map[string]string{"key": "sk-b"}),
	) // no key-auth anywhere
	if err := reconcileConsumer(t, r, "tenant-b"); err != nil {
		t.Fatal(err)
	}
	var con routingv1.APIConsumer
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-b"}, &con)
	if err == nil && len(con.Finalizers) > 0 {
		t.Fatalf("held on a gateway that does not exist: %v", con.Finalizers)
	}
}

// A settled consumer must not be rewritten on every pass. The status write is an update event
// on the object, and this controller watches its own kind with no predicate, so a write that
// always happens - lastSyncTime used to be stamped unconditionally - makes each round enqueue
// the next one. Here that costs an uncached Secret GET for every consumer on the gateway,
// every iteration.
func TestConsumerSettledStatusIsNotRewritten(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		keyAuthWith(),
	)
	nn := types.NamespacedName{Namespace: "llm-route", Name: "tenant-a"}
	version := func() string {
		var con routingv1.APIConsumer
		if err := c.Get(context.Background(), nn, &con); err != nil {
			t.Fatal(err)
		}
		return con.ResourceVersion
	}
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	settled := version()
	for i := 0; i < 3; i++ {
		if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
			t.Fatal(err)
		}
	}
	if v := version(); v != settled {
		t.Fatalf("the object was rewritten with nothing to say (resourceVersion %s -> %s)", settled, v)
	}

	// ...but a real change still gets written.
	var con routingv1.APIConsumer
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	con.Spec.ConsumerName = "renamed"
	con.Generation = 2
	if err := c.Update(context.Background(), &con); err != nil {
		t.Fatal(err)
	}
	before := version()
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if version() == before {
		t.Fatal("a changed consumer did not update its status")
	}

	// ...and so does a stale heartbeat, so that lastSyncTime does not freeze on a settled
	// object and read as a dead controller.
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	stale := metav1.NewTime(time.Now().Add(-2 * consumerResyncTime))
	con.Status.LastSyncTime = &stale
	if err := c.Status().Update(context.Background(), &con); err != nil {
		t.Fatal(err)
	}
	before = version()
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if version() == before {
		t.Fatal("lastSyncTime was left stale: nothing would show the controller is still alive")
	}
}

// recordApplyOwnership writes the managedFields entry a real API server would have recorded
// for this apply: the paths the patch names, under the given manager.
func recordApplyOwnership(ctx context.Context, cl client.Client, obj client.Object,
	patch []byte, manager string) error {
	var body map[string]any
	if err := json.Unmarshal(patch, &body); err != nil {
		return err
	}
	fields := toFieldSet(body)
	delete(fields, "f:apiVersion")
	delete(fields, "f:kind")
	delete(fields, "f:metadata")
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())
	if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
		return err
	}
	mfs := live.GetManagedFields()
	for i := range mfs {
		if mfs[i].Manager == manager && mfs[i].Operation == metav1.ManagedFieldsOperationApply {
			mfs[i].FieldsV1 = &metav1.FieldsV1{Raw: raw}
			live.SetManagedFields(mfs)
			return cl.Update(ctx, live)
		}
	}
	live.SetManagedFields(append(mfs, metav1.ManagedFieldsEntry{
		Manager:    manager,
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: live.GetAPIVersion(),
		FieldsType: "FieldsV1",
		FieldsV1:   &metav1.FieldsV1{Raw: raw},
	}))
	return cl.Update(ctx, live)
}

// toFieldSet turns {"spec":{"defaultConfig":{"consumers":[...]}}} into
// {"f:spec":{"f:defaultConfig":{"f:consumers":{}}}} - the shape managedFields uses, with
// leaves (including whole lists, which are atomic here) as empty objects.
func toFieldSet(v map[string]any) map[string]any {
	out := map[string]any{}
	for k, val := range v {
		if child, ok := val.(map[string]any); ok {
			out["f:"+k] = toFieldSet(child)
			continue
		}
		out["f:"+k] = map[string]any{}
	}
	return out
}

// The takeover. Until a manager other than helm owns defaultConfig.consumers, the gateway
// chart refuses to stop rendering it (modelsphere/modelsphere#90) - and a non-apply Update
// only takes ownership of fields whose VALUE it changes, so adopting the live entries
// unchanged, which is the first thing this controller does on a populated gateway, moves
// nothing. Hence one force apply of just that field, with its current value.
func TestConsumerTakesOwnershipOfTheConsumerList(t *testing.T) {
	r, c, applies := newConsumerReconcilerRecording(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		// The chart's list, owned by helm, with tenant-a already in it: the migration case,
		// where the rendered list matches live and an Update would therefore change nothing.
		keyAuthWith(liveConsumer("tenant-a", "Bearer sk-a"), liveConsumer("ops", "Bearer sk-ops")),
	)
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if n := applies.count("higress-system/key-auth"); n != 1 {
		t.Fatalf("server-side applies = %d, want exactly 1 (the ownership claim)", n)
	}
	// The claim must name ONLY consumers. defaultConfig is tracked field by field, but an
	// apply that named keys or global_auth would take those from helm too, and one that
	// dropped them while owning the parent would leave a key-auth that cannot parse at all
	// - and the plugin is FAIL_CLOSE.
	// Parsed, not grepped: "keys" also appears INSIDE each consumer entry, where it belongs,
	// so a substring check reports a problem that is not there (it did).
	var claim map[string]any
	if err := json.Unmarshal([]byte(applies.byObject["higress-system/key-auth"][0]), &claim); err != nil {
		t.Fatal(err)
	}
	spec, _ := claim["spec"].(map[string]any)
	def, _ := spec["defaultConfig"].(map[string]any)
	if len(spec) != 1 || len(def) != 1 {
		t.Fatalf("the claim names more than defaultConfig.consumers: spec=%v", spec)
	}
	if _, ok := def["consumers"]; !ok {
		t.Fatalf("the claim does not name consumers: %v", def)
	}

	// Nothing else changed: the claim carries the CURRENT value, so the gateway's list is
	// not disturbed by taking ownership of it...
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if strings.Join(names, ",") != "ops,tenant-a" {
		t.Fatalf("consumers = %v, want the live list untouched", names)
	}
	// ...and keys/global_auth are still there, which is what a FAIL_CLOSE plugin needs.
	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "higress-system", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	if keys, found, _ := unstructured.NestedStringSlice(ka.Object, "spec", "defaultConfig", "keys"); !found || len(keys) == 0 {
		t.Fatal("the claim removed defaultConfig.keys: key-auth can no longer parse its config")
	}
	if _, found, _ := unstructured.NestedBool(ka.Object, "spec", "defaultConfig", "global_auth"); !found {
		t.Fatal("the claim removed defaultConfig.global_auth")
	}

	// Claimed once, not once per pass: every apply that changes something is an ECDS push of
	// key-auth to the whole data plane.
	for i := 0; i < 3; i++ {
		if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
			t.Fatal(err)
		}
	}
	if n := applies.count("higress-system/key-auth"); n != 1 {
		t.Fatalf("server-side applies = %d after four passes, want 1: the claim repeats", n)
	}
}
