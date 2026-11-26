package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
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

// THE guard, in its current form. An unreadable Secret must not drop that consumer from the
// list - a consumer missing from key-auth is a caller that gets 401 on its next request - and
// it must not freeze the gateway either, which is what failing the round did: nobody else
// could be added, rotated or revoked until somebody restored a Secret.
//
// So its LIVE entry is carried over verbatim and everyone else's changes go through. The
// alternative the review suggested - fail just that object, i.e. drop it - trades a
// management-plane problem for a data-plane outage: a tenant denied because somebody deleted
// an unrelated Secret.
func TestConsumerUnreadableSecretDoesNotRevokeAnyone(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		consumerFixture("tenant-b", "missing-secret"), // Secret does not exist
		credSecret("tenant-a-keys", map[string]string{"key": "sk-tenant-a"}),
		keyAuthWith(liveConsumer("tenant-a", "sk-tenant-a"), liveConsumer("tenant-b", "sk-tenant-b")),
	)
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// Nobody lost access...
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if strings.Join(names, ",") != "tenant-a,tenant-b" {
		t.Fatalf("live consumers = %v, want both: someone's key just stopped working", names)
	}
	if got := creds0Of(t, c, "higress-system", "tenant-b"); strings.Join(got, ",") != "sk-tenant-b" {
		t.Fatalf("tenant-b's entry = %v, want it untouched", got)
	}
	// ...the healthy one was programmed, so the gateway is not frozen...
	var healthy routingv1.APIConsumer
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-a"}, &healthy)
	if !condTrue(healthy.Status.Conditions, condProgrammed) {
		t.Fatalf("the healthy consumer was blocked by somebody else's Secret: %s",
			consumerCondMessage(healthy.Status.Conditions, condProgrammed))
	}
	// ...and the object whose Secret is missing says so.
	if err := reconcileConsumer(t, r, "tenant-b"); err != nil {
		t.Fatal(err)
	}
	var broken routingv1.APIConsumer
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-b"}, &broken)
	if condTrue(broken.Status.Conditions, condProgrammed) {
		t.Fatal("the consumer with no Secret reports Programmed")
	}
	if msg := consumerCondMessage(broken.Status.Conditions, condProgrammed); !strings.Contains(msg, "keeps working") {
		t.Fatalf("the message does not say its existing key still works: %q", msg)
	}
}

// The single most dangerous write this controller can make: an empty consumer list means
// every caller is refused on every route. It can only come from a bug or a half-finished
// migration, never from intent.
func TestConsumerRefusesToEmptyTheGateway(t *testing.T) {
	r, c := newConsumerReconciler(t,
		keyAuthWith(liveConsumer("tenant-a", "sk-a"), liveConsumer("ops", "sk-ops")),
	)
	_, err := r.programGateway(context.Background(), "higress-system", nil)
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
	if !strings.Contains(consumerCondMessage(first.Status.Conditions, condProgrammed), "team-b") {
		t.Fatalf("the other claimant is not named: %q", consumerCondMessage(first.Status.Conditions, condProgrammed))
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

	_, err := r.programGateway(context.Background(), "higress-system", nil)
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

// consumerCondMessage: a condition's message, for tests that check the failure NAMES what
// went wrong rather than just that something did.
//
// Not called condMessage: the ModelFleet suite declares a helper of that name, and two files
// in one package cannot both declare it. On separate branches each builds fine and whichever
// merged second would fail on main - a collision git never reports as a conflict, because the
// two branches never touch the same file.
func consumerCondMessage(conds []metav1.Condition, typ string) string {
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
	_, err := r.programGateway(context.Background(), "higress-system", nil)
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

// Deleting a consumer is no longer blocked by somebody else's broken Secret at all: that
// consumer's live entry is carried over instead of failing the round, so the deletion goes
// through and revokes exactly its own key.
//
// This replaces a test that asserted the opposite - held under RevokeError until the deadline -
// which was the behaviour the review called out: a deletion that cannot complete is how a
// leaked key stays live.
func TestConsumerDeleteIsNotBlockedByAnotherBrokenSecret(t *testing.T) {
	now := metav1.Now()
	doomed := consumerFixture("tenant-b", "tenant-b-keys")
	doomed.DeletionTimestamp = &now
	doomed.Status.ConsumerName = "tenant-b"
	doomed.Status.AppliedGatewayNamespace = "higress-system"
	broken := consumerFixture("tenant-a", "secret-that-does-not-exist")
	broken.Status.ConsumerName = "tenant-a"
	broken.Status.AppliedGatewayNamespace = "higress-system"
	r, c := newConsumerReconciler(t, doomed, broken,
		credSecret("tenant-b-keys", map[string]string{"key": "sk-b"}),
		keyAuthManagedBy("tenant-a,tenant-b",
			liveConsumer("tenant-a", "Bearer sk-a"), liveConsumer("tenant-b", "Bearer sk-b")),
	)
	if err := reconcileConsumer(t, r, "tenant-b"); err != nil {
		t.Fatal(err)
	}
	// tenant-b's key is gone...
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if strings.Join(names, ",") != "tenant-a" {
		t.Fatalf("consumers = %v, want only tenant-a: the deletion did not revoke", names)
	}
	// ...tenant-a keeps the key it still has, despite its Secret being gone...
	if got := creds0Of(t, c, "higress-system", "tenant-a"); strings.Join(got, ",") != "Bearer sk-a" {
		t.Fatalf("tenant-a credentials = %v, want them preserved", got)
	}
	// ...and the object was released rather than held to the deadline.
	var con routingv1.APIConsumer
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "tenant-b"}, &con)
	if err == nil && len(con.Finalizers) > 0 {
		t.Fatalf("still held: %v", con.Finalizers)
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

// Adopting a consumer whose entry the chart already renders, with the same credential, leaves
// the list byte-identical - so the "nothing changed, do not write" check returns early and the
// entry is never recorded as ours. Deleting that APIConsumer afterwards leaves its entry in
// place as somebody else's and the key keeps working, which is the one thing deleting a
// consumer must never do.
//
// Found on the live migration: three consumers were adopted and the annotation named one.
func TestConsumerAdoptionIsRecordedEvenWhenTheListDoesNotChange(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		consumerFixture("tenant-b", "tenant-b-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		credSecret("tenant-b-keys", map[string]string{"key": "sk-b"}),
		// Exactly what the two APIConsumers render, so no round changes the list at all.
		keyAuthWith(liveConsumer("tenant-a", "Bearer sk-a"), liveConsumer("tenant-b", "Bearer sk-b")),
	)
	for _, n := range []string{"tenant-a", "tenant-b"} {
		if err := reconcileConsumer(t, r, n); err != nil {
			t.Fatal(err)
		}
	}
	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "higress-system", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	if got := ka.GetAnnotations()[managedConsumersAnnotation]; got != "tenant-a,tenant-b" {
		t.Fatalf("managed-consumers = %q, want both: the second adoption was skipped as a no-op", got)
	}

	// And the point of recording it: deleting one revokes its key. deletionTimestamp is
	// immutable on an existing object, so the deletion is made the way the API server makes
	// it - a Delete, which parks the object on its finalizer.
	nn := types.NamespacedName{Namespace: "llm-route", Name: "tenant-b"}
	var con routingv1.APIConsumer
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), &con); err != nil {
		t.Fatal(err)
	}
	if err := reconcileConsumer(t, r, "tenant-b"); err != nil {
		t.Fatal(err)
	}
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if strings.Join(names, ",") != "tenant-a" {
		t.Fatalf("consumers = %v, want only tenant-a: the adopted entry was not revoked", names)
	}
}

// A carried-over entry is copied in verbatim, so it was never compared against the
// credentials this controller renders - and key-auth does not care where a duplicate came
// from: it fails to parse the whole defaultConfig, and the plugin is FAIL_CLOSE, so every
// route on the gateway denies every caller.
//
// Reachable on the documented migration path: a chart entry whose name is not a DNS label
// cannot be adopted under that name (consumerName is pinned to one), so it gets a new
// APIConsumer carrying the SAME key - the whole point being that the caller changes nothing.
func TestConsumerRefusesACredentialSharedWithAForeignEntry(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-shared"}),
		// The chart's entry, under a name no APIConsumer can take, with the same key.
		keyAuthWith(liveConsumer("tenant_a", "Bearer sk-shared")),
	)
	_, err := r.programGateway(context.Background(), "higress-system", nil)
	if err == nil {
		t.Fatal("wrote a list key-auth refuses to parse: every caller on the gateway would be denied")
	}
	for _, want := range []string{"tenant_a", "tenant-a", "every caller on every route"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the error does not mention %s: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "sk-shared") {
		t.Fatalf("the error leaks the credential: %v", err)
	}
	// Nothing was written: the gateway keeps the list it had.
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if strings.Join(names, ",") != "tenant_a" {
		t.Fatalf("consumers = %v, want the live list untouched", names)
	}
}

// status.appliedGatewayNamespace must record the gateway the round actually programmed, not
// the one the spec names when the status write lands. Otherwise a spec edit arriving in
// between makes the next round believe the credential is already where it should be, and it
// is never withdrawn from the gateway that really has it.
func TestConsumerStatusRecordsTheGatewayItProgrammed(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		keyAuthWith(),
		keyAuthInNamespace("higress-other"),
	)
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	nn := types.NamespacedName{Namespace: "llm-route", Name: "tenant-a"}
	var con routingv1.APIConsumer
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	if got := con.Status.AppliedGatewayNamespace; got != "higress-system" {
		t.Fatalf("appliedGatewayNamespace = %q, want higress-system", got)
	}
	// Now move it, and confirm the recorded value is what the NEW round programmed - which
	// is also what makes the withdrawal from the old gateway happen.
	con.Spec.Gateway.Namespace = "higress-other"
	con.Generation = 2
	if err := c.Update(context.Background(), &con); err != nil {
		t.Fatal(err)
	}
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	if got := con.Status.AppliedGatewayNamespace; got != "higress-other" {
		t.Fatalf("appliedGatewayNamespace = %q, want higress-other", got)
	}

	// The failure this guards against is an interleaving inside ONE round - the gateway is
	// programmed, then the spec changes, then the status write lands - which two sequential
	// reconciles cannot reproduce: by the time the second one writes its status, the spec
	// already says what it is about to record either way. So the contract is checked
	// directly: what gets recorded is the gateway that was PROGRAMMED, even when the spec
	// now names another one.
	con.Spec.Gateway.Namespace = "higress-elsewhere"
	if err := c.Update(context.Background(), &con); err != nil {
		t.Fatal(err)
	}
	r.setConsumerStatus(context.Background(), nn, metav1.ConditionTrue, "Programmed",
		"1 credential(s) accepted by the gateway in higress-other", 1, "tenant-a", "higress-other")
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	if got := con.Status.AppliedGatewayNamespace; got != "higress-other" {
		t.Fatalf("appliedGatewayNamespace = %q, want the gateway that was programmed "+
			"(higress-other), not the one the spec now names - otherwise the next round "+
			"never withdraws the credential from where it actually is", got)
	}
	if n := len(consumerNames(gatewayConsumers(t, c, "higress-system"))); n != 0 {
		t.Fatalf("the old gateway still accepts %d consumer(s)", n)
	}
	if n := len(consumerNames(gatewayConsumers(t, c, "higress-other"))); n != 1 {
		t.Fatalf("the new gateway has %d consumer(s), want 1", n)
	}
}

// The annotation on key-auth is the only record for an object that is already gone, but it
// lives on an object this controller does not own: recreate that WasmPlugin and it is lost.
// With only the annotation, deleting an APIConsumer afterwards classifies its own entry as
// somebody else's, carries it over, and releases the object with no error - the revoked
// tenant's key keeps working. Every object that still exists reports its own name in status,
// so that is the second source.
func TestConsumerRevokesWhenTheAnnotationWasLost(t *testing.T) {
	doomed := consumerFixture("tenant-b", "tenant-b-keys")
	doomed.Status.ConsumerName = "tenant-b"
	doomed.Status.AppliedGatewayNamespace = "higress-system"
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		doomed,
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		credSecret("tenant-b-keys", map[string]string{"key": "sk-b"}),
		// Recreated key-auth: the entries are there, the annotation is not.
		keyAuthWith(liveConsumer("tenant-a", "Bearer sk-a"), liveConsumer("tenant-b", "Bearer sk-b")),
	)
	var con routingv1.APIConsumer
	nn := types.NamespacedName{Namespace: "llm-route", Name: "tenant-b"}
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), &con); err != nil {
		t.Fatal(err)
	}
	if err := reconcileConsumer(t, r, "tenant-b"); err != nil {
		t.Fatal(err)
	}
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if strings.Join(names, ",") != "tenant-a" {
		t.Fatalf("consumers = %v, want only tenant-a: the deleted tenant's key still works", names)
	}
}

// Deleting a consumer that was never programmed, on a gateway whose list still belongs to the
// chart, must not sit in Terminating waiting out the revoke deadline: rendering zero consumers
// over a non-empty list is refused by the empty-list guard on every pass, and it would report
// that a key could not be withdrawn when none was ever granted.
func TestConsumerDeleteOfANeverProgrammedObjectIsImmediate(t *testing.T) {
	broken := consumerFixture("tenant-new", "secret-that-does-not-exist")
	r, c := newConsumerReconciler(t, broken,
		// The chart's list: non-empty, and nothing of ours in it.
		keyAuthWith(liveConsumer("chart-a", "Bearer sk-chart-a")),
	)
	var con routingv1.APIConsumer
	nn := types.NamespacedName{Namespace: "llm-route", Name: "tenant-new"}
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), &con); err != nil {
		t.Fatal(err)
	}
	if err := reconcileConsumer(t, r, "tenant-new"); err != nil {
		t.Fatal(err)
	}
	err := c.Get(context.Background(), nn, &con)
	if err == nil && len(con.Finalizers) > 0 {
		t.Fatalf("held in Terminating with nothing to revoke: %v", con.Finalizers)
	}
	// ...and the chart's list is untouched.
	if n := len(consumerNames(gatewayConsumers(t, c, "higress-system"))); n != 1 {
		t.Fatalf("the chart's list has %d entries, want 1", n)
	}
}

// Taking over a consumer NAME that already exists in the gateway and belongs to nobody must
// require proof that the writer holds the key it already accepts. An APIConsumer can be
// created in any namespace with any consumerName, and whatever it renders replaces the entry
// - so without this, putting consumerName: tenant-a and your own key in your own Secret
// inherits every route tenant-a is allowed on, and the real tenant-a gets 401. Neither
// existing guard fires: the names do not collide between APIConsumers, and no credential is
// shared.
func TestConsumerCannotTakeOverANameWithoutItsKey(t *testing.T) {
	r, c := newConsumerReconciler(t,
		// Created in some other namespace, claiming the chart's consumer name.
		func() client.Object {
			o := consumerFixture("team-x-impostor", "team-x-keys")
			o.Namespace = "team-x"
			o.Spec.ConsumerName = "tenant-a"
			return o
		}(),
		func() client.Object {
			s := credSecret("team-x-keys", map[string]string{"key": "sk-attacker"})
			s.Namespace = "team-x"
			return s
		}(),
		// The chart's entry, with the real key, managed by nobody.
		keyAuthWith(liveConsumer("tenant-a", "Bearer sk-the-real-key")),
	)
	// The object is left out of the gateway rather than failing the round for everyone, and
	// the reason lands on its own status - so this is checked through a reconcile, not
	// through programGateway's return.
	if err := reconcileConsumer2(t, r, "team-x", "team-x-impostor"); err != nil {
		t.Fatal(err)
	}
	var imp0 routingv1.APIConsumer
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "team-x", Name: "team-x-impostor"}, &imp0); err != nil {
		t.Fatal(err)
	}
	if condTrue(imp0.Status.Conditions, condProgrammed) {
		t.Fatal("a foreign consumer name was taken over with a different key")
	}
	msg0 := consumerCondMessage(imp0.Status.Conditions, condProgrammed)
	for _, want := range []string{"tenant-a", "existing", "consumerName"} {
		if !strings.Contains(msg0, want) {
			t.Fatalf("the message does not explain the refusal (%s missing): %q", want, msg0)
		}
	}
	if strings.Contains(msg0, "sk-the-real-key") || strings.Contains(msg0, "sk-attacker") {
		t.Fatalf("the message leaks a credential: %q", msg0)
	}
	// The real key still works: nothing was written.
	creds := creds0Of(t, c, "higress-system", "tenant-a")
	if strings.Join(creds, ",") != "Bearer sk-the-real-key" {
		t.Fatalf("live credentials = %v, want the original untouched", creds)
	}

	// ...and carrying the existing key DOES adopt it, which is what a migration does.
	sec := credSecret("team-x-keys", map[string]string{"key": "sk-the-real-key"})
	sec.Namespace = "team-x"
	if err := c.Update(context.Background(), sec); err != nil {
		t.Fatal(err)
	}
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatalf("adoption with the existing key was refused: %v", err)
	}
	if got := creds0Of(t, c, "higress-system", "tenant-a"); strings.Join(got, ",") != "Bearer sk-the-real-key" {
		t.Fatalf("after adoption the credentials are %v", got)
	}
}

// A fresh object that has never been programmed must not stop every OTHER consumer on the
// gateway from being added, rotated or revoked. Fail-closed is for a consumer that has a key
// in the list and would lose it; one that has nothing in there loses nothing by being skipped,
// and holding the whole gateway hostage blocks exactly the operation you need in a hurry -
// removing a leaked key.
func TestConsumerBrokenNewObjectDoesNotFreezeTheGateway(t *testing.T) {
	broken := consumerFixture("tenant-new", "secret-that-does-not-exist")
	existing := consumerFixture("tenant-a", "tenant-a-keys")
	existing.Status.ConsumerName = "tenant-a"
	existing.Status.AppliedGatewayNamespace = "higress-system"
	r, c := newConsumerReconciler(t, broken, existing,
		credSecret("tenant-a-keys", map[string]string{"key": "sk-rotated"}),
		keyAuthManagedBy("tenant-a", liveConsumer("tenant-a", "Bearer sk-old")),
	)
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatalf("one never-programmed object blocked the whole round: %v", err)
	}
	// tenant-a's rotation went through...
	if got := creds0Of(t, c, "higress-system", "tenant-a"); strings.Join(got, ",") != "Bearer sk-rotated" {
		t.Fatalf("tenant-a credentials = %v, want the rotated key", got)
	}
	// ...and the broken one is simply not in the list.
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if strings.Join(names, ",") != "tenant-a" {
		t.Fatalf("consumers = %v, want only tenant-a", names)
	}

	// And a consumer that IS in the list and whose Secret breaks does not fail the round
	// either: its live entry is preserved, so it keeps working and nobody else is blocked.
	// Dropping it would revoke a working key; failing the round would freeze the gateway.
	if err := c.Delete(context.Background(), credSecret("tenant-a-keys", map[string]string{"key": "x"})); err != nil {
		t.Fatal(err)
	}
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatalf("an unreadable Secret for a live consumer froze the round: %v", err)
	}
	if got := creds0Of(t, c, "higress-system", "tenant-a"); strings.Join(got, ",") != "Bearer sk-rotated" {
		t.Fatalf("tenant-a credentials = %v, want the entry preserved as it was", got)
	}
}

// creds0Of: one live consumer's credentials.
func creds0Of(t *testing.T, c client.Client, gwNS, name string) []string {
	t.Helper()
	for _, lc := range gatewayConsumers(t, c, gwNS) {
		m := lc.(map[string]any)
		if n, _ := m["name"].(string); n == name {
			raw, _ := m["credentials"].([]any)
			out := make([]string, 0, len(raw))
			for _, v := range raw {
				out = append(out, v.(string))
			}
			return out
		}
	}
	return nil
}

// Revoking the LAST consumer on a gateway, when no status claims that gateway any more.
//
// It happens when the gateway write succeeded and the status write that follows it did not -
// the key is live with no record on the object - and that object is then deleted. No status
// names the gateway, so "has this controller ever owned this list" cannot be answered from
// statuses alone, and the empty-list guard refuses: the finalizer gives up at the revoke
// deadline and the key stays live, with no APIConsumer left to ever clean it up. The
// annotation on key-auth knows better, and emptying a list that it says is ours IS the
// revocation.
func TestConsumerRevokesTheLastConsumerWithoutAStatus(t *testing.T) {
	doomed := consumerFixture("tenant-a", "tenant-a-keys")
	// Programmed, then the status write failed: no appliedGatewayNamespace.
	doomed.Status.AppliedGatewayNamespace = ""
	r, c := newConsumerReconciler(t, doomed,
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		// ...but the gateway's own annotation records the entry as ours.
		keyAuthManagedBy("tenant-a", liveConsumer("tenant-a", "Bearer sk-a")),
	)
	var con routingv1.APIConsumer
	nn := types.NamespacedName{Namespace: "llm-route", Name: "tenant-a"}
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), &con); err != nil {
		t.Fatal(err)
	}
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if n := len(consumerNames(gatewayConsumers(t, c, "higress-system"))); n != 0 {
		t.Fatalf("the gateway still accepts %d consumer(s): the key was never revoked", n)
	}
	if err := c.Get(context.Background(), nn, &con); err == nil && len(con.Finalizers) > 0 {
		t.Fatalf("still held: %v", con.Finalizers)
	}
}

// keyAuthWithoutConsumers: what the gateway chart creates now that it no longer renders the
// field - a key-auth with keys and global_auth and NO consumers key at all.
func keyAuthWithoutConsumers() *unstructured.Unstructured {
	o := wasmPlugin("key-auth")
	_ = unstructured.SetNestedField(o.Object, false, "spec", "defaultConfig", "global_auth")
	_ = unstructured.SetNestedStringSlice(o.Object, []string{"x-higress-dummy-key"},
		"spec", "defaultConfig", "keys")
	return o
}

// The gateway chart stopped rendering defaultConfig.consumers, so this controller is the only
// thing that can create it - and key_auth's parsePluginConfig returns false when neither
// consumers nor credentials is present, with the plugin FAIL_CLOSE: the gateway refuses every
// request on every route until the key exists.
//
// The hard part is the trigger. A fresh install has ZERO APIConsumers, so the APIConsumer
// watch never fires; the key-auth watch does, and the event also says which namespace, so
// nothing has to be configured or guessed.
func TestKeyAuthWithoutConsumersGetsAnEmptyList(t *testing.T) {
	r, c := newConsumerReconciler(t, keyAuthWithoutConsumers()) // no APIConsumer anywhere
	kr := &KeyAuthReconciler{APIConsumerReconciler: r}
	if _, err := kr.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "higress-system", Name: "key-auth"},
	}); err != nil {
		t.Fatal(err)
	}
	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "higress-system", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	live, found, err := unstructured.NestedSlice(ka.Object, "spec", "defaultConfig", "consumers")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("consumers is still absent: key_auth cannot parse this config and the " +
			"gateway refuses every request (FAIL_CLOSE)")
	}
	if len(live) != 0 {
		t.Fatalf("consumers = %v, want an empty list", live)
	}
	// The rest of defaultConfig is untouched - writing the key must not cost the others.
	if keys, ok, _ := unstructured.NestedStringSlice(ka.Object, "spec", "defaultConfig", "keys"); !ok || len(keys) != 1 {
		t.Fatal("defaultConfig.keys was lost")
	}
	if _, ok, _ := unstructured.NestedBool(ka.Object, "spec", "defaultConfig", "global_auth"); !ok {
		t.Fatal("defaultConfig.global_auth was lost")
	}
}

// key-auth deleted and recreated takes every consumer entry with it. The objects still exist,
// so their own resync would restore them within the interval, but the watch makes it the next
// event instead - and the same path covers it, so there is no second way to write this field.
func TestKeyAuthRecreatedRestoresTheConsumers(t *testing.T) {
	existing := consumerFixture("tenant-a", "tenant-a-keys")
	existing.Status.ConsumerName = "tenant-a"
	existing.Status.AppliedGatewayNamespace = "higress-system"
	r, c := newConsumerReconciler(t, existing,
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		keyAuthWithoutConsumers(), // as helm just recreated it: no entries at all
	)
	kr := &KeyAuthReconciler{APIConsumerReconciler: r}
	if _, err := kr.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "higress-system", Name: "key-auth"},
	}); err != nil {
		t.Fatal(err)
	}
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if strings.Join(names, ",") != "tenant-a" {
		t.Fatalf("consumers = %v, want tenant-a restored after the recreation", names)
	}
	if got := creds0Of(t, c, "higress-system", "tenant-a"); strings.Join(got, ",") != "Bearer sk-a" {
		t.Fatalf("credentials = %v", got)
	}
}

// A key-auth that is gone again between the event and the round is not an error to retry.
func TestKeyAuthEventForAMissingGatewayIsNotAnError(t *testing.T) {
	r, _ := newConsumerReconciler(t)
	kr := &KeyAuthReconciler{APIConsumerReconciler: r}
	res, err := kr.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "higress-gone", Name: "key-auth"},
	})
	if err != nil {
		t.Fatalf("a deleted gateway was reported as an error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("requeued after %v for a gateway that does not exist", res.RequeueAfter)
	}
}

// One new object that collides must not freeze consumer management for the whole gateway.
// RenderConsumers refuses a colliding view set, and letting that error out of programGateway
// meant nobody on the gateway could be added, rotated or REVOKED - including pulling a leaked
// key - and a deletion in flight would retry to the revoke deadline and then release with the
// key still live.
func TestConsumerCollidingNewcomerDoesNotFreezeTheGateway(t *testing.T) {
	serving := consumerFixture("tenant-a", "tenant-a-keys")
	serving.Status.ConsumerName = "tenant-a"
	serving.Status.AppliedGatewayNamespace = "higress-system"
	// A newcomer in its own namespace, claiming the name that tenant-a is serving.
	impostor := consumerFixture("team-x", "team-x-keys")
	impostor.Namespace = "team-x"
	impostor.Spec.ConsumerName = "tenant-a"
	xsec := credSecret("team-x-keys", map[string]string{"key": "sk-x"})
	xsec.Namespace = "team-x"
	r, c := newConsumerReconciler(t, serving, impostor, xsec,
		credSecret("tenant-a-keys", map[string]string{"key": "sk-rotated"}),
		keyAuthManagedBy("tenant-a", liveConsumer("tenant-a", "Bearer sk-old")),
	)
	// tenant-a's rotation must go through despite the collision...
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatalf("the colliding newcomer froze the round: %v", err)
	}
	if got := creds0Of(t, c, "higress-system", "tenant-a"); strings.Join(got, ",") != "Bearer sk-rotated" {
		t.Fatalf("tenant-a credentials = %v, want the rotated key", got)
	}
	// ...and the newcomer learns why it is not in the gateway, on its own object.
	if err := reconcileConsumer2(t, r, "team-x", "team-x"); err != nil {
		t.Fatal(err)
	}
	var imp routingv1.APIConsumer
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "team-x", Name: "team-x"}, &imp); err != nil {
		t.Fatal(err)
	}
	if condTrue(imp.Status.Conditions, condProgrammed) {
		t.Fatal("the newcomer reports Programmed although it was left out")
	}
	msg := consumerCondMessage(imp.Status.Conditions, condProgrammed)
	for _, want := range []string{"tenant-a", "llm-route/tenant-a", "left out"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("the message does not say what happened (%s missing): %q", want, msg)
		}
	}
	// The impostor's key is not in the gateway.
	if got := creds0Of(t, c, "higress-system", "tenant-a"); strings.Contains(strings.Join(got, ","), "sk-x") {
		t.Fatalf("the newcomer's key reached the gateway: %v", got)
	}
}

// Two claimants and nobody serving the name yet: neither gets it. Letting the first win would
// arbitrate "who is tenant-a" by list order, and nobody loses access by refusing, because
// nobody had the name.
func TestConsumerTwoNewcomersGetNeitherTheName(t *testing.T) {
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
		t.Fatal(err)
	}
	if n := len(gatewayConsumers(t, c, "higress-system")); n != 0 {
		t.Fatalf("%d consumer(s) written: one of two disputed claims was granted", n)
	}
	var first routingv1.APIConsumer
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "llm-route", Name: "team-a"}, &first)
	if condTrue(first.Status.Conditions, condProgrammed) {
		t.Fatal("a disputed name was granted")
	}
	if msg := consumerCondMessage(first.Status.Conditions, condProgrammed); !strings.Contains(msg, "team-b") {
		t.Fatalf("the other claimant is not named: %q", msg)
	}
}

// A live entry whose credential is stored WITHOUT the Bearer prefix must still be adoptable by
// whoever holds that key. The rendered side is always prefixed, so comparing verbatim made the
// proof impossible - no Secret content could satisfy it - while the refusal told the operator
// to add a key they had already added.
func TestConsumerAdoptsAnEntryStoredWithoutThePrefix(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-bare"}),
		// Written by hand, or by something that did not add the prefix.
		keyAuthWith(liveConsumer("tenant-a", "sk-bare")),
	)
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatalf("adoption refused although the Secret holds that very key: %v", err)
	}
	// Adopted, and now rendered in the form key_auth actually compares against.
	if got := creds0Of(t, c, "higress-system", "tenant-a"); strings.Join(got, ",") != "Bearer sk-bare" {
		t.Fatalf("credentials = %v, want the prefixed form", got)
	}
}

// reconcileConsumer2: reconcile an object outside the default namespace.
func reconcileConsumer2(t *testing.T, r *APIConsumerReconciler, ns, name string) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	})
	return err
}

// The same value under two Secret keys is ordinary mid-rotation: copy `current` into `next`,
// then edit it. It used to arrive as one consumer carrying a credential twice, which the
// collision check read as two claimants on one credential - both from this very object - and
// refused the round for the whole gateway, with a message about "two consumers" naming one
// object twice.
func TestConsumerSecretWithADuplicatedValueDoesNotFreezeTheGateway(t *testing.T) {
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{
			"current": "sk-a",
			"next":    "sk-a", // copied, not yet edited
		}),
		keyAuthWith(),
	)
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatalf("a Secret holding one value twice froze the round: %v", err)
	}
	got := creds0Of(t, c, "higress-system", "tenant-a")
	if strings.Join(got, ",") != "Bearer sk-a" {
		t.Fatalf("credentials = %v, want the value once", got)
	}
}

// One consumer storing `sk-x` and another `Bearer sk-x` are the same credential once rendered,
// and key-auth refuses a duplicate by failing to parse the whole config. Comparing raw values
// let that pair past the collision check and into RenderConsumers, which refuses the round
// instead of leaving the newcomer out.
func TestConsumerPrefixedAndBareDuplicateIsCaughtAsACollision(t *testing.T) {
	serving := consumerFixture("tenant-a", "tenant-a-keys")
	serving.Status.ConsumerName = "tenant-a"
	serving.Status.AppliedGatewayNamespace = "higress-system"
	newcomer := consumerFixture("tenant-b", "tenant-b-keys")
	r, c := newConsumerReconciler(t, serving, newcomer,
		credSecret("tenant-a-keys", map[string]string{"key": "sk-shared"}),
		// The same key, spelled the other way.
		credSecret("tenant-b-keys", map[string]string{"key": "Bearer sk-shared"}),
		keyAuthManagedBy("tenant-a", liveConsumer("tenant-a", "Bearer sk-shared")),
	)
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatalf("the round failed instead of leaving the newcomer out: %v", err)
	}
	names := consumerNames(gatewayConsumers(t, c, "higress-system"))
	if strings.Join(names, ",") != "tenant-a" {
		t.Fatalf("consumers = %v, want only the incumbent: a duplicate credential would make "+
			"key-auth reject the whole configuration", names)
	}
}

// An object that wants a name it cannot prove it holds is left out, not allowed to freeze the
// gateway: everybody else must still be addable, rotatable and revocable.
func TestConsumerUnprovenAdoptionDoesNotFreezeTheGateway(t *testing.T) {
	serving := consumerFixture("tenant-a", "tenant-a-keys")
	serving.Status.ConsumerName = "tenant-a"
	serving.Status.AppliedGatewayNamespace = "higress-system"
	impostor := consumerFixture("team-x", "team-x-keys")
	impostor.Namespace = "team-x"
	impostor.Spec.ConsumerName = "orphan" // a live entry owned by no APIConsumer
	xsec := credSecret("team-x-keys", map[string]string{"key": "sk-x"})
	xsec.Namespace = "team-x"
	r, c := newConsumerReconciler(t, serving, impostor, xsec,
		credSecret("tenant-a-keys", map[string]string{"key": "sk-rotated"}),
		keyAuthManagedBy("tenant-a",
			liveConsumer("tenant-a", "Bearer sk-old"),
			liveConsumer("orphan", "Bearer sk-orphan")),
	)
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatalf("an unprovable adoption froze the round: %v", err)
	}
	// tenant-a's rotation went through...
	if got := creds0Of(t, c, "higress-system", "tenant-a"); strings.Join(got, ",") != "Bearer sk-rotated" {
		t.Fatalf("tenant-a credentials = %v, want the rotated key", got)
	}
	// ...the orphan entry is untouched, and the impostor's key is not in the gateway.
	if got := creds0Of(t, c, "higress-system", "orphan"); strings.Join(got, ",") != "Bearer sk-orphan" {
		t.Fatalf("orphan credentials = %v, want them untouched", got)
	}
}

// The case the gate on its own leaves broken: autoconfig starts in a cluster with no
// WasmPlugin kind (Higress's CRDs come from the higress CORE release, and the helmfile
// deploys autoconfig before higress-llm, which needs it), so the watch is correctly skipped -
// and then nothing would ever write key-auth's consumers key. The gateway would be FAIL_CLOSE
// and stay that way until somebody restarted autoconfig, which is worse than the crash-loop
// the gate prevents, because nothing announces it.
//
// The sweep closes it: no informer, so it runs whether or not the kind exists, and a Higress
// installed later is picked up on the next round.
func TestGatewaySweepWritesTheKeyWithoutAWatch(t *testing.T) {
	r, c := newConsumerReconciler(t, keyAuthWithoutConsumers()) // zero APIConsumers
	r.ensureGateways(context.Background())

	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "higress-system", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	live, found, err := unstructured.NestedSlice(ka.Object, "spec", "defaultConfig", "consumers")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("the sweep did not create the consumers key: on a cluster where the watch " +
			"could not be registered, the gateway would stay FAIL_CLOSE until a restart")
	}
	if len(live) != 0 {
		t.Fatalf("consumers = %v, want an empty list", live)
	}
}

// A cluster with no Higress at all: the sweep must be a no-op, not an error and not a log
// line every minute. isAbsent covers both a missing CRD and a missing object.
func TestGatewaySweepIsQuietWithoutHigress(t *testing.T) {
	s := runtime.NewScheme()
	if err := routingv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	// A client whose scheme has no WasmPlugin kind stands in for a cluster without Higress.
	c := ctrlfake.NewClientBuilder().WithScheme(s).Build()
	r := &APIConsumerReconciler{Client: c, APIReader: c, Scheme: s}
	r.ensureGateways(context.Background()) // must not panic, must not block
}

// Every read of key-auth has to avoid the client's cache. The cached client builds an informer
// on first use of a kind, and this controller runs where the WasmPlugin kind does not exist
// yet - which is exactly why the watch is gated on discovery. A cached read here would
// reintroduce that dependency one layer down, where nothing would notice until a cluster
// without Higress fell over.
//
// Checked by giving the reconciler a client that fails every read and an APIReader that
// works: anything that reaches for the cache instead shows up as a failure.
func TestKeyAuthReadsBypassTheCache(t *testing.T) {
	working := ctrlfake.NewClientBuilder().WithScheme(fleetScheme(t)).
		WithObjects(keyAuthWithoutConsumers()).
		WithStatusSubresource(&routingv1.APIConsumer{}).
		Build()
	r := &APIConsumerReconciler{
		Client:    &readRefusingClient{Client: working},
		APIReader: working,
		Scheme:    fleetScheme(t),
	}
	// The sweep lists and then programs, so this covers both.
	r.ensureGateways(context.Background())

	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	if err := working.Get(context.Background(), types.NamespacedName{
		Namespace: "higress-system", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedSlice(ka.Object, "spec", "defaultConfig", "consumers"); !found {
		t.Fatal("the consumers key was not written: something read key-auth through the cache, " +
			"which on a cluster without the WasmPlugin kind is the dependency the discovery " +
			"gate exists to avoid")
	}
}

// readRefusingClient fails reads of WasmPlugin only - the kind that may not exist - and
// passes everything else through. Refusing every read would prove nothing: APIConsumer and
// Secret reads are allowed to use the cache, since this controller is only registered where
// the APIConsumer CRD is served. The first version of this refused everything and failed for
// that reason, which looked exactly like the bug it was written to find.
type readRefusingClient struct {
	client.Client
}

func (c *readRefusingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object,
	opts ...client.GetOption) error {
	if isWasmPlugin(obj) {
		return fmt.Errorf("read key-auth through the cache: %s", key)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *readRefusingClient) List(ctx context.Context, list client.ObjectList,
	opts ...client.ListOption) error {
	if u, ok := list.(*unstructured.UnstructuredList); ok &&
		strings.Contains(u.GetObjectKind().GroupVersionKind().Kind, "WasmPlugin") {
		return fmt.Errorf("list WasmPlugins through the cache")
	}
	return c.Client.List(ctx, list, opts...)
}

func isWasmPlugin(obj client.Object) bool {
	u, ok := obj.(*unstructured.Unstructured)
	return ok && u.GetObjectKind().GroupVersionKind() == wasmPluginGVK
}

// The sweep must not write to a key-auth in a namespace nothing in this cluster points at.
// Adding `consumers: []` there means acting on an object nobody pointed this controller at -
// another team's Higress in their own namespace - and taking SSA ownership of a field on it.
func TestGatewaySweepLeavesForeignGatewaysAlone(t *testing.T) {
	foreign := keyAuthWithoutConsumers()
	foreign.SetNamespace("someone-elses-higress")
	r, c := newConsumerReconciler(t, foreign)
	r.ensureGateways(context.Background())

	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "someone-elses-higress", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedSlice(ka.Object, "spec", "defaultConfig", "consumers"); found {
		t.Fatal("wrote to a gateway nothing in this cluster points at")
	}

	// ...but it does act once something names it.
	cons := consumerFixture("tenant-a", "tenant-a-keys")
	cons.Spec.Gateway.Namespace = "someone-elses-higress"
	if err := c.Create(context.Background(), cons); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), credSecret("tenant-a-keys",
		map[string]string{"key": "sk-a"})); err != nil {
		t.Fatal(err)
	}
	r.ensureGateways(context.Background())
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "someone-elses-higress", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedSlice(ka.Object, "spec", "defaultConfig", "consumers"); !found {
		t.Fatal("did not write to a gateway an APIConsumer names")
	}
}

// A key-auth configured the OTHER way key_auth allows - a top-level `credentials` list and no
// consumers - must not have `consumers` added to it: the plugin refuses a config carrying
// both, so the write would take that gateway down. And such a gateway is, by definition, not
// one autoconfig set up.
func TestConsumerRefusesAKeyAuthUsingTopLevelCredentials(t *testing.T) {
	other := wasmPlugin("key-auth")
	_ = unstructured.SetNestedStringSlice(other.Object, []string{"sk-theirs"},
		"spec", "defaultConfig", "credentials")
	_ = unstructured.SetNestedStringSlice(other.Object, []string{"x-api-key"},
		"spec", "defaultConfig", "keys")
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		other,
	)
	_, err := r.programGateway(context.Background(), "higress-system", nil)
	if err == nil {
		t.Fatal("wrote consumers into a key-auth that uses top-level credentials: the plugin " +
			"refuses a config carrying both, so that gateway would stop answering")
	}
	if !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("the error does not explain what is wrong: %v", err)
	}
	// Their config is untouched.
	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "higress-system", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedSlice(ka.Object, "spec", "defaultConfig", "consumers"); found {
		t.Fatal("consumers was added anyway")
	}
}

// The label is what makes a gateway chart installed OUTSIDE the default namespace work before
// any fleet or consumer exists - the fresh-install case the sweep exists for. Without it such
// a gateway would sit FAIL_CLOSE until somebody created the first object.
func TestGatewaySweepWritesToALabelledGatewayAnywhere(t *testing.T) {
	ka := keyAuthWithoutConsumers()
	ka.SetNamespace("gateway-elsewhere")
	ka.SetLabels(map[string]string{consumersLabel: "autoconfig"})
	r, c := newConsumerReconciler(t, ka) // no APIConsumer, no fleet, not the default namespace
	r.ensureGateways(context.Background())

	var got unstructured.Unstructured
	got.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "gateway-elsewhere", Name: "key-auth"}, &got); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedSlice(got.Object, "spec", "defaultConfig", "consumers"); !found {
		t.Fatal("the labelled gateway did not get its consumers key: a chart installed outside " +
			"higress-system would stay FAIL_CLOSE until the first APIConsumer or fleet")
	}

	// ...and the label has to say autoconfig. Somebody else's label value is not consent.
	other := keyAuthWithoutConsumers()
	other.SetNamespace("gateway-other")
	other.SetLabels(map[string]string{consumersLabel: "somebody-else"})
	if err := c.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	r.ensureGateways(context.Background())
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "gateway-other", Name: "key-auth"}, &got); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedSlice(got.Object, "spec", "defaultConfig", "consumers"); found {
		t.Fatal("wrote to a gateway whose label names a different manager")
	}
}

// The scope check has to hold on EVERY path into a write, not just the sweep. It was in the
// sweep only, and the key-auth watch - which fires for every key-auth in the cluster - walked
// straight past it: a probe on the test cluster found autoconfig writing to a gateway labelled
// for another manager and to an unlabelled one in a namespace nothing names. No unit test
// covered the watch path with a foreign gateway, so nothing here said a word.
func TestKeyAuthWatchRespectsTheGatewayScope(t *testing.T) {
	foreign := keyAuthWithoutConsumers()
	foreign.SetNamespace("someone-elses-higress")
	theirs := keyAuthWithoutConsumers()
	theirs.SetNamespace("labelled-for-someone-else")
	theirs.SetLabels(map[string]string{consumersLabel: "somebody-else"})
	r, c := newConsumerReconciler(t, foreign, theirs)
	kr := &KeyAuthReconciler{APIConsumerReconciler: r}

	for _, ns := range []string{"someone-elses-higress", "labelled-for-someone-else"} {
		if _, err := kr.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: "key-auth"},
		}); err != nil {
			t.Fatalf("%s: %v", ns, err)
		}
		var ka unstructured.Unstructured
		ka.SetGroupVersionKind(wasmPluginGVK)
		if err := c.Get(context.Background(), types.NamespacedName{
			Namespace: ns, Name: "key-auth"}, &ka); err != nil {
			t.Fatal(err)
		}
		if _, found, _ := unstructured.NestedSlice(ka.Object, "spec", "defaultConfig", "consumers"); found {
			t.Fatalf("%s: the watch wrote to a gateway this controller has no claim on", ns)
		}
	}
}

// A gateway this controller does not manage must not come back as success. Returning no error
// and no reason made Reconcile indistinguishable from a write that happened: it set
// Programmed=True and status.credentials for a consumer whose key was never programmed, so
// the object said "N credential(s) accepted" while every request with that key got 401.
//
// The false status.appliedGatewayNamespace was the worse half - it feeds claimed[], which
// skips the adoption-proof check later.
func TestConsumerForeignGatewayIsNotReportedAsProgrammed(t *testing.T) {
	ka := keyAuthWith(liveConsumer("chart-a", "Bearer sk-chart-a"))
	ka.SetLabels(map[string]string{consumersLabel: "somebody-else"})
	r, c := newConsumerReconciler(t,
		consumerFixture("tenant-a", "tenant-a-keys"),
		credSecret("tenant-a-keys", map[string]string{"key": "sk-a"}),
		ka,
	)
	if err := reconcileConsumer(t, r, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	var con routingv1.APIConsumer
	nn := types.NamespacedName{Namespace: "llm-route", Name: "tenant-a"}
	if err := c.Get(context.Background(), nn, &con); err != nil {
		t.Fatal(err)
	}
	if condTrue(con.Status.Conditions, condProgrammed) {
		t.Fatal("reported Programmed for a gateway labelled for another manager: every " +
			"request with this key gets 401 while the object says it was accepted")
	}
	if con.Status.AppliedGatewayNamespace != "" {
		t.Fatalf("recorded appliedGatewayNamespace=%q for a gateway it never wrote - that "+
			"value feeds claimed[] and would skip the adoption proof later",
			con.Status.AppliedGatewayNamespace)
	}
	if msg := consumerCondMessage(con.Status.Conditions, condProgrammed); !strings.Contains(msg, consumersLabel) {
		t.Fatalf("the message does not explain why: %q", msg)
	}
	// And nothing was written there.
	if n := len(consumerNames(gatewayConsumers(t, c, "higress-system"))); n != 1 {
		t.Fatalf("the foreign gateway's list has %d entries, want its own 1", n)
	}
}

// Two brand-new objects claiming a CHART-rendered consumer name must not freeze the gateway.
// Each one sees a live entry under that name and no other object's STATUS claiming it, so both
// used to come out as "incumbents", the collision check hit "more than one incumbent" and
// refused the round for everyone: no key addable, rotatable or revocable, and a deletion in
// flight giving up at the deadline with its key still live.
func TestConsumerTwoNewcomersOnAChartNameDoNotFreezeTheGateway(t *testing.T) {
	a := consumerFixture("team-a", "a-keys")
	a.Spec.ConsumerName = "chart-a"
	b := consumerFixture("team-b", "b-keys")
	b.Spec.ConsumerName = "chart-a"
	// Someone already serving, to prove the round still goes through for them.
	serving := consumerFixture("tenant-x", "x-keys")
	serving.Status.ConsumerName = "tenant-x"
	serving.Status.AppliedGatewayNamespace = "higress-system"
	r, c := newConsumerReconciler(t, a, b, serving,
		credSecret("a-keys", map[string]string{"key": "sk-a"}),
		credSecret("b-keys", map[string]string{"key": "sk-b"}),
		credSecret("x-keys", map[string]string{"key": "sk-rotated"}),
		// chart-a is the chart's, not ours: the annotation names only tenant-x.
		keyAuthManagedBy("tenant-x",
			liveConsumer("chart-a", "Bearer sk-chart-a"),
			liveConsumer("tenant-x", "Bearer sk-old")),
	)
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatalf("two newcomers on a chart-rendered name froze the round: %v", err)
	}
	// tenant-x's rotation went through...
	if got := creds0Of(t, c, "higress-system", "tenant-x"); strings.Join(got, ",") != "Bearer sk-rotated" {
		t.Fatalf("tenant-x credentials = %v, want the rotated key", got)
	}
	// ...the chart's entry is untouched, and neither claimant got the name.
	if got := creds0Of(t, c, "higress-system", "chart-a"); strings.Join(got, ",") != "Bearer sk-chart-a" {
		t.Fatalf("chart-a credentials = %v, want the chart's own", got)
	}
}

// A preserved name - ours, with a Secret that cannot be read right now - has to stay in the
// managed-consumers record. Dropping it loses a revocation in the one case the record exists
// for: gateway write landed, status write did not, the Secret then broke, the object is then
// deleted. Without the name recorded, its entry is carried over as somebody else's and the
// revoked tenant's key keeps working.
func TestConsumerPreservedNameStaysInTheRecord(t *testing.T) {
	broken := consumerFixture("tenant-a", "gone-secret")
	healthy := consumerFixture("tenant-b", "tenant-b-keys")
	healthy.Status.ConsumerName = "tenant-b"
	healthy.Status.AppliedGatewayNamespace = "higress-system"
	r, c := newConsumerReconciler(t, broken, healthy,
		credSecret("tenant-b-keys", map[string]string{"key": "sk-b2"}),
		keyAuthManagedBy("tenant-a,tenant-b",
			liveConsumer("tenant-a", "Bearer sk-a"), liveConsumer("tenant-b", "Bearer sk-b")),
	)
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatal(err)
	}
	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "higress-system", Name: "key-auth"}, &ka); err != nil {
		t.Fatal(err)
	}
	got := ka.GetAnnotations()[managedConsumersAnnotation]
	if !strings.Contains(got, "tenant-a") {
		t.Fatalf("managed-consumers = %q: the preserved name was dropped, so deleting it "+
			"later would leave its key working", got)
	}
}

// With a consumer preserved - ours, Secret unreadable - the "nothing changed, do not write"
// short-circuit still has to hold. It did not: the annotation records mine ∪ preserve while
// the comparison used mine alone, so the recorded set was a strict superset forever, the
// check was false on every round, and the gateway object was written on every pass. The
// writes were content-identical, so nothing reached the data plane - which is exactly why
// this needs a test that counts writes rather than looking at the result.
func TestConsumerPreservedNameDoesNotCauseAWritePerRound(t *testing.T) {
	broken := consumerFixture("tenant-a", "gone-secret")
	healthy := consumerFixture("tenant-b", "tenant-b-keys")
	healthy.Status.ConsumerName = "tenant-b"
	healthy.Status.AppliedGatewayNamespace = "higress-system"
	r, c := newConsumerReconciler(t, broken, healthy,
		credSecret("tenant-b-keys", map[string]string{"key": "sk-b"}),
		keyAuthManagedBy("tenant-a,tenant-b",
			liveConsumer("tenant-a", "Bearer sk-a"), liveConsumer("tenant-b", "Bearer sk-b")),
	)
	rv := func() string {
		var ka unstructured.Unstructured
		ka.SetGroupVersionKind(wasmPluginGVK)
		if err := c.Get(context.Background(), types.NamespacedName{
			Namespace: "higress-system", Name: "key-auth"}, &ka); err != nil {
			t.Fatal(err)
		}
		return ka.GetResourceVersion()
	}
	// First round settles the annotation (it gains the preserved name).
	if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
		t.Fatal(err)
	}
	settled := rv()
	for i := 0; i < 3; i++ {
		if _, err := r.programGateway(context.Background(), "higress-system", nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := rv(); got != settled {
		t.Fatalf("key-auth was written again with nothing changed (resourceVersion %s -> %s): "+
			"a preserved consumer makes every round rewrite the gateway", settled, got)
	}
	// ...and the preserved name is still recorded, which is what made the comparison tricky.
	var ka unstructured.Unstructured
	ka.SetGroupVersionKind(wasmPluginGVK)
	_ = c.Get(context.Background(), types.NamespacedName{
		Namespace: "higress-system", Name: "key-auth"}, &ka)
	if got := ka.GetAnnotations()[managedConsumersAnnotation]; !strings.Contains(got, "tenant-a") {
		t.Fatalf("managed-consumers = %q, want the preserved name kept", got)
	}
}
