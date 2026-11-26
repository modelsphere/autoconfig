// APIConsumer: who may call the gateway, and with which API keys.
//
// Every APIConsumer naming one gateway namespace is rendered, together, into that gateway's
// key-auth defaultConfig.consumers. One object per caller, one write per gateway.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	routingv1 "autoconfig/api/v1alpha1"
	"autoconfig/internal/sink"
)

const (
	consumerFinalizer  = "routing.modelsphere.dev/apiconsumer"
	condProgrammed     = "Programmed"
	consumerResyncTime = 60 * time.Second
	// How long deletion keeps trying to revoke the key before it releases the object anyway.
	// Long enough to ride out a gateway rollout or a Secret that is briefly unreadable, short
	// enough that nobody has to hand-edit a finalizer.
	consumerRevokeDeadline = 15 * time.Minute

	// Records which consumer names in key-auth came from APIConsumers.
	managedConsumersAnnotation = "routing.modelsphere.dev/managed-consumers"
)

// ModelFleetReconciler already defines wasmPluginGVK, isAbsent and sameRules; this controller
// shares them deliberately — a second definition is a second thing to keep in step. The
// gateway namespace is resolved separately (consumerGatewayNamespace), because an APIConsumer
// has no fleet to inherit one from.

type APIConsumerReconciler struct {
	client.Client
	// APIReader reads Secrets straight from the API server. The cached client would start an
	// informer on every Secret in the cluster to serve a handful of reads.
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=routing.modelsphere.dev,resources=apiconsumers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=routing.modelsphere.dev,resources=apiconsumers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=routing.modelsphere.dev,resources=apiconsumers/finalizers,verbs=update

func (r *APIConsumerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var consumer routingv1.APIConsumer
	if err := r.Get(ctx, req.NamespacedName, &consumer); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil // deleted and finalized; the rewrite already happened
		}
		return ctrl.Result{}, err
	}

	gwNS := consumerGatewayNamespace(&consumer)

	if !consumer.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&consumer, consumerFinalizer) {
			// Rewrite the list without this consumer BEFORE letting the object go: the
			// reconciler only ever sees the objects that exist, so after the finalizer is
			// gone nothing would remember that this credential is still accepted. Deleting
			// an APIConsumer has to actually revoke the key.
			// Is there anything to revoke at all? status.appliedGatewayNamespace alone is
			// not enough to say no - a round that wrote the credential and then failed to
			// write its own status leaves the key live with no record of it, and skipping
			// the withdrawal would strand that key for good. But status says nothing AND
			// the live list holds no entry under this consumer's name is conclusive: there
			// is no credential of ours in that gateway, so there is nothing to take out.
			//
			// Without this, the first APIConsumer on a not-yet-migrated gateway - created
			// with a typo'd secret ref, so never programmed - sat in Terminating for the
			// full revoke deadline when someone deleted it to try again: the round renders
			// zero consumers over a non-empty chart-rendered list, which the empty-list
			// guard refuses every time, reporting that a key could not be withdrawn when
			// none was ever granted.
			if consumer.Status.AppliedGatewayNamespace == "" {
				if absent, err := r.hasNoLiveEntry(ctx, gwNS, consumerName(&consumer)); err == nil && absent {
					log.V(1).Info("nothing to revoke: never programmed and no entry in the gateway",
						"gateway", gwNS, "consumer", consumerName(&consumer))
					if err := r.patchConsumerFinalizer(ctx, &consumer, false); err != nil {
						return ctrl.Result{}, err
					}
					return ctrl.Result{}, nil
				}
			}
			for _, ns := range distinctNamespaces(gwNS, consumer.Status.AppliedGatewayNamespace) {
				err := r.programGateway(ctx, ns, &consumer)
				// A gateway that is GONE needs no rewrite, and neither does one whose CRD
				// has been uninstalled: there is nothing left to revoke the key from.
				// Anything else - a misspelled namespace, an unreadable Secret belonging to
				// ANOTHER consumer on the same gateway ("one broken Secret stops the whole
				// round") - would otherwise block the finalizer on every pass and leave this
				// object in Terminating for good, needing a hand-edited finalizer to free.
				// Deletion keeps retrying for a while and then gives up and lets go, with
				// the reason on the object: a stuck object is worse than a stale entry
				// nobody can point at.
				if err == nil || errors.Is(err, errGatewayAbsent) {
					continue
				}
				if time.Since(consumer.DeletionTimestamp.Time) < consumerRevokeDeadline {
					r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionFalse, "RevokeError",
						fmt.Sprintf("could not take this consumer out of gateway %q: %v "+
							"(still retrying; the object is held until this succeeds or "+
							"%s has passed)", ns, err, consumerRevokeDeadline), 0, "", "")
					return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
				}
				log.Error(err, "giving up on revoking a consumer; releasing the object anyway",
					"gateway", ns, "deadline", consumerRevokeDeadline)
			}
			if err := r.patchConsumerFinalizer(ctx, &consumer, false); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&consumer, consumerFinalizer) {
		if err := r.patchConsumerFinalizer(ctx, &consumer, true); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Moving gateway.namespace must take the credentials out of the gateway they used to be
	// in. Same reason ModelFleet records appliedGatewayNamespace: nothing else remembers.
	if old := consumer.Status.AppliedGatewayNamespace; old != "" && old != gwNS {
		// isAbsent: the gateway it used to be in has been uninstalled, so there is nothing
		// to take it out of. Retrying that forever would mean this consumer's key never
		// reaches its NEW gateway - the one failure mode worse than a stale entry.
		if err := r.programGateway(ctx, old, nil); err != nil && !errors.Is(err, errGatewayAbsent) {
			r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionFalse, "WithdrawError",
				fmt.Sprintf("could not take this consumer out of its previous gateway %q: %v", old, err), 0, "", "")
			return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
		}
	}

	if err := r.programGateway(ctx, gwNS, nil); err != nil {
		r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionFalse, "ProgramError", err.Error(), 0, "", "")
		return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
	}

	name := consumerName(&consumer)
	creds, err := r.readCredentials(ctx, &consumer)
	if err != nil {
		// Cannot happen: programGateway just read the same Secret successfully. Reported
		// rather than ignored, because "cannot happen" and "is not checked" are different.
		r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionFalse, "SecretError", err.Error(), 0, name, "")
		return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
	}
	log.V(1).Info("consumer programmed", "consumer", name, "gateway", gwNS, "credentials", len(creds))
	r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionTrue, "Programmed",
		fmt.Sprintf("%d credential(s) accepted by the gateway in %s", len(creds), gwNS),
		int32(len(creds)), name, gwNS)
	return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
}

// programGateway renders every APIConsumer that names this gateway namespace into its
// key-auth plugin, in one write.
//
// `excluding` is the object being deleted: it still exists in the API server while its
// finalizer runs, so it has to be left out explicitly.
// errGatewayAbsent: this gateway's key-auth, or the WasmPlugin CRD itself, is not there. The
// only failure that means "there is nothing left to revoke a key from".
var errGatewayAbsent = errors.New("the gateway's key-auth is not present")

func (r *APIConsumerReconciler) programGateway(ctx context.Context, gwNS string, excluding *routingv1.APIConsumer) error {
	var list routingv1.APIConsumerList
	if err := r.List(ctx, &list); err != nil {
		// A failed list renders an empty consumer list, which is the single most dangerous
		// write this controller can make: it would lock every caller out of every route.
		// Never guess from a partial read.
		return fmt.Errorf("list APIConsumers: %w", err)
	}

	// Has this controller ever programmed THIS gateway? It is the difference between "the
	// last consumer was just revoked" and "nobody has migrated this gateway yet, and its list
	// still belongs to the chart". Both look like "zero objects want this gateway", and only
	// one of them may empty a live list.
	ownedBefore := false
	// A second source of attribution, independent of the annotation on key-auth: the names
	// APIConsumers themselves report having programmed into this gateway. The annotation is
	// the only record for an object that is already gone, but it lives on an object we do
	// not own - recreate or kubectl replace that WasmPlugin and it is lost. With only the
	// annotation, deleting an APIConsumer after that would classify its own entry as
	// somebody else's, carry it over, release the object without an error, and leave the
	// revoked tenant's key working. Statuses cover every object that still exists, including
	// the one being deleted.
	claimed := map[string]bool{}
	for i := range list.Items {
		c := &list.Items[i]
		if c.Status.AppliedGatewayNamespace == gwNS {
			ownedBefore = true
			if c.Status.ConsumerName != "" {
				claimed[c.Status.ConsumerName] = true
			}
		}
	}

	views := make([]sink.ConsumerView, 0, len(list.Items))
	for i := range list.Items {
		c := &list.Items[i]
		if consumerGatewayNamespace(c) != gwNS {
			continue
		}
		if !c.DeletionTimestamp.IsZero() {
			continue // on its way out; its own finalizer pass rewrites the list
		}
		if excluding != nil && c.Namespace == excluding.Namespace && c.Name == excluding.Name {
			continue
		}
		creds, err := r.readCredentials(ctx, c)
		if err != nil {
			// Do NOT drop this consumer and write the rest. A consumer missing from the list
			// is a caller that gets 401 on its next request, and a Secret that is briefly
			// unreadable must not revoke someone's key. Refusing the whole write leaves the
			// gateway on its last good list - every caller keeps working - and the failure
			// is reported on the object that caused it.
			return fmt.Errorf("%s/%s: %w (nothing was written; the gateway keeps its "+
				"current consumer list)", c.Namespace, c.Name, err)
		}
		views = append(views, sink.ConsumerView{
			Name:        consumerName(c),
			Credentials: creds,
			Source:      c.Namespace + "/" + c.Name,
		})
	}

	consumers, err := sink.RenderConsumers(views)
	if err != nil {
		return err
	}
	return r.writeConsumers(ctx, gwNS, consumers, ownedBefore, claimed)
}

// writeConsumers replaces defaultConfig.consumers in the gateway's key-auth plugin, leaving
// every other key in that config alone.
func (r *APIConsumerReconciler) writeConsumers(ctx context.Context, gwNS string,
	consumers []sink.KeyAuthConsumer, ownedBefore bool, claimed map[string]bool) error {
	log := logf.FromContext(ctx)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(wasmPluginGVK)
		if err := r.Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
			if isAbsent(err) {
				// Distinguished by a sentinel rather than by IsNotFound on whatever came
				// back: a MISSING SECRET is also NotFound, and treating that as "the gateway
				// is gone" would let a deletion release its object while the credential is
				// still live in key-auth. The one case that must be told apart from every
				// other failure deserves its own signal, not a shared error class.
				return fmt.Errorf("key-auth in namespace %q: %w", gwNS, errGatewayAbsent)
			}
			return fmt.Errorf("key-auth in namespace %q: %w", gwNS, err)
		}
		live, _, err := unstructured.NestedSlice(obj.Object, "spec", "defaultConfig", "consumers")
		if err != nil {
			return fmt.Errorf("read defaultConfig.consumers: %w", err)
		}
		// Take ownership of the field before writing it, if it is not ours yet.
		//
		// Why this cannot be left to the write itself: a non-apply Update only takes
		// ownership of the fields whose VALUES it changes, and the first thing this
		// controller does on an already-populated gateway is adopt the live entries
		// unchanged. Measured on the test cluster, a same-value Update moves nothing, so the
		// field stays helm's for as long as the rendered list happens to match - and the
		// gateway chart holds its upgrade back until a non-helm manager owns the field,
		// which would then never happen.
		//
		// A force apply of just this field is enough: it leaves the value alone and makes
		// autoconfig a CO-OWNER alongside helm (also measured - it does not dispossess
		// helm), which is what the chart waits for. The chart then stops rendering the
		// field, helm's claim on it lapses, and this controller is left as the only owner.
		// `keys` and `global_auth` are untouched and stay helm's: defaultConfig is tracked
		// field by field, not as one atomic blob.
		if len(consumers) > 0 && !ownsConsumers(obj) {
			if err := r.claimConsumers(ctx, gwNS, live); err != nil {
				return fmt.Errorf("take ownership of %s/key-auth defaultConfig.consumers: %w", gwNS, err)
			}
			// Re-read: the apply bumped resourceVersion, and the Update below would be
			// refused on the stale one.
			if err := r.Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
				return fmt.Errorf("key-auth in namespace %q after taking ownership: %w", gwNS, err)
			}
		}

		// THE guard. An empty list where the gateway has entries means every caller is
		// refused on every route - the whole site's authentication, off, in one write.
		//
		// But emptying it is sometimes exactly right: revoking the last consumer, or moving
		// every consumer to another gateway, both end here legitimately. The two cases look
		// identical from the desired set alone - zero objects want this gateway - so the
		// question asked instead is whether this controller has ever OWNED this list.
		// If some APIConsumer records this gateway in status.appliedGatewayNamespace, the
		// list is ours and emptying it is a revocation. If none does, the live entries came
		// from somewhere else (the chart, during a half-finished migration) and wiping them
		// would take the site's authentication down for a reason nobody asked for.
		if len(consumers) == 0 && len(live) > 0 && !ownedBefore {
			return fmt.Errorf("refusing to write an empty consumer list over %d live consumer(s) "+
				"in %s/key-auth: no APIConsumer has ever programmed this gateway, so those "+
				"entries belong to something else (the gateway chart renders them until the "+
				"migration is finished) and removing them would deny every caller on every route",
				len(live), gwNS)
		}

		next := make([]any, 0, len(consumers))
		mine := make(map[string]bool, len(consumers))
		for _, c := range consumers {
			m, err := sink.ToMap(c)
			if err != nil {
				return err
			}
			mine[c.Name] = true
			next = append(next, m)
		}
		// Carry over the live entries this controller did not write. The empty-list guard
		// above only catches a write that renders NOTHING, which left the much more likely
		// case open: the gateway chart renders three tenants, someone creates their first
		// APIConsumer, and a full replacement with one entry revokes the other three - a 401
		// for each of them, from the first step the documentation tells people to take.
		//
		// Anything whose name is not in managedConsumerNames is somebody else's and is kept
		// verbatim. A name this controller HAS managed is not carried over, because that is
		// what deleting an APIConsumer has to mean: the key stops working.
		managed, err := managedConsumerNames(obj)
		if err != nil {
			return err
		}
		var foreign []string
		for _, lc := range live {
			m, ok := lc.(map[string]any)
			if !ok {
				return fmt.Errorf("%s/key-auth: defaultConfig.consumers holds a %T, not an "+
					"object; refusing to rewrite a list this controller cannot read", gwNS, lc)
			}
			name, _ := m["name"].(string)
			if name == "" {
				return fmt.Errorf("%s/key-auth: a live consumer has no name; refusing to "+
					"rewrite a list this controller cannot account for", gwNS)
			}
			if mine[name] || managed[name] || claimed[name] {
				continue
			}
			foreign = append(foreign, name)
			next = append(next, lc)
		}
		sortConsumerList(next)

		// The duplicate-credential check has to run over the FINAL list, not only over the
		// APIConsumers. sink.RenderConsumers refuses two APIConsumers sharing a key, but a
		// carried-over entry is copied in verbatim and was never compared against ours - and
		// key-auth does not care where a duplicate came from: it logs `duplicate consumer
		// credential`, fails to parse the whole defaultConfig, and the plugin is FAIL_CLOSE,
		// so every route on the gateway denies every caller.
		//
		// It is reachable on the documented migration path, not in theory: a chart entry
		// whose name cannot be adopted (consumerName is pinned to a DNS label, and the
		// chart's might not be one) gets a new APIConsumer carrying the SAME key, because
		// the point is not to make the caller change anything. Both entries then exist.
		if err := refuseDuplicateCredentials(gwNS, next); err != nil {
			return err
		}
		// Unchanged AND already attributed: do not write. Every write here is an ECDS push
		// of key-auth to the whole data plane, and the console calls this on every key edit.
		//
		// The second half is not decoration. Adopting a consumer whose entry the chart
		// already renders, with the same credential, leaves the list byte-identical - so a
		// check on the list alone returns early and never records that the entry is now
		// OURS. That is how the live migration ended up with an annotation naming one of
		// three adopted consumers: the first write recorded itself, and the next two were
		// skipped as no-ops. Deleting either of those two would then have left its entry in
		// place as somebody else's, and the key would have kept working - the one thing
		// deleting an APIConsumer must never do.
		if sameRules(live, next) && sameNames(managed, mine) {
			return nil
		}
		if err := unstructured.SetNestedSlice(obj.Object, next, "spec", "defaultConfig", "consumers"); err != nil {
			return err
		}
		// Written next to the data it describes, because nothing else can tell the two apart
		// later: once an APIConsumer is deleted, its object is gone and its entry would look
		// exactly like one of the chart's.
		setManagedConsumerNames(obj, consumers)
		log.Info("programming gateway consumers", "gateway", gwNS,
			"mine", len(consumers), "carried over", len(foreign), "was", len(live))
		return r.Update(ctx, obj)
	})
}

// readCredentials reads every value in the consumer's Secret. All of them are accepted, which
// is what makes a rotation possible: add the new key, let both work, remove the old one.
func (r *APIConsumerReconciler) readCredentials(ctx context.Context, c *routingv1.APIConsumer) ([]string, error) {
	ref := c.Spec.CredentialsSecretRef
	if strings.ContainsAny(ref, "/:") {
		// No namespace prefix, and no "name:key" form: this controller holds a cluster-wide
		// `secrets: get`, so a ref that could point elsewhere would turn "can write an
		// APIConsumer in this namespace" into "can read any Secret in the cluster".
		return nil, fmt.Errorf("credentialsSecretRef %q: must be the bare name of a Secret in "+
			"this object's own namespace", ref)
	}
	var sec corev1.Secret
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: ref}, &sec); err != nil {
		return nil, fmt.Errorf("credentialsSecretRef %q: %w", ref, err)
	}
	keys := make([]string, 0, len(sec.Data))
	for k := range sec.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys) // stable order, so an unchanged Secret renders an unchanged list
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		v := strings.TrimSpace(string(sec.Data[k]))
		if v == "" {
			continue // an empty value would be a credential that matches nothing
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("secret %q holds no usable credential", ref)
	}
	return out, nil
}

func (r *APIConsumerReconciler) patchConsumerFinalizer(ctx context.Context, c *routingv1.APIConsumer, add bool) error {
	// A merge patch, not an Update: a typed Update sends the object as THIS binary's types
	// understand it and silently drops spec fields it does not know, which is how an older
	// replica mid-rollout erases a field a newer CRD just accepted.
	before := c.DeepCopy()
	if add {
		if !controllerutil.AddFinalizer(c, consumerFinalizer) {
			return nil
		}
	} else if !controllerutil.RemoveFinalizer(c, consumerFinalizer) {
		return nil
	}
	return r.Patch(ctx, c, client.MergeFrom(before))
}

// setConsumerStatus records the outcome of one round. appliedGateway is the namespace this
// round actually programmed, passed in rather than re-read from the spec: the object is
// re-Get here, and a spec edit that lands between the write and this status update would
// otherwise record a gateway that was never programmed. The next round then compares
// status.appliedGatewayNamespace against the new spec, finds them equal, and never withdraws
// the credential from the gateway that really has it - the one failure this field exists to
// prevent. Empty means "no gateway was programmed in this round", which leaves the recorded
// one alone.
func (r *APIConsumerReconciler) setConsumerStatus(ctx context.Context, nn types.NamespacedName,
	status metav1.ConditionStatus, reason, msg string, creds int32, name, appliedGateway string) {
	log := logf.FromContext(ctx)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var c routingv1.APIConsumer
		if err := r.Get(ctx, nn, &c); err != nil {
			return err
		}
		st := &c.Status
		prev := st.DeepCopy()
		if status == metav1.ConditionTrue {
			// Only on success: observedGeneration is how the console answers "is this key
			// live yet", and advancing it on a failed round says yes while the gateway still
			// carries the old list.
			st.ObservedGeneration = c.Generation
			if appliedGateway != "" {
				st.AppliedGatewayNamespace = appliedGateway
			}
			st.ConsumerName = name
			st.Credentials = creds
		}
		setConsumerCond(st, status, reason, msg, c.Generation)
		if statusSettled(prev, st, consumerResyncTime, prev.LastSyncTime) {
			return nil
		}
		now := metav1.Now()
		st.LastSyncTime = &now
		return r.Status().Update(ctx, &c)
	})
	if err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "update APIConsumer status")
	}
}

func setConsumerCond(st *routingv1.APIConsumerStatus, status metav1.ConditionStatus, reason, msg string, gen int64) {
	meta := metav1.Condition{
		Type: condProgrammed, Status: status, Reason: reason, Message: msg,
		ObservedGeneration: gen, LastTransitionTime: metav1.Now(),
	}
	for i := range st.Conditions {
		if st.Conditions[i].Type == condProgrammed {
			if st.Conditions[i].Status == status {
				meta.LastTransitionTime = st.Conditions[i].LastTransitionTime
			}
			st.Conditions[i] = meta
			return
		}
	}
	st.Conditions = append(st.Conditions, meta)
}

func consumerName(c *routingv1.APIConsumer) string {
	if c.Spec.ConsumerName != "" {
		return c.Spec.ConsumerName
	}
	return c.Name
}

func consumerGatewayNamespace(c *routingv1.APIConsumer) string {
	if c.Spec.Gateway.Namespace != "" {
		return c.Spec.Gateway.Namespace
	}
	return defaultGatewayNamespace
}

func distinctNamespaces(a, b string) []string {
	if b == "" || a == b {
		return []string{a}
	}
	return []string{a, b}
}

func (r *APIConsumerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// No watch on Secrets, deliberately. Watching them would pick up a rotated key the
	// instant the Secret changed, but a Watches() on Secrets starts an informer over EVERY
	// Secret in the cluster - the cache and the cluster-wide list/watch that this controller
	// went out of its way not to need (it holds `secrets: get` and reads them uncached). A
	// key added to a Secret therefore takes effect on the next resync, within
	// consumerResyncTime; editing the APIConsumer itself is instant. Worth revisiting only if
	// a minute turns out to be too long for a rotation.
	return ctrl.NewControllerManagedBy(mgr).
		For(&routingv1.APIConsumer{}).
		Complete(r)
}

// managedConsumerNames: the consumer names this controller wrote last time, recorded on the
// WasmPlugin itself. It is the only way to tell "an entry I own, whose APIConsumer has since
// been deleted, and which must now stop working" from "an entry somebody else owns, which I
// must not touch". Both are just names in a list with no object behind them.
func managedConsumerNames(obj *unstructured.Unstructured) (map[string]bool, error) {
	out := map[string]bool{}
	raw := obj.GetAnnotations()[managedConsumersAnnotation]
	if raw == "" {
		return out, nil
	}
	for _, n := range strings.Split(raw, ",") {
		if n = strings.TrimSpace(n); n != "" {
			out[n] = true
		}
	}
	return out, nil
}

func setManagedConsumerNames(obj *unstructured.Unstructured, consumers []sink.KeyAuthConsumer) {
	names := make([]string, 0, len(consumers))
	for _, c := range consumers {
		names = append(names, c.Name)
	}
	sort.Strings(names)
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	if len(names) == 0 {
		delete(ann, managedConsumersAnnotation)
	} else {
		ann[managedConsumersAnnotation] = strings.Join(names, ",")
	}
	obj.SetAnnotations(ann)
}

// Stable order, so that carrying foreign entries over does not produce a different list every
// round and an ECDS push with it.
func sortConsumerList(list []any) {
	sort.SliceStable(list, func(i, j int) bool {
		a, _ := list[i].(map[string]any)
		b, _ := list[j].(map[string]any)
		an, _ := a["name"].(string)
		bn, _ := b["name"].(string)
		return an < bn
	})
}

// fieldManagerName is the manager this controller writes under. It is what the gateway chart
// looks for before it yields defaultConfig.consumers, so it is part of an agreed contract
// with that chart, not an internal detail - see modelsphere/modelsphere#90.
const fieldManagerName = "autoconfig"

// ownsConsumers: does this controller already own defaultConfig.consumers? Any entry under
// its own manager counts, whether recorded by an apply or by an update - the chart yields to
// any manager that is not helm.
func ownsConsumers(obj *unstructured.Unstructured) bool {
	for _, mf := range obj.GetManagedFields() {
		if mf.Manager != fieldManagerName || mf.FieldsV1 == nil {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(mf.FieldsV1.Raw, &fields); err != nil {
			continue
		}
		spec, _ := fields["f:spec"].(map[string]any)
		def, _ := spec["f:defaultConfig"].(map[string]any)
		if _, ok := def["f:consumers"]; ok {
			return true
		}
	}
	return false
}

// claimConsumers applies defaultConfig.consumers with its CURRENT value, which changes
// nothing and makes this controller an owner of the field. force, because the point is to
// claim a field somebody else owns; without it the apply is refused with a conflict.
//
// Only this one field is sent. defaultConfig's other keys are not named, and an apply does
// not remove fields it never claimed, so keys and global_auth stay exactly as they are -
// which matters, since a key-auth without a consumers OR credentials key fails to parse
// altogether and the plugin is FAIL_CLOSE.
func (r *APIConsumerReconciler) claimConsumers(ctx context.Context, gwNS string, live []any) error {
	claim := &unstructured.Unstructured{}
	claim.SetGroupVersionKind(wasmPluginGVK)
	claim.SetNamespace(gwNS)
	claim.SetName("key-auth")
	value := live
	if value == nil {
		// Absent rather than empty: claim it as the empty list, which is also the value a
		// gateway needs to have at all for its config to parse.
		value = []any{}
	}
	if err := unstructured.SetNestedSlice(claim.Object, value, "spec", "defaultConfig", "consumers"); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("taking ownership of the gateway consumer list",
		"gateway", gwNS, "entries", len(value))
	return r.Patch(ctx, claim, client.Apply, client.ForceOwnership, client.FieldOwner(fieldManagerName))
}

// sameNames: do the two name sets hold the same names?
func sameNames(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for n := range a {
		if !b[n] {
			return false
		}
	}
	return true
}

// refuseDuplicateCredentials rejects a list in which one credential appears under two
// consumers. Never written rather than written and hoped about: key-auth treats it as a parse
// error for the entire defaultConfig, and FAIL_CLOSE turns that into "no caller gets through
// on any route of this gateway".
//
// The message names the two consumers and never the credential: it lands on a CR, in the
// controller log, and in whatever ships those.
func refuseDuplicateCredentials(gwNS string, list []any) error {
	owner := map[string]string{}
	for _, entry := range list {
		m, ok := entry.(map[string]any)
		if !ok {
			continue // already refused above; keep this function total
		}
		name, _ := m["name"].(string)
		creds, _ := m["credentials"].([]any)
		for i, c := range creds {
			key, ok := c.(string)
			if !ok {
				return fmt.Errorf("%s/key-auth: consumer %q has a non-string credential at "+
					"position %d; refusing to write a list key-auth would reject", gwNS, name, i+1)
			}
			if prev, dup := owner[key]; dup {
				return fmt.Errorf("%s/key-auth: consumers %q and %q share credential #%d. "+
					"key-auth refuses a duplicate credential and fails to parse its whole "+
					"configuration, which denies every caller on every route of this gateway, "+
					"so nothing was written. If %q is an entry this controller does not manage "+
					"(the gateway chart still renders it), give the APIConsumer its own key, or "+
					"adopt that entry by naming the APIConsumer after it",
					gwNS, prev, name, i+1, prev)
			}
			owner[key] = name
		}
	}
	return nil
}

// hasNoLiveEntry: does this gateway's key-auth hold no consumer under this name? Used only to
// decide that a deletion has nothing to revoke, so an error or a missing gateway answers
// "cannot say" and the caller falls back to withdrawing properly.
func (r *APIConsumerReconciler) hasNoLiveEntry(ctx context.Context, gwNS, name string) (bool, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(wasmPluginGVK)
	if err := r.Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
		return false, err
	}
	live, _, err := unstructured.NestedSlice(obj.Object, "spec", "defaultConfig", "consumers")
	if err != nil {
		return false, err
	}
	for _, lc := range live {
		m, ok := lc.(map[string]any)
		if !ok {
			return false, fmt.Errorf("unreadable consumer entry")
		}
		if n, _ := m["name"].(string); n == name {
			return false, nil
		}
	}
	return true, nil
}
