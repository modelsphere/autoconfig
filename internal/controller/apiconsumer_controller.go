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
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

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
				_, err := r.programGateway(ctx, ns, &consumer)
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
		if _, err := r.programGateway(ctx, old, nil); err != nil && !errors.Is(err, errGatewayAbsent) {
			r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionFalse, "WithdrawError",
				fmt.Sprintf("could not take this consumer out of its previous gateway %q: %v", old, err), 0, "", "")
			return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
		}
	}

	round, err := r.programGateway(ctx, gwNS, nil)
	// A reason specific to THIS object comes first, even when the round also failed. It was
	// left out on purpose - a collision with somebody already serving traffic, an identity it
	// cannot prove it holds, a Secret that cannot be read - and that is what whoever created
	// it needs to read. The round's own error is usually a consequence: leaving this object
	// out can be what made the rendered list empty, and the empty-list guard then refuses to
	// write it over live entries, which is correct but says nothing about this object.
	if why := round.reasonFor(&consumer); why != "" {
		r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionFalse, "NotProgrammed",
			why, 0, "", "")
		return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
	}
	if err != nil {
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

// gatewayReader: every read of a key-auth WasmPlugin goes through the uncached reader, not the
// client's cache.
//
// Not a performance choice. The cached client builds an informer for a kind on first use, and
// this controller runs on clusters where the WasmPlugin kind does not exist yet - Higress's
// CRDs come from the higress CORE release, and autoconfig is deployed before higress-llm,
// which needs it. The key-auth watch is gated on discovery for that reason; routing these
// reads through the cache would reintroduce the same dependency one layer down, where no test
// would notice, and the behaviour when a CRD appears mid-run (does a lazy informer start, and
// how long does it block?) is something I would then be asserting rather than knowing.
//
// Uncached, the answer is the same in every state: a missing kind is an error that isAbsent
// recognises, and a kind installed later works on the next read. The cost is one GET per
// gateway per round.
func (r *APIConsumerReconciler) gatewayReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// gatewayRound reports what one programming round did with objects it could not include.
// Keyed by "namespace/name", the value is why. It exists because this round is shared by every
// APIConsumer on the gateway: a newcomer that cannot be rendered must not fail the round for
// everybody else, but it still has to learn about it on its own object, so skipping it
// silently is not an option either.
type gatewayRound struct {
	skipped map[string]string
}

func (g gatewayRound) reasonFor(c *routingv1.APIConsumer) string {
	if g.skipped == nil || c == nil {
		return ""
	}
	return g.skipped[c.Namespace+"/"+c.Name]
}

func (r *APIConsumerReconciler) programGateway(ctx context.Context, gwNS string, excluding *routingv1.APIConsumer) (gatewayRound, error) {
	var list routingv1.APIConsumerList
	if err := r.List(ctx, &list); err != nil {
		// A failed list renders an empty consumer list, which is the single most dangerous
		// write this controller can make: it would lock every caller out of every route.
		// Never guess from a partial read.
		return gatewayRound{}, fmt.Errorf("list APIConsumers: %w", err)
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

	// The names already in this gateway's list. Needed before rendering, to tell "this
	// consumer's Secret is unreadable and it has a key in there" (fail closed) from "...and
	// it has nothing in there" (skip it rather than freeze everyone else).
	liveEntries, liveManaged, err := r.liveConsumerState(ctx, gwNS)
	if err != nil && !errors.Is(err, errGatewayAbsent) {
		return gatewayRound{}, err
	}
	liveNames := map[string]bool{}
	for n := range liveEntries {
		liveNames[n] = true
	}

	round := gatewayRound{skipped: map[string]string{}}
	// incumbent: is the entry under this consumer's name in this gateway THIS object's?
	// Everything that can go wrong with a newcomer is survivable by leaving it out; the same
	// thing wrong with an incumbent means refusing the round, because dropping it revokes a
	// working key.
	//
	// Not "is there an entry under that name", which is what this asked first and is a hole:
	// an object whose consumerName names somebody else's live entry would have counted as an
	// incumbent, so an impostor and the consumer it was impersonating came out as two
	// incumbents and the round was refused for the whole gateway - exactly the freeze this
	// is here to avoid, and with the impersonation deciding it.
	//
	// Its own status is conclusive. Failing that, an entry with no other claimant is
	// plausibly its own: that is the object whose gateway write landed and whose status
	// write did not, and skipping it WOULD revoke a live key.
	incumbent := func(c *routingv1.APIConsumer) bool {
		if c.Status.AppliedGatewayNamespace == gwNS {
			return true
		}
		name := consumerName(c)
		if !hasLiveEntry(liveNames, name) {
			return false
		}
		for i := range list.Items {
			o := &list.Items[i]
			if o.Namespace == c.Namespace && o.Name == c.Name {
				continue
			}
			if o.Status.AppliedGatewayNamespace == gwNS && o.Status.ConsumerName == name {
				return false // that entry is on record as somebody else's
			}
		}
		return true
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
			// Fail closed for a consumer that HAS a key in this gateway: dropping it from
			// the list is a 401 on its next request, and a Secret that is briefly
			// unreadable must not revoke anyone. The whole round is refused, the gateway
			// keeps its last good list, and the failure is reported on the object that
			// caused it.
			//
			// But a consumer that was never programmed here has nothing in the list to
			// lose, and refusing the round on its behalf freezes the gateway for everybody
			// else: while a fresh object with a typo'd secretRef exists, no other consumer
			// can be added, rotated or REVOKED - including removing a leaked key, and
			// including a deletion, which then retries for the revoke deadline and
			// releases with the key still live. One new broken object must not hold the
			// rest hostage, so it is skipped, with the error on its own object.
			if !incumbent(c) {
				round.skipped[c.Namespace+"/"+c.Name] = err.Error()
				continue
			}
			return round, fmt.Errorf("%s/%s: %w (nothing was written; the gateway keeps its "+
				"current consumer list)", c.Namespace, c.Name, err)
		}
		views = append(views, sink.ConsumerView{
			Name:        consumerName(c),
			Credentials: creds,
			Source:      c.Namespace + "/" + c.Name,
		})
	}

	// A newcomer that collides with somebody already here - same consumer name, or the same
	// credential - is dropped the same way an unreadable Secret is, and for the same reason.
	// RenderConsumers refuses the whole view set on a collision, which is right as a last
	// line of defence but wrong as the response to one new object: it would freeze every key
	// on the gateway, including REVOKING a leaked one, and a deletion in flight would retry
	// to the deadline and then release with the key still live.
	//
	// The refusals themselves stay - the duplicate-name check is what stops a
	// namespace-scoped writer from stealing a managed consumer name, and the duplicate
	// credential one is what stops a gateway-wide parse failure. Only who pays for the
	// collision changes: the newcomer, not everybody.
	views, err = r.dropCollidingNewcomers(ctx, views, list.Items, gwNS, incumbent, round)
	if err != nil {
		return round, err
	}

	// Taking over a consumer name that already exists here and belongs to no APIConsumer
	// requires carrying every credential it accepts - proof that whoever wrote this object
	// already holds the key. consumerName is free-form and an APIConsumer can be created in
	// any namespace, so without it, writing `consumerName: tenant-a` and your own key
	// inherits every route tenant-a is allowed on.
	//
	// Decided here, where the objects are, and the object that cannot prove it is left out
	// rather than failing the round: wanting an identity it is not entitled to must not stop
	// every other consumer on this gateway from being added, rotated or revoked.
	views = dropUnprovenAdoptions(views, liveEntries, liveManaged, claimed, round, gwNS)

	if len(round.skipped) > 0 {
		logf.FromContext(ctx).Info("left consumers out of this round",
			"gateway", gwNS, "skipped", round.skipped)
	}
	consumers, err := sink.RenderConsumers(views)
	if err != nil {
		return round, err
	}
	return round, r.writeConsumers(ctx, gwNS, consumers, ownedBefore, claimed)
}

// dropCollidingNewcomers resolves two claims on one identity - the same consumer name, or the
// same credential - without failing the round for everybody else on the gateway.
//
// RenderConsumers refuses a colliding view set outright, which is right as a last line of
// defence and wrong as the response to one new object: it would freeze every key on this
// gateway, including REVOKING a leaked one, and a deletion in flight would retry to the
// deadline and then release with the key still live.
//
// Who pays for the collision:
//
//   - exactly one claimant is already serving traffic here: it keeps the identity, and every
//     newcomer is left out and told why. This is what stops a namespace-scoped writer from
//     taking a name that belongs to someone else.
//   - nobody is serving it yet: ALL the claimants are left out. Letting the first one win
//     would be arbitrating "who is tenant-a" by list order, which is a coin flip in a
//     dispute - and nobody is losing access, because nobody had the name.
//   - two claimants are already serving it: refused as an error. One bad newcomer cannot
//     produce this, it means two live consumers genuinely share an identity, and dropping
//     either revokes a working key. A person has to look.
func (r *APIConsumerReconciler) dropCollidingNewcomers(ctx context.Context,
	views []sink.ConsumerView, items []routingv1.APIConsumer, gwNS string,
	incumbent func(*routingv1.APIConsumer) bool, round gatewayRound) ([]sink.ConsumerView, error) {
	bySource := map[string]*routingv1.APIConsumer{}
	for i := range items {
		bySource[items[i].Namespace+"/"+items[i].Name] = &items[i]
	}
	isIncumbent := func(v sink.ConsumerView) bool {
		c, ok := bySource[v.Source]
		return ok && incumbent(c)
	}

	// claims[identity] = the views claiming it, in list order.
	type claim struct {
		kind  string // "consumer name" or "credential"
		views []sink.ConsumerView
	}
	claims := map[string]*claim{}
	add := func(kind, identity string, v sink.ConsumerView) {
		key := kind + "\x00" + identity
		if claims[key] == nil {
			claims[key] = &claim{kind: kind}
		}
		claims[key].views = append(claims[key].views, v)
	}
	for _, v := range views {
		add("consumer name", v.Name, v)
		for _, cred := range v.Credentials {
			// By credential VALUE - never put in a message - and with the prefix normalised,
			// because that is how key-auth will see them: `sk-x` in one consumer and
			// `Bearer sk-x` in another render to the same string, so comparing raw values
			// let that pair past this check and into RenderConsumers, which refuses the
			// whole round instead of leaving the newcomer out.
			add("credential", strings.TrimPrefix(cred, bearerPrefixValue), v)
		}
	}

	drop := map[string]string{} // Source -> reason
	for key, cl := range claims {
		if len(cl.views) < 2 {
			continue
		}
		identity := key[strings.Index(key, "\x00")+1:]
		var incumbents, newcomers []sink.ConsumerView
		for _, v := range cl.views {
			if isIncumbent(v) {
				incumbents = append(incumbents, v)
			} else {
				newcomers = append(newcomers, v)
			}
		}
		switch {
		case len(incumbents) > 1:
			if cl.kind == "credential" {
				return nil, fmt.Errorf("%s/key-auth: %d consumers already serving this "+
					"gateway share one credential; key-auth refuses a duplicate credential "+
					"and fails to parse its whole configuration, which denies every caller "+
					"on every route. Nothing was written, and this needs a person: dropping "+
					"either of them revokes a working key. The objects are %s",
					gwNS, len(incumbents), sourcesOf(incumbents))
			}
			return nil, fmt.Errorf("%s/key-auth: consumer name %q is claimed by %s, and more "+
				"than one of them is already serving this gateway. Nothing was written, and "+
				"this needs a person: dropping either revokes a working key",
				gwNS, identity, sourcesOf(incumbents))
		case len(incumbents) == 1:
			for _, v := range newcomers {
				drop[v.Source] = collisionReason(cl.kind, identity, gwNS,
					"it is already served by "+incumbents[0].Source)
			}
		default:
			for _, v := range cl.views {
				drop[v.Source] = collisionReason(cl.kind, identity, gwNS,
					"it is claimed by "+sourcesOf(cl.views)+" and nothing here serves it yet, "+
						"so none of them was given it - which of these objects owns this "+
						"identity is not something this controller may decide")
			}
		}
	}
	if len(drop) == 0 {
		return views, nil
	}
	kept := make([]sink.ConsumerView, 0, len(views))
	for _, v := range views {
		if why, dropped := drop[v.Source]; dropped {
			round.skipped[v.Source] = why
			continue
		}
		kept = append(kept, v)
	}
	_ = ctx
	return kept, nil
}

// collisionReason: never names the credential, only what collided and with whom. This text
// lands on a CR and in the logs.
func collisionReason(kind, identity, gwNS, who string) string {
	what := fmt.Sprintf("consumer name %q", identity)
	if kind == "credential" {
		what = "one of this object's credentials"
	}
	return fmt.Sprintf("%s collides on gateway %s: %s. This object was left out of the "+
		"gateway; everything else on it is unaffected", what, gwNS, who)
}

func sourcesOf(views []sink.ConsumerView) string {
	out := make([]string, 0, len(views))
	for _, v := range views {
		out = append(out, v.Source)
	}
	sort.Strings(out)
	return strings.Join(out, " and ")
}

// writeConsumers replaces defaultConfig.consumers in the gateway's key-auth plugin, leaving
// every other key in that config alone.
func (r *APIConsumerReconciler) writeConsumers(ctx context.Context, gwNS string,
	consumers []sink.KeyAuthConsumer, ownedBefore bool, claimed map[string]bool) error {
	log := logf.FromContext(ctx)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(wasmPluginGVK)
		if err := r.gatewayReader().Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
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
		live, found, err := unstructured.NestedSlice(obj.Object, "spec", "defaultConfig", "consumers")
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
			if err := r.gatewayReader().Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
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
		// Which names this controller wrote last time, read before the guard below because
		// the guard needs it.
		managed, err := managedConsumerNames(obj)
		if err != nil {
			return err
		}

		// The annotation counts as ownership too, not only a status. Otherwise the last
		// consumer on a gateway cannot be revoked in one specific case: its gateway write
		// succeeded, its status write did not, and it is then deleted - no status claims the
		// gateway, so ownedBefore is false, the guard refuses, the finalizer gives up at the
		// deadline, and the key stays live with no APIConsumer left to clean it up. If the
		// annotation says the live entries are ours, emptying the list IS the revocation.
		if len(managed) > 0 {
			ownedBefore = true
		}
		if len(consumers) == 0 && len(live) > 0 && !ownedBefore {
			return fmt.Errorf("refusing to write an empty consumer list over %d live consumer(s) "+
				"in %s/key-auth: no APIConsumer has ever programmed this gateway, so those "+
				"entries belong to something else (the gateway chart renders them until the "+
				"migration is finished) and removing them would deny every caller on every route",
				len(live), gwNS)
		}

		next := make([]any, 0, len(consumers))
		mine := make(map[string]bool, len(consumers))
		myCreds := make(map[string]map[string]bool, len(consumers))
		for _, c := range consumers {
			m, err := sink.ToMap(c)
			if err != nil {
				return err
			}
			mine[c.Name] = true
			myCreds[c.Name] = make(map[string]bool, len(c.Credentials))
			for _, cred := range c.Credentials {
				myCreds[c.Name][cred] = true
			}
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
			if managed[name] || claimed[name] {
				continue // already ours; this is a rotation, not an adoption
			}
			if mine[name] {
				// Ours this round. Whether this object is ENTITLED to a name that already
				// existed here is decided in programGateway, where the objects are: by the
				// time a view reaches this function it has been cleared, and a view that was
				// not is no longer in the list.
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
		// `found` first: an ABSENT consumers key is not the same as an empty one. key_auth's
		// parsePluginConfig returns false when neither consumers nor credentials is present,
		// and the plugin is FAIL_CLOSE - so a gateway whose key-auth has no consumers key at
		// all refuses every request on every route. Writing the key, even empty, is what
		// makes that config parse. Without this the early return fires (sameRules treats
		// nil and [] as equal) and the key is never created.
		//
		// This is the gateway chart's side of the contract: it stopped rendering the field,
		// so this controller is the only thing that can put it there.
		if found && sameRules(live, next) && sameNames(managed, mine) {
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
		// FieldOwner spelled out, although the client's default manager happens to be this
		// same name (measured on the test cluster: managedFields carries `autoconfig
		// Update`). consumers is an atomic field, so if the default were ever something
		// else - it is derived from the user agent, which a binary name or a library bump
		// can change - each changing Update would move ownership away from the name
		// ownsConsumers looks for, and every change would then cost a force-apply plus an
		// Update: two config pushes where one belongs.
		return r.Update(ctx, obj, client.FieldOwner(fieldManagerName))
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
	seen := map[string]bool{}
	for _, k := range keys {
		v := strings.TrimSpace(string(sec.Data[k]))
		if v == "" {
			continue // an empty value would be a credential that matches nothing
		}
		// Deduplicated, comparing with the prefix normalised. The same value under two
		// Secret keys is ordinary mid-rotation (copy `current` to `next`, then edit it), and
		// it used to arrive as one consumer carrying a credential twice: the collision check
		// then saw two claimants on one credential - both from this same object, so both
		// "incumbents" - and refused the round for the whole gateway, with a message about
		// two consumers that named one object twice.
		if norm := strings.TrimPrefix(v, bearerPrefixValue); seen[norm] {
			continue
		} else {
			seen[norm] = true
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
	if err := r.gatewayReader().Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
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

// credentialsNotCovered: how many of a live entry's credentials the rendered consumer does not
// carry. 0 means the writer demonstrably holds every key that entry already accepts.
func credentialsNotCovered(live map[string]any, rendered map[string]bool) int {
	creds, _ := live["credentials"].([]any)
	// Compared with the prefix stripped from BOTH sides. The rendered set is already
	// prefixed, so a live entry stored bare could never be matched by any Secret content:
	// `sk-x` renders as `Bearer sk-x`, and `Bearer sk-x` renders as the same thing, so the
	// proof was impossible and the refusal told the operator to add a key they had already
	// added. Possession of the key is the question here; how the entry spells it is not.
	//
	// Live entries do carry the prefix on these clusters (the gateway chart requires it), but
	// a hand-written one need not, and ModelFleet's own checkAuth strips it for the same
	// reason.
	bare := make(map[string]bool, len(rendered))
	for c := range rendered {
		bare[strings.TrimPrefix(c, bearerPrefixValue)] = true
	}
	missing := 0
	for _, c := range creds {
		key, ok := c.(string)
		if !ok || !bare[strings.TrimPrefix(key, bearerPrefixValue)] {
			missing++
		}
	}
	return missing
}

// bearerPrefixValue mirrors sink's prefix. Duplicated rather than exported, because the sink
// owns how a credential is RENDERED and this is only about recognising one.
const bearerPrefixValue = "Bearer "

func liveCredentialCount(live map[string]any) int {
	creds, _ := live["credentials"].([]any)
	return len(creds)
}

// liveConsumerState: what this gateway's key-auth currently holds - each consumer name with
// its credentials - and which of those names this controller recorded as its own.
func (r *APIConsumerReconciler) liveConsumerState(ctx context.Context, gwNS string) (
	map[string][]string, map[string]bool, error) {
	entries := map[string][]string{}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(wasmPluginGVK)
	if err := r.gatewayReader().Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
		if isAbsent(err) {
			return entries, nil, fmt.Errorf("key-auth in namespace %q: %w", gwNS, errGatewayAbsent)
		}
		return entries, nil, err
	}
	managed, err := managedConsumerNames(obj)
	if err != nil {
		return entries, nil, err
	}
	live, _, err := unstructured.NestedSlice(obj.Object, "spec", "defaultConfig", "consumers")
	if err != nil {
		return entries, managed, err
	}
	for _, lc := range live {
		m, ok := lc.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		if name == "" {
			continue
		}
		creds, _ := m["credentials"].([]any)
		vals := make([]string, 0, len(creds))
		for _, c := range creds {
			if v, ok := c.(string); ok {
				vals = append(vals, v)
			}
		}
		entries[name] = vals
	}
	return entries, managed, nil
}

// liveConsumerNames: the consumer names this gateway's key-auth currently holds.
func (r *APIConsumerReconciler) liveConsumerNames(ctx context.Context, gwNS string) (map[string]bool, error) {
	out := map[string]bool{}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(wasmPluginGVK)
	if err := r.gatewayReader().Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
		if isAbsent(err) {
			return out, fmt.Errorf("key-auth in namespace %q: %w", gwNS, errGatewayAbsent)
		}
		return out, err
	}
	live, _, err := unstructured.NestedSlice(obj.Object, "spec", "defaultConfig", "consumers")
	if err != nil {
		return out, err
	}
	for _, lc := range live {
		if m, ok := lc.(map[string]any); ok {
			if n, _ := m["name"].(string); n != "" {
				out[n] = true
			}
		}
	}
	return out, nil
}

func hasLiveEntry(names map[string]bool, name string) bool { return names[name] }

// KeyAuthReconciler watches each gateway's key-auth WasmPlugin, which is what makes this
// controller responsible for a field no APIConsumer may exist to trigger.
//
// Three things need it, and the APIConsumer watch covers none of them:
//
//   - A fresh install. helm creates key-auth WITHOUT a consumers key (the chart stopped
//     rendering it), and nobody has created an APIConsumer yet. key_auth cannot parse a
//     defaultConfig with neither consumers nor credentials, and the plugin is FAIL_CLOSE, so
//     the gateway refuses everything until the key exists. With only an APIConsumer watch,
//     zero objects means zero reconciles and it never does.
//   - key-auth being deleted and recreated (uninstall/install, or by hand). Every consumer
//     entry goes with it. The objects still exist, so their own resync would restore them
//     within the resync interval, but a watch makes it immediate - and with zero objects the
//     missing key would again never be written.
//   - The namespace question. With no APIConsumer there is nothing to say WHICH gateway to
//     look at. The event answers it: this fires for every key-auth in the cluster, so no
//     configured default is needed and none is guessed.
type KeyAuthReconciler struct {
	*APIConsumerReconciler
}

func (r *KeyAuthReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	// Same path as an APIConsumer round, deliberately: it renders every APIConsumer for this
	// gateway AND guarantees the consumers key exists. Re-deriving a smaller "just ensure the
	// key" write here would be a second way to write one field.
	if _, err := r.programGateway(ctx, req.Namespace, nil); err != nil {
		if errors.Is(err, errGatewayAbsent) {
			return ctrl.Result{}, nil // deleted again; the next create re-triggers this
		}
		log.Error(err, "could not program the gateway from a key-auth event", "gateway", req.Namespace)
		return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
	}
	return ctrl.Result{}, nil
}

func (r *KeyAuthReconciler) SetupWithManager(mgr ctrl.Manager) error {
	keyAuth := &unstructured.Unstructured{}
	keyAuth.SetGroupVersionKind(wasmPluginGVK)
	return ctrl.NewControllerManagedBy(mgr).
		For(keyAuth, builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
			// Only the plugin this controller writes. Every other WasmPlugin on the cluster
			// would otherwise enqueue a gateway round that has nothing to do.
			return o.GetName() == "key-auth"
		}))).
		Named("keyauth").
		Complete(r)
}

// credentialsReformatted: would adoption keep the same key but change how it is spelled? True
// when a live credential lacks the prefix the rendered one carries.
func credentialsReformatted(live map[string]any, rendered map[string]bool) bool {
	creds, _ := live["credentials"].([]any)
	for _, c := range creds {
		key, ok := c.(string)
		if !ok || strings.HasPrefix(key, bearerPrefixValue) {
			continue
		}
		if rendered[bearerPrefixValue+key] {
			return true
		}
	}
	return false
}

// dropUnprovenAdoptions leaves out any view that claims a consumer name already present in
// the gateway and owned by no APIConsumer, unless it carries every credential that entry
// accepts. Comparison ignores the `Bearer ` prefix on either side: possession of the key is
// the question, how the entry spells it is not.
func dropUnprovenAdoptions(views []sink.ConsumerView, liveEntries map[string][]string,
	liveManaged, claimed map[string]bool, round gatewayRound, gwNS string) []sink.ConsumerView {
	kept := make([]sink.ConsumerView, 0, len(views))
	for _, v := range views {
		existing, present := liveEntries[v.Name]
		if !present || liveManaged[v.Name] || claimed[v.Name] {
			kept = append(kept, v) // nothing there, or already ours: a rotation
			continue
		}
		have := make(map[string]bool, len(v.Credentials))
		for _, c := range v.Credentials {
			have[strings.TrimPrefix(c, bearerPrefixValue)] = true
		}
		missing := 0
		for _, c := range existing {
			if !have[strings.TrimPrefix(c, bearerPrefixValue)] {
				missing++
			}
		}
		if missing == 0 {
			kept = append(kept, v)
			continue
		}
		round.skipped[v.Source] = fmt.Sprintf(
			"consumer %q already exists in gateway %s and is managed by no APIConsumer, and "+
				"this object does not carry %d of its %d existing credential(s). Adopting an "+
				"existing consumer requires its current key(s), so that taking over a name "+
				"cannot grant someone else's access. Put the existing key in this object's "+
				"Secret to adopt it and rotate afterwards, or give it a different "+
				"consumerName. This object was left out of the gateway; everything else on "+
				"it is unaffected", v.Name, gwNS, missing, len(existing))
	}
	return kept
}

// ensureGateways runs one gateway round for every key-auth WasmPlugin in the cluster. It is
// what the periodic sweep below calls, and it exists separately from the watch for one
// reason: the watch cannot be registered at all where the WasmPlugin kind is not served yet,
// and that is the normal state on a fresh install.
//
// Higress's CRDs come from the higress CORE release, while the helmfile deploys autoconfig
// BEFORE higress-llm (which needs it). So autoconfig can start in a cluster that has no
// WasmPlugin kind, skip the watch - correctly, or the manager would exit - and then nothing
// would ever write key-auth's consumers key. The gateway would be FAIL_CLOSE and stay that
// way until somebody restarted autoconfig, which is a worse failure than the crash-loop the
// gate prevents, because nothing announces it.
//
// Uncached List, through APIReader: a cached one would build an informer for a kind that may
// not exist, which is the problem this is here to avoid. A missing kind is just a failed List
// and the next sweep tries again, so a Higress installed later is picked up on its own.
func (r *APIConsumerReconciler) ensureGateways(ctx context.Context) {
	log := logf.FromContext(ctx)
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(wasmPluginGVK.GroupVersion().WithKind("WasmPluginList"))
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.List(ctx, list); err != nil {
		if isAbsent(err) {
			return // no Higress (yet): nothing to do, and nothing to complain about
		}
		log.V(1).Info("could not list WasmPlugins for the gateway sweep", "err", err.Error())
		return
	}
	for i := range list.Items {
		o := &list.Items[i]
		if o.GetName() != "key-auth" {
			continue
		}
		if _, err := r.programGateway(ctx, o.GetNamespace(), nil); err != nil &&
			!errors.Is(err, errGatewayAbsent) {
			log.V(1).Info("gateway sweep did not program this gateway",
				"gateway", o.GetNamespace(), "err", err.Error())
		}
	}
}

// StartGatewaySweep is a manager Runnable: it keeps the consumers key present even on a
// cluster where the watch could not be registered, and picks up a Higress installed after
// this process started. One round per interval over every key-auth; each round writes only
// when something differs, so a settled cluster costs a List and a few cached reads.
func (r *APIConsumerReconciler) StartGatewaySweep(ctx context.Context) error {
	t := time.NewTicker(consumerResyncTime)
	defer t.Stop()
	r.ensureGateways(ctx) // once at startup, so a fresh install does not wait an interval
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.ensureGateways(ctx)
		}
	}
}
