// APIConsumer: who may call the gateway, and with which API keys.
//
// Every APIConsumer naming one gateway namespace is rendered, together, into that gateway's
// key-auth defaultConfig.consumers. One object per caller, one write per gateway.
package controller

import (
	"context"
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
)

// ModelFleetReconciler already defines wasmPluginGVK, gatewayNamespace and isAbsent; this
// controller shares them deliberately — a second definition is a second thing to keep in step.

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
			for _, ns := range distinctNamespaces(gwNS, consumer.Status.AppliedGatewayNamespace) {
				if err := r.programGateway(ctx, ns, &consumer); err != nil {
					return ctrl.Result{}, err
				}
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
		if err := r.programGateway(ctx, old, nil); err != nil {
			r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionFalse, "WithdrawError",
				fmt.Sprintf("could not take this consumer out of its previous gateway %q: %v", old, err), 0, "")
			return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
		}
	}

	if err := r.programGateway(ctx, gwNS, nil); err != nil {
		r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionFalse, "ProgramError", err.Error(), 0, "")
		return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
	}

	name := consumerName(&consumer)
	creds, err := r.readCredentials(ctx, &consumer)
	if err != nil {
		// Cannot happen: programGateway just read the same Secret successfully. Reported
		// rather than ignored, because "cannot happen" and "is not checked" are different.
		r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionFalse, "SecretError", err.Error(), 0, name)
		return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
	}
	log.V(1).Info("consumer programmed", "consumer", name, "gateway", gwNS, "credentials", len(creds))
	r.setConsumerStatus(ctx, req.NamespacedName, metav1.ConditionTrue, "Programmed",
		fmt.Sprintf("%d credential(s) accepted by the gateway in %s", len(creds), gwNS),
		int32(len(creds)), name)
	return ctrl.Result{RequeueAfter: consumerResyncTime}, nil
}

// programGateway renders every APIConsumer that names this gateway namespace into its
// key-auth plugin, in one write.
//
// `excluding` is the object being deleted: it still exists in the API server while its
// finalizer runs, so it has to be left out explicitly.
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
	for i := range list.Items {
		if list.Items[i].Status.AppliedGatewayNamespace == gwNS {
			ownedBefore = true
			break
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
	return r.writeConsumers(ctx, gwNS, consumers, ownedBefore)
}

// writeConsumers replaces defaultConfig.consumers in the gateway's key-auth plugin, leaving
// every other key in that config alone.
func (r *APIConsumerReconciler) writeConsumers(ctx context.Context, gwNS string, consumers []sink.KeyAuthConsumer, ownedBefore bool) error {
	log := logf.FromContext(ctx)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(wasmPluginGVK)
		if err := r.Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
			return fmt.Errorf("key-auth in namespace %q: %w", gwNS, err)
		}
		live, _, err := unstructured.NestedSlice(obj.Object, "spec", "defaultConfig", "consumers")
		if err != nil {
			return fmt.Errorf("read defaultConfig.consumers: %w", err)
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
		for _, c := range consumers {
			m, err := sink.ToMap(c)
			if err != nil {
				return err
			}
			next = append(next, m)
		}
		if sameRules(live, next) {
			// Unchanged: do not write. Every write here is an ECDS push of key-auth to the
			// whole data plane, and the console calls this on every key edit.
			return nil
		}
		if err := unstructured.SetNestedSlice(obj.Object, next, "spec", "defaultConfig", "consumers"); err != nil {
			return err
		}
		log.Info("programming gateway consumers", "gateway", gwNS, "consumers", len(next), "was", len(live))
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

func (r *APIConsumerReconciler) setConsumerStatus(ctx context.Context, nn types.NamespacedName,
	status metav1.ConditionStatus, reason, msg string, creds int32, name string) {
	log := logf.FromContext(ctx)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var c routingv1.APIConsumer
		if err := r.Get(ctx, nn, &c); err != nil {
			return err
		}
		st := &c.Status
		if status == metav1.ConditionTrue {
			// Only on success: observedGeneration is how the console answers "is this key
			// live yet", and advancing it on a failed round says yes while the gateway still
			// carries the old list.
			st.ObservedGeneration = c.Generation
			st.AppliedGatewayNamespace = consumerGatewayNamespace(&c)
			st.ConsumerName = name
			st.Credentials = creds
		}
		now := metav1.Now()
		st.LastSyncTime = &now
		setConsumerCond(st, status, reason, msg, c.Generation)
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
