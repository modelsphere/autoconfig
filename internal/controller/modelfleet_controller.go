// ModelFleet reconciler: watch ModelFleet → resolve each member's ModelRoute → render the
// Higress objects that make the members one model behind one address → write status back.
//
// Why here and not in the gateway's own control plane: Higress decides which config sources it
// reads inside pkg/ingress/kube, with no registration point for another translator, so teaching
// it this CR means carrying a patch through every upstream bump. Half of this reconcile is not
// the gateway's business anyway — resolving ModelRoutes, summing capacity, renormalising
// weights, writing status.
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	routingv1 "autoconfig/api/v1alpha1"
	"autoconfig/internal/sink"
)

const (
	fleetFinalizer = "routing.modelsphere.dev/fleet-cleanup"
	// fleetResyncEvery: fallback resync. The gateway's own config push is seconds-scale, and
	// a member's readiness changes without this CR changing, so we re-render periodically
	// rather than only on events.
	fleetResyncEvery  = 30 * time.Second
	condGatewaySynced = "GatewaySynced"
	condPolicyApplied = "PolicyApplied"
	condReady         = "Ready"

	defaultGatewayNamespace = "higress-system"
	defaultUpstreamService  = "openresty"
	defaultUpstreamPort     = 8080
	// defaultUpstreamSecret: where the openresty API key already lives on these clusters, as
	// a Secret mounted into the openresty pods. Defaulting to it keeps the credential out of
	// every ModelFleet: the gateway needs the same key as every other caller of openresty, so
	// naming it per fleet was asking each fleet to repeat one cluster-wide fact.
	defaultUpstreamSecret = "openresty-api-keys:keys"
	// upstreamAuthNone: the explicit opt-out, for an openresty with auth turned off.
	upstreamAuthNone = "none"

	// modelHeader: model-router copies body.model into this header before routing, because
	// Envoy cannot match on a body field. Every per-model Ingress matches on it.
	modelHeader = "x-higress-llm-model"
)

// wasmPluginGVK: Higress's WasmPlugin. Handled unstructured on purpose — pulling in the
// Higress API module for one CRD would tie this controller's build to the gateway's release
// cadence, and the only part of the object we touch is a list of match rules.
var wasmPluginGVK = schema.GroupVersionKind{
	Group: "extensions.higress.io", Version: "v1alpha1", Kind: "WasmPlugin",
}

// fleetPlugin is one of the gateway plugins whose match rules this controller owns per route.
type fleetPlugin struct {
	// name of the WasmPlugin object in the gateway namespace.
	name string
	// tolerateMissing: may this plugin be absent even though we had a rule to write?
	//
	// Normally no: a fleet that asked for an allow-list and got no key-auth is an
	// unauthenticated route, and that is worse as a silent skip than as a failed sync.
	// Having nothing to write is always tolerated - that is just a plugin this cluster does
	// not use. The exceptions are per-plugin and depend on what the fleet actually asked
	// for, so this is a decision rather than a constant.
	tolerateMissing func(*fleetRender) bool
	// config renders this plugin's per-route config; returning nil means "no rule for this
	// fleet", which removes any rule we previously wrote.
	config func(*fleetRender) (any, error)
}

// ModelFleetReconciler reconciles ModelFleet.
type ModelFleetReconciler struct {
	client.Client
	// APIReader reads straight from the API server, bypassing the manager's cache. Secrets go
	// through it: a cached Get on a Secret starts an informer over EVERY Secret in the
	// cluster, which is a memory footprint and a blast radius out of all proportion to
	// needing one key. Nil falls back to the cached client, which keeps the tests simple.
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// reader returns the uncached reader when one is wired up.
func (r *ModelFleetReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// +kubebuilder:rbac:groups=routing.modelsphere.dev,resources=modelfleets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=routing.modelsphere.dev,resources=modelfleets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=routing.modelsphere.dev,resources=modelfleets/finalizers,verbs=update
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=extensions.higress.io,resources=wasmplugins,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

// fleetRender is everything the renderers need, resolved once per reconcile.
type fleetRender struct {
	fleet       *routingv1.ModelFleet
	modelName   string
	ingressRef  string // "ns/name", how a WasmPlugin match rule names our Ingress
	members     []sink.FleetMemberView
	effective   map[string]int32
	upstreamKey string
	// upstreamKeyDefaulted: the credential came from the default Secret, not from a named one.
	upstreamKeyDefaulted bool
	// appliedExtras: the extraPlugins keys written this round, for status.
	appliedExtras []string
	// orphans: trafficPolicy entries naming members that no longer exist. Reported, never
	// rejected: refusing the policy would deadlock console's member delete against
	// decision-gen's stale policy, with neither side able to write first.
	orphans []string
}

func (r *ModelFleetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var fleet routingv1.ModelFleet
	if err := r.Get(ctx, req.NamespacedName, &fleet); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !fleet.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&fleet, fleetFinalizer) {
			// The Ingress is garbage-collected through its owner reference; the plugin match
			// rules live in another namespace and in objects we do not own, so they have to be
			// withdrawn by hand before we let the object go.
			if err := r.cleanupPluginRules(ctx, &fleet); err != nil {
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&fleet, fleetFinalizer)
			if err := r.Update(ctx, &fleet); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&fleet, fleetFinalizer) {
		controllerutil.AddFinalizer(&fleet, fleetFinalizer)
		if err := r.Update(ctx, &fleet); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	rend, err := r.resolve(ctx, &fleet)
	if err != nil {
		r.setFleetStatus(ctx, req.NamespacedName, nil, condGatewaySynced, metav1.ConditionFalse, "ResolveError", err.Error())
		return ctrl.Result{RequeueAfter: fleetResyncEvery}, nil
	}

	// Plugin rules first, Ingress last. The Ingress is what makes the route reachable, so
	// everything that guards it has to be in place before it exists: writing the Ingress
	// first means that a failure half way through the plugins publishes a live route whose
	// key-auth rule was never written — unauthenticated, answering 200, with nothing but a
	// GatewaySynced=False on the CR to say so.
	//
	// A match rule naming an Ingress that does not exist yet is inert, so this order has no
	// window of its own.
	if err := r.applyPluginRules(ctx, rend); err != nil {
		r.setFleetStatus(ctx, req.NamespacedName, rend, condGatewaySynced, metav1.ConditionFalse, "PluginError", err.Error())
		return ctrl.Result{RequeueAfter: fleetResyncEvery}, nil
	}
	if err := r.applyIngress(ctx, rend); err != nil {
		r.setFleetStatus(ctx, req.NamespacedName, rend, condGatewaySynced, metav1.ConditionFalse, "IngressError", err.Error())
		return ctrl.Result{RequeueAfter: fleetResyncEvery}, nil
	}

	log.V(1).Info("fleet synced", "model", rend.modelName, "members", len(rend.members))
	r.setFleetStatus(ctx, req.NamespacedName, rend, condGatewaySynced, metav1.ConditionTrue, "Synced", "gateway objects written")
	return ctrl.Result{RequeueAfter: fleetResyncEvery}, nil
}

// resolve joins the CR's intent with the live state of each member's ModelRoute.
func (r *ModelFleetReconciler) resolve(ctx context.Context, fleet *routingv1.ModelFleet) (*fleetRender, error) {
	rend := &fleetRender{
		fleet:      fleet,
		modelName:  fleet.Spec.ModelName,
		ingressRef: fleet.Namespace + "/" + ingressName(fleet),
	}
	if rend.modelName == "" {
		rend.modelName = fleet.Name
	}

	policy := map[string]routingv1.PolicyMember{}
	if p := fleet.Spec.TrafficPolicy; p != nil {
		for _, pm := range p.Members {
			policy[pm.Name] = pm
		}
	}
	declared := map[string]bool{}

	for _, m := range fleet.Spec.Members {
		declared[m.Name] = true
		view := sink.FleetMemberView{Name: m.Name}
		if pm, ok := policy[m.Name]; ok {
			view.Tier, view.Weight = pm.Tier, pm.Weight
		}
		if m.Capabilities != nil {
			view.MaxConcurrency = m.Capabilities.MaxConcurrency
		}

		var mr routingv1.ModelRoute
		ns, name := splitRef(m.Backend.ModelRouteRef, fleet.Namespace)
		if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &mr); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("member %s: get ModelRoute %s/%s: %w", m.Name, ns, name, err)
			}
			// Left not-ready with no path: the member simply does not enter the table.
			rend.members = append(rend.members, view)
			continue
		}
		route := mr.Spec.Nginx.Route
		if route == "" {
			route = mr.Name
		}
		view.Path = "/" + route
		// Readiness is taken from the ModelRoute's own status rather than rediscovered here:
		// that controller already watches the EndpointSlices, and a second discovery pass
		// would give two answers to one question whenever the two disagree.
		view.Ready = mr.Status.Ready && mr.Status.Backends > 0
		view.ReadyReplicas = int32(mr.Status.Backends)
		rend.members = append(rend.members, view)
	}

	if p := fleet.Spec.TrafficPolicy; p != nil {
		for _, pm := range p.Members {
			if !declared[pm.Name] {
				rend.orphans = append(rend.orphans, pm.Name)
			}
		}
		sort.Strings(rend.orphans)
	}
	// Status reports the share actually in force, which differs from the table's weights
	// when everything falls through to the default member (see sink.TrafficShares).
	rend.effective = sink.TrafficShares(rend.members, fleet.Spec.DefaultMember)

	switch ref := fleet.Spec.Gateway.UpstreamAuthSecretRef; ref {
	case upstreamAuthNone:
		// openresty is not asking for a key on this cluster.
	default:
		defaulted := ref == ""
		rend.upstreamKeyDefaulted = defaulted
		if defaulted {
			ref = defaultUpstreamSecret
		}
		key, err := r.readSecretKey(ctx, ref, fleet.Namespace)
		if err != nil {
			if defaulted {
				// Do not fall through to "no credential": openresty would answer 401 on the
				// first real request, long after this reconcile looked fine.
				return nil, fmt.Errorf("%w (default upstream credential; set "+
					"gateway.upstreamAuthSecretRef to another Secret, or to %q if this "+
					"openresty needs no key)", err, upstreamAuthNone)
			}
			return nil, err
		}
		if defaulted {
			// That Secret is openresty's own key file, and its format is
			// "key[:owner][,key[:owner]...]" - not a bare token. Sending the file's contents
			// as the bearer token means sending "sk-...:admin", which openresty rejects with
			// 401 on every request while this reconcile reports a healthy sync. Measured on a
			// test cluster: the gateway presented exactly the bytes in openresty's own key
			// file and openresty refused them.
			//
			// Only for the defaulted ref. An explicitly named Secret was made for this
			// purpose and holds the token verbatim, and guessing at the format of a
			// credential someone handed us is worse than using it as given.
			if key, err = firstOpenrestyAPIKey(key); err != nil {
				return nil, fmt.Errorf("%s: %w", ref, err)
			}
		}
		rend.upstreamKey = key
	}
	if err := r.checkAuth(ctx, fleet, rend); err != nil {
		return nil, err
	}
	return rend, nil
}

// firstOpenrestyAPIKey takes the first key out of openresty's api-keys file, whose format is
// "key[:owner][,key[:owner]...]" - the same parse openresty itself does, so a file it accepts
// is a file this understands. Any listed key is accepted upstream (that is what makes
// rotation possible), so the first one is as good as any.
func firstOpenrestyAPIKey(spec string) (string, error) {
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if i := strings.Index(item, ":"); i >= 0 {
			item = strings.TrimSpace(item[:i]) // "key:owner" - the owner is a label, not part of the key
		}
		if item != "" {
			return item, nil
		}
	}
	return "", fmt.Errorf("no api key in the openresty key file (expected \"key[:owner][,...]\")")
}

// checkAuth validates this fleet's auth settings against the gateway's own key-auth config,
// because the plugin itself will not: an allow-list naming a consumer that does not exist is
// accepted by the API server and by the plugin, and simply denies everyone with 403 while the
// CR reports a healthy sync. The gateway chart used to catch this at render time; now that the
// allow-list is written here, the check belongs here too.
func (r *ModelFleetReconciler) checkAuth(ctx context.Context, fleet *routingv1.ModelFleet, rend *fleetRender) error {
	if len(fleet.Spec.Gateway.AuthConsumers) == 0 && rend.upstreamKey == "" {
		return nil
	}
	gwNS := gatewayNamespace(fleet)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(wasmPluginGVK)
	if err := r.Get(ctx, types.NamespacedName{Namespace: gwNS, Name: "key-auth"}, obj); err != nil {
		if apierrors.IsNotFound(err) && len(fleet.Spec.Gateway.AuthConsumers) == 0 {
			return nil // nothing to check; the missing plugin is handled where rules are written
		}
		// Name the namespace: gateway.namespace defaults to higress-system and must match the
		// namespace the gateway chart installs into, and a bare NotFound makes that look like
		// a broken plugin rather than a namespace that does not line up.
		return fmt.Errorf("key-auth in namespace %q (spec.gateway.namespace): %w", gwNS, err)
	}
	consumers, _, _ := unstructured.NestedSlice(obj.Object, "spec", "defaultConfig", "consumers")
	known := map[string]bool{}
	creds := map[string]string{}
	for _, c := range consumers {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(m, "name")
		known[name] = true
		cs, _, _ := unstructured.NestedStringSlice(m, "credentials")
		for _, cred := range cs {
			creds[strings.TrimPrefix(cred, "Bearer ")] = name
		}
	}
	var unknown []string
	for _, want := range fleet.Spec.Gateway.AuthConsumers {
		if !known[want] {
			unknown = append(unknown, want)
		}
	}
	if len(unknown) > 0 {
		return fmt.Errorf("gateway.authConsumers name consumers that key-auth in %q does not define: %s "+
			"(the route would be published and then deny every caller with 403)",
			gwNS, strings.Join(unknown, ", "))
	}
	// The upstream credential is what the gateway presents to openresty. If it is also a
	// consumer's credential at the gateway, then anyone who learns it can call the gateway as
	// that consumer — one leak, two doors.
	if rend.upstreamKey != "" {
		if who, ok := creds[rend.upstreamKey]; ok {
			return fmt.Errorf("gateway.upstreamAuthSecretRef holds the same credential as key-auth consumer %q; "+
				"the upstream key must not double as a caller credential", who)
		}
	}
	return nil
}

// applyIngress writes the per-model Ingress: match the header model-router filled in, send
// everything to openresty, and deliberately set no rewrite-path — the path is the router
// plugin's to choose per member, and an Ingress-level rewrite would pin every member to one.
func (r *ModelFleetReconciler) applyIngress(ctx context.Context, rend *fleetRender) error {
	fleet := rend.fleet
	svcName, svcPort := upstreamService(fleet)
	pathType := networkingv1.PathTypePrefix

	ing := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{
		Name: ingressName(fleet), Namespace: fleet.Namespace,
	}}
	// CreateOrUpdate rather than Server-Side Apply: it writes only when the object actually
	// changed, which matters because every write here is a config push to the gateway. The
	// object is ours alone (named after the fleet, owned by it), so there is no second writer
	// whose fields an apply would have to protect.
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ing, func() error {
		if ing.Annotations == nil {
			ing.Annotations = map[string]string{}
		}
		ing.Annotations["higress.io/exact-match-header-"+modelHeader] = rend.modelName
		// Long-lived streaming responses: the defaults cut a generation short.
		ing.Annotations["higress.io/proxy-read-timeout"] = "3600"
		ing.Annotations["higress.io/proxy-send-timeout"] = "3600"
		// No silent failover between members: a retry chosen by the proxy would bypass the
		// tier logic the router plugin exists to apply.
		ing.Annotations["higress.io/proxy-next-upstream"] = "off"
		// Deliberately NOT set: higress.io/rewrite-path. The per-member path is the router
		// plugin's to choose; an Ingress-level rewrite would pin every member to one path.

		if ing.Labels == nil {
			ing.Labels = map[string]string{}
		}
		ing.Labels["app.kubernetes.io/managed-by"] = "autoconfig"
		ing.Labels["routing.modelsphere.dev/fleet"] = fleet.Name

		ing.Spec.IngressClassName = strPtr(gatewayClass(fleet))
		ing.Spec.Rules = []networkingv1.IngressRule{{
			IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
				Paths: []networkingv1.HTTPIngressPath{{
					Path:     "/",
					PathType: &pathType,
					Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
						Name: svcName,
						Port: networkingv1.ServiceBackendPort{Number: svcPort},
					}},
				}},
			}},
		}}
		// Owner reference, so deleting the fleet takes the Ingress with it; the plugin match
		// rules are in another namespace and need the finalizer instead.
		return controllerutil.SetControllerReference(fleet, ing, r.Scheme)
	})
	return err
}

// fleetPlugins is the set of gateway plugins that need a rule per fleet route. Each entry is
// one object we edit; leaving one out does not fail loudly, it fails as 401s or as a model
// with no metrics, which is why the table lists what each one is for.
func fleetPlugins() []fleetPlugin {
	return []fleetPlugin{{
		// Without a rule here there is no backend table at all, so nothing can be split.
		name: "ai-backend-router",
		config: func(f *fleetRender) (any, error) {
			rev := int64(0)
			if p := f.fleet.Spec.TrafficPolicy; p != nil {
				rev = p.Revision
			}
			return sink.RenderDecisionTable(f.modelName, f.fleet.Spec.DefaultMember, f.members, rev)
		},
	}, {
		// Without a rule here the route is not in any consumer's allow-list: 403. Missing
		// plugin + a requested allow-list = an unauthenticated route, so that fails the sync.
		name: "key-auth",
		config: func(f *fleetRender) (any, error) {
			if len(f.fleet.Spec.Gateway.AuthConsumers) == 0 {
				return nil, nil
			}
			return sink.RenderKeyAuth(f.fleet.Spec.Gateway.AuthConsumers), nil
		},
	}, {
		// Without a rule here the gateway presents no credential to openresty: 401.
		// Tolerated only when the credential was DEFAULTED rather than asked for: a cluster
		// that never deployed ai-proxy and never named a Secret is one where openresty takes
		// no key, and defaulting must not turn that into a fleet that refuses to publish.
		// An explicit upstreamAuthSecretRef is an instruction, and is enforced.
		name:            "ai-proxy",
		tolerateMissing: func(f *fleetRender) bool { return f.upstreamKeyDefaulted },
		config: func(f *fleetRender) (any, error) {
			if f.upstreamKey == "" {
				return nil, nil
			}
			return sink.RenderAIProxy(f.upstreamKey)
		},
	}, {
		// Without a rule here the model simply has no token accounting: its absence costs
		// metrics and nothing else, so it is always tolerated.
		name:            "ai-statistics",
		tolerateMissing: func(*fleetRender) bool { return true },
		config:          func(f *fleetRender) (any, error) { return sink.RenderAIStatistics(), nil },
	}}
}

// renderedPlugins: the plugins this controller renders itself. The passthrough path must
// never write to one of these - the CRD refuses it at apply time, and this is the belt to
// that pair of braces, since a stale CRD on an older cluster would otherwise let it through.
func renderedPlugins() map[string]bool {
	out := map[string]bool{}
	for _, p := range fleetPlugins() {
		if p.tolerateMissing == nil { // the three whose config we compute; ai-statistics may be overridden
			out[p.name] = true
		}
	}
	return out
}

func (r *ModelFleetReconciler) applyPluginRules(ctx context.Context, rend *fleetRender) error {
	var errs []string
	extra := rend.fleet.Spec.Gateway.ExtraPlugins
	rendered := renderedPlugins()
	for _, p := range fleetPlugins() {
		// An OVERRIDABLE plugin that the fleet passes through is the fleet's, not ours.
		// Rendering it here too would write the object twice per reconcile - our default,
		// then theirs - which is two config pushes to the gateway every resync plus a
		// window where our default is live instead of what they asked for.
		//
		// Only overridable ones: the three we compute must still be written even when a
		// stale CRD let them into extraPlugins, or passing one through would stop the real
		// table, allow-list or credential from being programmed at all.
		if _, overridden := extra[p.name]; overridden && !rendered[p.name] {
			continue
		}
		cfg, err := p.config(rend)
		if err != nil {
			// A render error must not be written: the plugin validates its config at the
			// gateway, not at the API server, and one plugin that fails to load stalls the
			// config push for every plugin on that gateway.
			errs = append(errs, fmt.Sprintf("%s: render: %v", p.name, err))
			continue
		}
		var raw map[string]any
		if cfg != nil {
			if raw, err = sink.ToMap(cfg); err != nil {
				errs = append(errs, fmt.Sprintf("%s: encode: %v", p.name, err))
				continue
			}
		}
		if err := r.upsertMatchRule(ctx, rend, p, raw); err != nil {
			if apierrors.IsNotFound(err) && (raw == nil ||
				(p.tolerateMissing != nil && p.tolerateMissing(rend))) {
				continue
			}
			errs = append(errs, fmt.Sprintf("%s: %v", p.name, err))
		}
	}
	// Passthrough plugins: whatever the fleet declared, written verbatim. Sorted so the work
	// is deterministic and an unchanged spec produces no writes.
	names := make([]string, 0, len(extra))
	for name := range extra {
		names = append(names, name)
	}
	sort.Strings(names)
	written := make([]string, 0, len(names))
	for _, name := range names {
		if rendered[name] {
			errs = append(errs, fmt.Sprintf("%s: rendered by this controller, cannot be passed through", name))
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal(extra[name].Raw, &raw); err != nil {
			errs = append(errs, fmt.Sprintf("%s: config is not an object: %v", name, err))
			continue
		}
		if raw == nil {
			// `plugin: null` unmarshals cleanly into a nil map, which downstream means
			// "withdraw our rule" - so an emptied entry would quietly take the plugin off a
			// live route while reporting a healthy sync. Declaring a plugin means giving it
			// a config; removing the key is how you take it away.
			errs = append(errs, fmt.Sprintf("%s: config is null; remove the key to withdraw the rule", name))
			continue
		}
		// Not optional: the fleet asked for this config, so a plugin that is not there means
		// the route goes live without something it was told to have.
		if err := r.upsertMatchRule(ctx, rend, fleetPlugin{name: name}, raw); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		written = append(written, name)
	}
	// Only the keys whose rules are actually on the gateway: status.appliedExtraPlugins is
	// what the next round withdraws from, so recording a key we refused or failed to write
	// would make a later reconcile withdraw a rule it never wrote.
	rend.appliedExtras = written

	// Keys that were in the spec last time and are not now: we wrote those rules, so we take
	// them back. Nothing else will - the reconciler only ever sees the current spec, so a
	// removed rate-limit entry would keep being enforced on a live route with nothing in the
	// CR to say so.
	// Keyed on what the spec asks for, not on what was written: a key whose write failed this
	// round is still wanted, and withdrawing it over a transient error would take a live rule
	// off the route on the way to retrying it.
	desired := map[string]bool{}
	for _, n := range names {
		desired[n] = true
	}
	builtin := map[string]bool{}
	for _, p := range fleetPlugins() {
		builtin[p.name] = true
	}
	for _, old := range rend.fleet.Status.AppliedExtraPlugins {
		// Never withdraw a plugin this controller writes itself. An overridable one
		// (ai-statistics) that the fleet stops overriding has just been written again by the
		// loop above, with our default - withdrawing it here would take it off the route
		// moments after programming it, so the route would lose token accounting until some
		// later reconcile happened to put it back.
		if desired[old] || rendered[old] || builtin[old] {
			continue
		}
		if err := r.upsertMatchRule(ctx, rend, fleetPlugin{name: old}, nil); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Sprintf("%s: withdraw: %v", old, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// upsertMatchRule replaces this fleet's rule inside one WasmPlugin, leaving every other rule
// untouched. cfg == nil withdraws our rule.
//
// Read-modify-write rather than Server-Side Apply, because `matchRules` is a plain array in the
// WasmPlugin CRD with no listType marker, which makes it atomic under SSA: an apply would
// replace the whole list and drop every rule written by the gateway chart. Our rule is
// identified by the Ingress it binds to, so re-running is idempotent and two fleets never
// collide.
func (r *ModelFleetReconciler) upsertMatchRule(ctx context.Context, rend *fleetRender, p fleetPlugin, cfg map[string]any) error {
	ns := gatewayNamespace(rend.fleet)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(wasmPluginGVK)
		if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: p.name}, obj); err != nil {
			return err
		}
		rules, _, err := unstructured.NestedSlice(obj.Object, "spec", "matchRules")
		if err != nil {
			return fmt.Errorf("read matchRules: %w", err)
		}
		kept := make([]any, 0, len(rules)+1)
		for _, item := range rules {
			if !ruleBindsTo(item, rend.ingressRef) {
				kept = append(kept, item)
			}
		}
		if cfg != nil {
			kept = append(kept, map[string]any{
				"ingress":       []any{rend.ingressRef},
				"configDisable": false,
				"config":        cfg,
			})
		}
		if sameRules(rules, kept) {
			// Nothing changed: do not write. Every write here is a config push to the
			// gateway, and a controller that rewrites an identical object on each resync
			// turns a 30s resync into a 30s config churn across the data plane.
			return nil
		}
		if err := unstructured.SetNestedSlice(obj.Object, kept, "spec", "matchRules"); err != nil {
			return err
		}
		return r.Update(ctx, obj)
	})
}

// cleanupPluginRules withdraws every rule this fleet put into the gateway plugins.
func (r *ModelFleetReconciler) cleanupPluginRules(ctx context.Context, fleet *routingv1.ModelFleet) error {
	rend := &fleetRender{fleet: fleet, ingressRef: fleet.Namespace + "/" + ingressName(fleet)}
	var errs []string
	plugins := fleetPlugins()
	// Passthrough plugins were written by us too, so they have to be withdrawn by us - both
	// the ones still in the spec and any we recorded having written.
	seen := map[string]bool{}
	for _, p := range plugins {
		seen[p.name] = true
	}
	for name := range fleet.Spec.Gateway.ExtraPlugins {
		if !seen[name] {
			plugins, seen[name] = append(plugins, fleetPlugin{name: name}), true
		}
	}
	for _, name := range fleet.Status.AppliedExtraPlugins {
		if !seen[name] {
			plugins, seen[name] = append(plugins, fleetPlugin{name: name}), true
		}
	}
	for _, p := range plugins {
		if err := r.upsertMatchRule(ctx, rend, p, nil); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Sprintf("%s: %v", p.name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("cleanup plugin rules: %s", strings.Join(errs, "; "))
	}
	return nil
}

func ruleBindsTo(item any, ingressRef string) bool {
	m, ok := item.(map[string]any)
	if !ok {
		return false
	}
	list, _, _ := unstructured.NestedStringSlice(m, "ingress")
	for _, v := range list {
		if v == ingressRef {
			return true
		}
	}
	return false
}

// sameRules compares two rule lists. A nil list and an empty one are the same list: the
// WasmPlugin may have no matchRules field at all, and json.Marshal renders those as "null"
// and "[]", which would make an unchanged plugin look changed and earn a pointless config
// push to the gateway on every resync.
func sameRules(a, b []any) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	ab, err1 := json.Marshal(a)
	bb, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(ab) == string(bb)
}

// setFleetStatus writes the observed state. Status is a subresource, so this never touches the
// spec and cannot race the console's or decision-gen's writes.
func (r *ModelFleetReconciler) setFleetStatus(ctx context.Context, nn types.NamespacedName, rend *fleetRender,
	syncType string, syncStatus metav1.ConditionStatus, reason, msg string) {
	log := logf.FromContext(ctx)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fleet routingv1.ModelFleet
		if err := r.Get(ctx, nn, &fleet); err != nil {
			return err
		}
		st := &fleet.Status
		if syncStatus == metav1.ConditionTrue {
			// Only on success. observedGeneration == generation is THE convergence signal
			// console and decision-gen poll; advancing it on a failed reconcile tells them
			// the new split is live while nothing was written and the table is frozen.
			st.ObservedGeneration = fleet.Generation
		}
		now := metav1.Now()
		st.LastSyncTime = &now

		if rend != nil {
			st.ModelName = rend.modelName
			if u := fleet.Spec.Gateway.ExternalURL; u != "" {
				st.Endpoint = strings.TrimSuffix(u, "/") + "/v1"
			} else {
				// Cleared or removed: blank it. status.endpoint is "the one address the
				// console shows", so leaving the old one there keeps a wrong address on
				// screen indefinitely.
				st.Endpoint = ""
			}
			st.Members = memberStatuses(&fleet, rend)
			st.AppliedExtraPlugins = rend.appliedExtras
			st.TotalCapacity = totalCapacity(st.Members, &fleet)

			policyOK, policyReason, policyMsg := metav1.ConditionTrue, "Applied", "traffic policy applied"
			if len(rend.orphans) > 0 {
				// Degraded, not failed: the rest of the policy is still applied.
				policyOK, policyReason = metav1.ConditionFalse, "OrphanMembers"
				policyMsg = "trafficPolicy references members that no longer exist, ignored: " +
					strings.Join(rend.orphans, ", ")
			}
			setCond(st, condPolicyApplied, policyOK, policyReason, policyMsg, fleet.Generation)
		}
		setCond(st, syncType, syncStatus, reason, msg, fleet.Generation)

		ready, readyReason, readyMsg := fleetReadiness(st, syncStatus)
		setCond(st, condReady, ready, readyReason, readyMsg, fleet.Generation)
		return r.Status().Update(ctx, &fleet)
	})
	if err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "update ModelFleet status")
	}
}

func memberStatuses(fleet *routingv1.ModelFleet, rend *fleetRender) []routingv1.MemberStatus {
	byName := map[string]sink.FleetMemberView{}
	for _, m := range rend.members {
		byName[m.Name] = m
	}
	out := make([]routingv1.MemberStatus, 0, len(fleet.Spec.Members))
	for _, spec := range fleet.Spec.Members {
		v := byName[spec.Name]
		phase := routingv1.MemberNotFound
		switch {
		case v.Ready:
			phase = routingv1.MemberReady
		case v.Path != "":
			// The ModelRoute exists; it just has nothing ready behind it.
			phase = routingv1.MemberDegraded
		}
		hw := spec.Hardware
		out = append(out, routingv1.MemberStatus{
			Name:            spec.Name,
			Phase:           phase,
			Hardware:        &hw,
			Path:            v.Path,
			ReadyReplicas:   v.ReadyReplicas,
			EffectiveWeight: rend.effective[spec.Name],
			Tier:            v.Tier,
		})
	}
	return out
}

// totalCapacity sums only the members that are actually ready: capacity is what can serve a
// request right now, and counting a drained member would overstate the fleet exactly when
// someone is looking at it to decide whether to shift traffic.
func totalCapacity(members []routingv1.MemberStatus, fleet *routingv1.ModelFleet) *routingv1.TotalCapacity {
	caps := map[string]int32{}
	for _, m := range fleet.Spec.Members {
		if m.Capabilities != nil {
			caps[m.Name] = m.Capabilities.MaxConcurrency
		}
	}
	var tc routingv1.TotalCapacity
	for _, m := range members {
		if m.Phase != routingv1.MemberReady {
			continue
		}
		tc.MaxConcurrency += caps[m.Name]
		tc.ReadyReplicas += m.ReadyReplicas
	}
	return &tc
}

// fleetReadiness: the fleet serves if the gateway is programmed and at least one member can
// take a request. One ready member out of three is still a serving fleet, so Ready stays true
// and the per-member Degraded phases carry the detail.
func fleetReadiness(st *routingv1.ModelFleetStatus, syncStatus metav1.ConditionStatus) (metav1.ConditionStatus, string, string) {
	if syncStatus != metav1.ConditionTrue {
		return metav1.ConditionFalse, "GatewayNotSynced", "gateway objects not written"
	}
	ready := 0
	for _, m := range st.Members {
		if m.Phase == routingv1.MemberReady {
			ready++
		}
	}
	if ready == 0 {
		return metav1.ConditionFalse, "NoReadyMember", "no member has a ready backend"
	}
	if ready < len(st.Members) {
		return metav1.ConditionTrue, "PartiallyReady",
			fmt.Sprintf("%d of %d members ready", ready, len(st.Members))
	}
	return metav1.ConditionTrue, "AllMembersReady", fmt.Sprintf("%d members ready", ready)
}

func setCond(st *routingv1.ModelFleetStatus, t string, s metav1.ConditionStatus, reason, msg string, gen int64) {
	for i := range st.Conditions {
		if st.Conditions[i].Type != t {
			continue
		}
		c := &st.Conditions[i]
		if c.Status != s {
			c.LastTransitionTime = metav1.Now()
		}
		c.Status, c.Reason, c.Message, c.ObservedGeneration = s, reason, msg, gen
		return
	}
	st.Conditions = append(st.Conditions, metav1.Condition{
		Type: t, Status: s, Reason: reason, Message: msg,
		ObservedGeneration: gen, LastTransitionTime: metav1.Now(),
	})
}

func (r *ModelFleetReconciler) readSecretKey(ctx context.Context, ref, defNS string) (string, error) {
	nameKey := ref
	ns := defNS
	if i := strings.Index(ref, "/"); i >= 0 {
		ns, nameKey = ref[:i], ref[i+1:]
	}
	i := strings.LastIndex(nameKey, ":")
	if i < 0 {
		return "", fmt.Errorf("upstreamAuthSecretRef %q: want [ns/]name:key", ref)
	}
	var sec corev1.Secret
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: ns, Name: nameKey[:i]}, &sec); err != nil {
		return "", fmt.Errorf("upstreamAuthSecretRef %q: %w", ref, err)
	}
	v, ok := sec.Data[nameKey[i+1:]]
	if !ok {
		return "", fmt.Errorf("upstreamAuthSecretRef %q: key %q not in secret", ref, nameKey[i+1:])
	}
	return strings.TrimSpace(string(v)), nil
}

func ingressName(f *routingv1.ModelFleet) string { return "llm-fleet-" + f.Name }

func gatewayClass(f *routingv1.ModelFleet) string {
	if f.Spec.Gateway.Class != "" {
		return f.Spec.Gateway.Class
	}
	return "higress"
}

func gatewayNamespace(f *routingv1.ModelFleet) string {
	if f.Spec.Gateway.Namespace != "" {
		return f.Spec.Gateway.Namespace
	}
	return defaultGatewayNamespace
}

// upstreamService parses gateway.service ("ns/name:port"); only the name and port are used,
// since the Ingress can only point at a Service in its own namespace.
func upstreamService(f *routingv1.ModelFleet) (string, int32) {
	s := f.Spec.Gateway.Service
	if s == "" {
		return defaultUpstreamService, defaultUpstreamPort
	}
	if i := strings.Index(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	port := int32(defaultUpstreamPort)
	if i := strings.LastIndex(s, ":"); i >= 0 {
		var p int
		if _, err := fmt.Sscanf(s[i+1:], "%d", &p); err == nil && p > 0 {
			port = int32(p)
		}
		s = s[:i]
	}
	if s == "" {
		s = defaultUpstreamService
	}
	return s, port
}

func splitRef(ref, defNS string) (string, string) {
	if i := strings.Index(ref, "/"); i >= 0 {
		return ref[:i], ref[i+1:]
	}
	return defNS, ref
}

func strPtr(s string) *string { return &s }

// SetupWithManager wires the watches: the CR itself, plus the ModelRoutes its members point at
// — a member going unready is a change to this fleet's effective weights even though nothing
// in the ModelFleet changed.
func (r *ModelFleetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&routingv1.ModelFleet{}, builder.WithPredicates()).
		Watches(&routingv1.ModelRoute{}, handler.EnqueueRequestsFromMapFunc(r.fleetsForModelRoute)).
		Named("modelfleet").
		Complete(r)
}

func (r *ModelFleetReconciler) fleetsForModelRoute(ctx context.Context, obj client.Object) []reconcile.Request {
	var fleets routingv1.ModelFleetList
	if err := r.List(ctx, &fleets); err != nil {
		return nil
	}
	var out []reconcile.Request
	for i := range fleets.Items {
		f := &fleets.Items[i]
		for _, m := range f.Spec.Members {
			ns, name := splitRef(m.Backend.ModelRouteRef, f.Namespace)
			if ns == obj.GetNamespace() && name == obj.GetName() {
				out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{
					Namespace: f.Namespace, Name: f.Name,
				}})
				break
			}
		}
	}
	return out
}
