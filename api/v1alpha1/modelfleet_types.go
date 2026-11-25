package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Hardware labels a member with the accelerator it runs on. It is descriptive only: nothing
// in the routing path reads it. The console shows it next to the member's pods, and the
// reconciler copies it into status so the console needs one read, not N.
type Hardware struct {
	// Vendor, e.g. nvidia / huawei.
	// +kubebuilder:validation:MinLength=1
	Vendor string `json:"vendor"`
	// Model, e.g. H100 / 910B4 / RTX6000D.
	// +kubebuilder:validation:MinLength=1
	Model string `json:"model"`
	// Replicas: how many engine instances this member is meant to have. Declarative intent
	// for display; the live count is status.members[].readyReplicas, discovered from the
	// referenced ModelRoute. They differ while a member is rolling or broken, and that
	// difference is exactly what the console needs to show.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas,omitempty"`
}

// MemberBackend points at the service that already serves this member. It is a reference on
// purpose: the path prefix, the port and the concurrency ceiling all have an authoritative
// value in the ModelRoute already, and copying them here would make two sources of truth.
type MemberBackend struct {
	// ModelRouteRef: a ModelRoute in this fleet's own namespace, the one swiss created for
	// this hardware type. The gateway path prefix is derived from its spec.nginx.route
	// (falling back to its metadata.name, same rule the ModelRoute itself uses).
	//
	// Same namespace only, and not merely as a convention: the fleet's Ingress sends every
	// member's path to the openresty in the fleet's namespace (an Ingress backend cannot name
	// a Service anywhere else), and that openresty serves the routes of its own namespace. A
	// member whose ModelRoute lives elsewhere would get a path that this openresty does not
	// serve - a 404 on a route the fleet reports as healthy. An "ns/" prefix naming another
	// namespace is refused rather than accepted and then silently dead.
	// +kubebuilder:validation:MinLength=1
	ModelRouteRef string `json:"modelRouteRef"`
}

// Capabilities are the member's declared limits. They are NOT enforced at the gateway today:
// admission by concurrency lives in openresty, and the router plugin has no landing place for
// a per-member context-length check. They are carried so the console can show them and so the
// policy writer can reason about them; see the status note on MaxConcurrency.
type Capabilities struct {
	// MaxContextTokens: the longest request this member can serve. Reserved: routing does not
	// consult it yet (the plugin cannot admit by context length), so a request longer than this
	// can still be routed here and will fail at the engine. Tracked as a plugin feature request.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxContextTokens int32 `json:"maxContextTokens,omitempty"`
	// MaxConcurrency: in-flight requests this member can take. Reserved for the same reason —
	// concurrency admission is openresty's job (per-route max_concurrency), not the gateway's.
	// The reconciler only sums it into status.totalCapacity for display.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxConcurrency int32 `json:"maxConcurrency,omitempty"`
}

// FleetMember is one hardware type's service inside the fleet. Written by the console.
type FleetMember struct {
	// Name identifies the member everywhere: it is the backend id in the router's decision
	// table and the label on the gateway's per-backend metrics, so it must stay stable —
	// renaming one is deleting it and adding another.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// Hardware: which accelerator this member runs on (display only).
	Hardware Hardware `json:"hardware"`
	// Backend: the ModelRoute that already serves this member.
	Backend MemberBackend `json:"backend"`
	// Capabilities: declared limits, reserved (see the type's note).
	// +optional
	Capabilities *Capabilities `json:"capabilities,omitempty"`
}

// StickinessKey says what identifies a session.
type StickinessKey struct {
	// Header carrying the session id, e.g. x-session-id.
	// +kubebuilder:validation:MinLength=1
	Header string `json:"header"`
}

// Stickiness pins one session to one member.
//
// NOT IMPLEMENTED at the gateway: ai-backend-router picks with WeightedRandom and has no hash
// picker, so setting this changes nothing today. It is refused at apply time rather than
// silently ignored (see the CEL rule on FleetSpec) — a knob that looks applied but is not is
// worse than one that is rejected. The field stays so the shape is settled for when the
// plugin grows a weighted-HRW picker.
type Stickiness struct {
	// Key: what identifies the session.
	Key StickinessKey `json:"key"`
}

// PolicyMember is the traffic share of one member. Written by decision-gen.
type PolicyMember struct {
	// Name of the member in spec.members. An entry naming a member that no longer exists is
	// ignored with a warning on the PolicyApplied condition, NOT rejected: rejecting it would
	// deadlock the console's delete against decision-gen's stale policy.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Tier: lower is preferred. Members in the same tier split traffic by weight; a higher
	// tier is only reached when every lower tier overflows (429/5xx). Tier and weight answer
	// different questions and cannot be collapsed into one number: weight is "how to split
	// among peers", tier is "where to go once the mains are full".
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=9
	Tier int32 `json:"tier,omitempty"`
	// Weight within the tier, relative. 0 takes the member out of rotation.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10000
	Weight int32 `json:"weight"`
}

// TrafficPolicy is decision-gen's half of the object: how traffic is meant to be split.
// Manual intervention ("drain ascend now") is a patch to this same field, not a side channel
// to the gateway.
type TrafficPolicy struct {
	// Revision: an audit tag, copied into the decision table's version string so a table read
	// off the gateway can be traced back to the decision that produced it.
	//
	// It is NOT the convergence signal — metadata.generation / status.observedGeneration are,
	// and those are maintained by the API server rather than by whoever writes here.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Revision int64 `json:"revision,omitempty"`
	// Reason: why this split, in one line, for the audit trail and the console.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Reason string `json:"reason,omitempty"`
	// Stickiness: reserved, refused at apply time today (see the type's note).
	// +optional
	Stickiness *Stickiness `json:"stickiness,omitempty"`
	// Members: the per-member split. A member with no entry here is served with weight 0 in
	// tier 0, i.e. reachable only as the default member — writing no policy at all must not
	// silently spray traffic across hardware nobody has measured.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=32
	Members []PolicyMember `json:"members,omitempty"`
}

// GatewaySpec selects the gateway that fronts this fleet.
type GatewaySpec struct {
	// Class: the only renderer today is higress. The split itself is done by the
	// ai-backend-router plugin, so there is no "mechanism" knob here.
	// +optional
	// +kubebuilder:validation:Enum=higress
	// +kubebuilder:default=higress
	Class string `json:"class,omitempty"`
	// Namespace holding the gateway's WasmPlugin objects. Defaults to higress-system, and
	// must be the namespace the gateway chart installs into (its `namespaces.higress`). The
	// two are configured separately, so a mismatch shows up as the plugins being "missing"
	// rather than as anything pointing at the namespace — the controller names this namespace
	// in that error for exactly that reason.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace,omitempty"`
	// Service: the openresty Service every member's path lives behind ("ns/name:port",
	// port defaults to 8080). The Ingress points here; the router plugin rewrites the path
	// per member. Defaults to "<fleet ns>/openresty:8080".
	// +optional
	Service string `json:"service,omitempty"`
	// ExtraPlugins: config for gateway plugins this controller has no opinion about, keyed by
	// the WasmPlugin object's name and passed through to it verbatim as this route's match
	// rule. It exists so that adding a plugin to the gateway — rate limiting, context
	// admission, whatever comes next — is a chart change and a few lines of YAML here, not a
	// new field, a new renderer and a new release of this controller.
	//
	// A plugin whose config this controller DOES have an opinion about is not accepted here,
	// because the two writers would fight over one rule: the backend router's table is
	// computed from live ModelRoute state, key-auth's allow-list is cross-checked against the
	// gateway's consumers, and ai-proxy's credential is resolved from a Secret so it never
	// lands in this object. That is refused at reconcile time (GatewaySynced=False, nothing
	// written) rather than by a CEL rule on this field, because a CEL rule on it cannot exist:
	// the API server drops a map whose values carry x-kubernetes-preserve-unknown-fields from
	// the CEL type system, so `self.gateway.extraPlugins` is an undefined field and the whole
	// CRD is rejected at install time. Verbatim passthrough and API-server-side key validation
	// cannot both be had on one field.
	//
	// ⚠ The content is NOT validated: plugin config is checked at the gateway, not by the API
	// server, so a wrong field here applies cleanly and then fails when the gateway loads the
	// plugin — and one plugin that fails to load stalls the config push for every plugin on
	// that gateway. Treat this as writing gateway config directly, because that is what it is.
	//
	// Being a map, each key is its own field under Server-Side Apply: a quota service can own
	// the rate-limit entry while the console owns the rest, with no extra object.
	// +optional
	// +kubebuilder:validation:MaxProperties=16
	ExtraPlugins map[string]apiextensionsv1.JSON `json:"extraPlugins,omitempty"`
	// ExternalURL: how clients reach this gateway from outside, e.g.
	// "http://172.28.44.17:31080". The controller publishes it as status.endpoint with "/v1"
	// appended — the one address the console shows. It is configuration rather than something
	// to discover: the gateway Service is a NodePort behind whatever the site puts in front
	// of it, and guessing an address the console would then show to users is worse than
	// requiring it to be stated.
	// +optional
	// +kubebuilder:validation:Pattern=`^https?://[^/]+$`
	ExternalURL string `json:"externalURL,omitempty"`
	// UpstreamAuthSecretRef: the Secret holding the API key the gateway presents to openresty,
	// as "ns/name:key" ("name:key" uses the fleet's namespace). The key is read by the
	// controller and rendered into the ai-proxy plugin config; it never appears in this CR.
	//
	// Omitted = "openresty-api-keys:keys", where that key already lives on these clusters.
	// The gateway needs the same credential as every other caller of openresty, so this is
	// one cluster-wide fact rather than something each fleet should restate. Set it only to
	// point at a different Secret, or to "none" for an openresty that needs no key — a
	// missing default Secret fails the sync rather than quietly publishing a route that will
	// 401 on its first real request.
	// +optional
	UpstreamAuthSecretRef string `json:"upstreamAuthSecretRef,omitempty"`
	// AuthConsumers: the key-auth consumers allowed on this fleet's route. The consumer
	// definitions (and their keys) stay in the gateway chart's defaultConfig — only the
	// allow-list for this route is ours to write, so no key ever lands in this CR.
	//
	// Required, because the gateway runs with global_auth=false: a route with no key-auth
	// match rule is not denied, it is OPEN. An empty list would therefore publish a public
	// route while reporting a healthy sync, which is the one outcome this controller orders
	// its writes to avoid. ["*"] is how a public route is asked for on purpose — it writes no
	// rule, exactly as an empty list used to, but it says so in the object.
	//
	// Names are checked against what key-auth actually defines before the route is published:
	// a name with a typo is accepted by both the API server and the plugin, and then denies
	// every caller with 403 while this CR reports a healthy sync.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:XValidation:rule="!self.exists(c, c == '*') || size(self) == 1",message="authConsumers: \"*\" means \"no authentication on this route\" and cannot be combined with consumer names"
	AuthConsumers []string `json:"authConsumers"`
}

// ModelFleetSpec aggregates several single-hardware services into one externally visible model.
//
// Three writers share this object and each owns a disjoint subtree (console: everything but
// trafficPolicy; decision-gen: trafficPolicy; this controller: status). That only holds under
// Server-Side Apply with a per-writer fieldManager — a client-side apply or a constructed-object
// Update will silently wipe the other writer's fields.
//
// defaultMember must name a real member: it is where the plugin lands when every member is
// unusable, so a dangling name is a hole in the one path that is supposed to have no holes.
// +kubebuilder:validation:XValidation:rule="self.members.exists(m, m.name == self.defaultMember)",message="defaultMember must name one of spec.members"
// Stickiness has no landing place in ai-backend-router yet. Refuse it instead of accepting a
// setting that will never take effect.
// +kubebuilder:validation:XValidation:rule="!has(self.trafficPolicy) || !has(self.trafficPolicy.stickiness)",message="trafficPolicy.stickiness is not supported by ai-backend-router yet; it would be silently ignored, so it is refused here"
type ModelFleetSpec struct {
	// ModelName is what the client sends as body.model, and what model-router copies into the
	// x-higress-llm-model header the Ingress matches on. Defaults to metadata.name.
	// +optional
	// +kubebuilder:validation:MaxLength=200
	ModelName string `json:"modelName,omitempty"`
	// Gateway: which gateway fronts this fleet.
	// +optional
	Gateway GatewaySpec `json:"gateway,omitempty"`
	// DefaultMember: where the plugin sends a request when no member is selectable (all
	// disabled, all weights zero, or the plugin errors internally). That round does not
	// overflow afterwards.
	//
	// It makes "set every weight to 0" mean "send everything here", NOT "stop serving" —
	// draining the fleet is removing members or taking the route down, never zeroing weights.
	// +kubebuilder:validation:MinLength=1
	DefaultMember string `json:"defaultMember"`
	// Members: one entry per hardware type, each pointing at the service swiss already created.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	Members []FleetMember `json:"members"`
	// TrafficPolicy: decision-gen's field. Absent = every member at weight 0, i.e. only the
	// default member serves.
	// +optional
	TrafficPolicy *TrafficPolicy `json:"trafficPolicy,omitempty"`
}

// TotalCapacity is the fleet's capacity, summed from what each READY member actually reports —
// not from the declared replica counts, and not from accelerator counts (cards are not
// interchangeable across vendors, so adding them up answers no question anyone has).
type TotalCapacity struct {
	// MaxConcurrency: sum of ready members' capabilities.maxConcurrency.
	MaxConcurrency int32 `json:"maxConcurrency"`
	// ReadyReplicas: sum of ready members' discovered backend counts.
	ReadyReplicas int32 `json:"readyReplicas"`
}

// MemberPhase is a member's routing state.
// +kubebuilder:validation:Enum=Ready;Degraded;NotFound
type MemberPhase string

const (
	// MemberReady: the referenced ModelRoute exists and has ready backends.
	MemberReady MemberPhase = "Ready"
	// MemberDegraded: the ModelRoute exists but has no ready backend right now.
	MemberDegraded MemberPhase = "Degraded"
	// MemberNotFound: modelRouteRef points at nothing.
	MemberNotFound MemberPhase = "NotFound"
)

// MemberStatus is one member as observed, and is the only place the console should read from.
type MemberStatus struct {
	// Name of the member.
	Name string `json:"name"`
	// Phase: Ready / Degraded / NotFound.
	Phase MemberPhase `json:"phase"`
	// Hardware, copied from spec so the console renders the pod table in one read.
	// +optional
	Hardware *Hardware `json:"hardware,omitempty"`
	// Path: the gateway path prefix this member resolves to, derived from the ModelRoute.
	// +optional
	Path string `json:"path,omitempty"`
	// ReadyReplicas: ready backends behind the referenced ModelRoute.
	ReadyReplicas int32 `json:"readyReplicas"`
	// EffectiveWeight: the weight actually shipped to the gateway, which is NOT the weight in
	// trafficPolicy once a member goes unready — the remaining members in that tier are
	// renormalised so the dead member's share is redistributed instead of being dropped on the
	// floor. decision-gen must read this, not its own last write, or it computes the next
	// round from traffic that never happened.
	EffectiveWeight int32 `json:"effectiveWeight"`
	// Tier the member was placed in.
	Tier int32 `json:"tier"`
}

// ModelFleetStatus is written only by this controller, through the status subresource.
type ModelFleetStatus struct {
	// ObservedGeneration: the spec generation this status reflects. Equal to
	// metadata.generation means the gateway has been told; the gateway's own config push adds
	// seconds on top (measured at 15s scale), which GatewaySynced reports.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Endpoint: the single address the console shows.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// ModelName actually programmed into the gateway.
	// +optional
	ModelName string `json:"modelName,omitempty"`
	// TotalCapacity: summed over ready members.
	// +optional
	TotalCapacity *TotalCapacity `json:"totalCapacity,omitempty"`
	// Members: per-member observed state.
	// +optional
	// +listType=map
	// +listMapKey=name
	Members []MemberStatus `json:"members,omitempty"`
	// AppliedExtraPlugins: the gateway.extraPlugins keys this controller has actually written
	// a rule for. Removing a key from the spec is otherwise undetectable - the reconciler
	// only ever sees what is there now - and the rule it wrote would keep enforcing itself on
	// the live route with nothing in the CR to say so. Same reason ModelRoute keeps
	// appliedRouteKey across a rename.
	// +optional
	// +listType=set
	AppliedExtraPlugins []string `json:"appliedExtraPlugins,omitempty"`
	// AppliedGatewayNamespace: the gateway namespace this fleet's rules were last written to,
	// for the same reason appliedExtraPlugins exists. Rules are written into WasmPlugins in
	// spec.gateway.namespace, and the reconciler only ever sees the namespace that is there
	// now: change the field and the rules in the old namespace would stay bound to this
	// fleet's Ingress reference forever, with nothing in the CR pointing at them.
	// +optional
	AppliedGatewayNamespace string `json:"appliedGatewayNamespace,omitempty"`
	// LastSyncTime: when this fleet was last reconciled — set on every pass, including the
	// ones that failed before writing anything. It answers "is the controller still looking
	// at this object", not "when did the gateway last change"; observedGeneration and
	// GatewaySynced answer the latter.
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`
	// Conditions: Ready (serving), GatewaySynced (objects written), PolicyApplied (the policy
	// was usable — orphan member references show up here as a warning, not as a rejection).
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mf
// +kubebuilder:printcolumn:name="Model",type=string,JSONPath=`.status.modelName`
// Members is the number of members, so it counts status.members - readyReplicas is a count
// of engine replicas across the fleet, which is a different (and usually larger) number.
// +kubebuilder:printcolumn:name="Members",type=integer,JSONPath=`.status.members.length()`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.totalCapacity.readyReplicas`
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=`.status.endpoint`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ModelFleet presents several single-hardware services as one model behind one address, and
// splits traffic across them by weight and tier at the gateway.
type ModelFleet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelFleetSpec   `json:"spec,omitempty"`
	Status ModelFleetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ModelFleetList is a list of ModelFleet.
type ModelFleetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ModelFleet `json:"items"`
}
