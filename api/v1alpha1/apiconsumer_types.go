package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConsumerGateway: which gateway this consumer may call.
type ConsumerGateway struct {
	// Namespace holding the gateway's key-auth WasmPlugin. Defaults to higress-system, and
	// must be the namespace the gateway chart installs into — the same field, with the same
	// meaning, as ModelFleet's gateway.namespace.
	//
	// It is also the grouping key: every APIConsumer naming one namespace is rendered into
	// that one plugin's consumer list, in one write.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace,omitempty"`
}

// APIConsumerSpec names one caller of the gateway and where its credentials live.
type APIConsumerSpec struct {
	// ConsumerName is what a route's ModelFleet names in gateway.authConsumers, and what
	// appears in the gateway's access log as x-mse-consumer. Defaults to metadata.name.
	//
	// Separate from the object's name because consumer names are a gateway-wide namespace
	// shared with whatever the chart still renders, while object names are per Kubernetes
	// namespace: two teams could otherwise collide without either of them being able to see
	// it. The reconciler refuses a duplicate rather than letting one silently win.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	ConsumerName string `json:"consumerName,omitempty"`
	// Gateway: which gateway's consumer list this belongs in.
	// +optional
	Gateway ConsumerGateway `json:"gateway,omitempty"`
	// CredentialsSecretRef: a Secret in this object's namespace holding this consumer's API
	// keys — EVERY value in it becomes an accepted credential, so a rotation is "add the new
	// key, let both work, remove the old one" with no window where neither does.
	//
	// Only the fleet's own namespace, and only a Secret: the controller holds a cluster-wide
	// `secrets: get`, so a ref that could name any namespace would turn "can write an
	// APIConsumer here" into "can read every Secret in the cluster".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	CredentialsSecretRef string `json:"credentialsSecretRef"`
}

// APIConsumerStatus is what the console reads to answer "is this key live yet".
type APIConsumerStatus struct {
	// ObservedGeneration: equal to metadata.generation once this consumer is programmed into
	// the gateway. Only advanced on a successful write, so it never claims a key is live
	// while the plugin still carries the old list.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// ConsumerName actually programmed, with the default expanded.
	// +optional
	ConsumerName string `json:"consumerName,omitempty"`
	// Credentials: how many keys this consumer currently has. The keys themselves are never
	// copied here — status is world-readable to anyone who can read the CR, and the point of
	// the Secret is that the key lives in one place. Two during a rotation, one otherwise.
	// +optional
	Credentials int32 `json:"credentials,omitempty"`
	// AppliedGatewayNamespace: the gateway this consumer was last programmed into. Moving
	// spec.gateway.namespace otherwise leaves the old gateway accepting these credentials
	// forever, with nothing in the object pointing at it.
	// +optional
	AppliedGatewayNamespace string `json:"appliedGatewayNamespace,omitempty"`
	// LastSyncTime: when this object was last reconciled, including rounds that wrote
	// nothing. "Is the controller still looking at this", not "when did the gateway change".
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`
	// Conditions: Programmed (the gateway carries this consumer's credentials).
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// APIConsumer is one caller of the gateway: a name and a set of API keys.
//
// It exists so that adding, rotating or removing a key is an object write rather than a helm
// release. Today consumers and their keys are rendered into key-auth's defaultConfig by the
// gateway chart, which means every new tenant is a change to a values file and a deploy.
//
// Why a separate kind rather than a field on ModelFleet: key-auth's `allow` (who may call
// THIS route) is per route and belongs to the fleet, but `consumers` (who exists at all, and
// with which keys) is one list for the whole gateway. Putting it on ModelFleet would have N
// fleets writing one field — the matchRules problem again, and worse, because defaultConfig
// is an opaque blob with no per-entry ownership under Server-Side Apply.
//
// ⚠ What this does NOT give you: secrecy. key-auth's config is a blob pushed to the data
// plane over ECDS, and the plugin cannot read a Secret itself, so the keys still end up in
// plain text inside the WasmPlugin object. Anyone who can read WasmPlugins in the gateway
// namespace can read every key. What it gives you is one source of truth, online rotation,
// and no release to add a tenant.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=apicon
// +kubebuilder:printcolumn:name="Consumer",type=string,JSONPath=`.status.consumerName`
// +kubebuilder:printcolumn:name="Keys",type=integer,JSONPath=`.status.credentials`
// +kubebuilder:printcolumn:name="Gateway",type=string,JSONPath=`.status.appliedGatewayNamespace`
// +kubebuilder:printcolumn:name="Programmed",type=string,JSONPath=`.status.conditions[?(@.type=="Programmed")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type APIConsumer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   APIConsumerSpec   `json:"spec,omitempty"`
	Status APIConsumerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type APIConsumerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []APIConsumer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&APIConsumer{}, &APIConsumerList{})
}
