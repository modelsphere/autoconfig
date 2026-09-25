// Package v1alpha1 定义 routing.modelsphere.dev/v1alpha1 的 ModelRoute CRD。
// +kubebuilder:object:generate=true
// +groupName=routing.modelsphere.dev
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "routing.modelsphere.dev", Version: "v1alpha1"}
	// SchemeBuilder registers the types into a runtime scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	// AddToScheme adds the types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&ModelRoute{}, &ModelRouteList{})
}
