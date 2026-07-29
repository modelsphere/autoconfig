// Package v1alpha1 定义 routing.4pd.io/v1alpha1 的 RouterBinding CRD。
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "routing.4pd.io", Version: "v1alpha1"}
	// SchemeBuilder registers the types into a runtime scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	// AddToScheme adds the types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&RouterBinding{}, &RouterBindingList{})
}
