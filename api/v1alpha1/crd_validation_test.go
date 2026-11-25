package v1alpha1

import (
	"os"
	"path/filepath"
	"testing"

	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextinstall "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// The generated CRDs must pass the same validation the API server runs on `kubectl apply`,
// which above all COMPILES every x-kubernetes-validations rule.
//
// Nothing else here can catch a bad CEL rule: the markers are plain strings to the Go
// compiler, controller-gen copies them through without reading them, and the fake client used
// by the controller tests ignores the schema entirely. So a rule that cannot compile looks
// perfectly healthy until `kubectl apply` refuses the whole CRD - and it refuses the CRD, not
// the rule, so the type stops being served and every ModelFleet in the cluster stops
// reconciling. That happened: a rule on gateway.extraPlugins, whose values carry
// x-kubernetes-preserve-unknown-fields, which puts the field outside CEL's type system
// entirely ("undefined field 'extraPlugins'"), found only on a test cluster.
func TestGeneratedCRDsAreAcceptedByTheAPIServer(t *testing.T) {
	scheme := runtime.NewScheme()
	apiextinstall.Install(scheme)

	paths, err := filepath.Glob("../../deploy/helm/autoconfig/crds/*.yaml")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no CRDs found to validate (glob err %v): run `make manifests`", err)
	}
	for _, p := range paths {
		name := filepath.Base(p)
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var v1crd apiextv1.CustomResourceDefinition
		if err := yaml.Unmarshal(b, &v1crd); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var crd apiext.CustomResourceDefinition
		if err := scheme.Convert(&v1crd, &crd, nil); err != nil {
			t.Fatalf("%s: convert: %v", name, err)
		}
		// A manifest has no status; the API server fills it in on create, and validation
		// insists on it, so stand in for what it would write.
		for _, v := range crd.Spec.Versions {
			if v.Storage {
				crd.Status.StoredVersions = append(crd.Status.StoredVersions, v.Name)
			}
		}
		if errs := apiextvalidation.ValidateCustomResourceDefinition(nil, &crd); len(errs) > 0 {
			t.Errorf("%s would be rejected by the API server:\n%v", name, errs.ToAggregate())
		}
	}
}
