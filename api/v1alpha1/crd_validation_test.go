package v1alpha1

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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

// Every phase this package can WRITE has to be in the CRD's enum for that field. It is not a
// style check: a value missing from the enum makes the API server reject the whole status
// write, so the object keeps its previous status forever and the only sign is an error line
// in the controller log. MemberDisabled shipped in exactly that state and got as far as a
// test cluster, where "the route was withdrawn but status still says Ready" was the symptom -
// nothing in the unit tests could see it, because they never go through CRD validation.
func TestStatusEnumsCoverEveryConstant(t *testing.T) {
	for _, tc := range []struct {
		crd    string
		path   []string
		values []string
	}{
		{
			crd: "routing.modelsphere.dev_modelfleets.yaml",
			path: []string{"spec", "versions", "0", "schema", "openAPIV3Schema", "properties",
				"status", "properties", "members", "items", "properties", "phase"},
			values: []string{
				string(MemberReady), string(MemberDegraded),
				string(MemberNotFound), string(MemberDisabled),
			},
		},
	} {
		t.Run(tc.crd, func(t *testing.T) {
			enum := enumAt(t, tc.crd, tc.path)
			for _, want := range tc.values {
				if !slices.Contains(enum, want) {
					t.Errorf("the controller can write phase %q, but the CRD only allows %v - "+
						"every status write carrying it will be rejected and the object will "+
						"keep its old status", want, enum)
				}
			}
		})
	}
}

// enumAt reads the enum of one property out of a CRD on disk, walking the path given (list
// indices as decimal strings).
func enumAt(t *testing.T, file string, path []string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "helm", "autoconfig", "crds", file))
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	cur := doc
	for _, step := range path {
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[step]
			if !ok {
				t.Fatalf("%s: no %q under %v", file, step, path)
			}
			cur = next
		case []any:
			i, err := strconv.Atoi(step)
			if err != nil || i >= len(node) {
				t.Fatalf("%s: bad list index %q", file, step)
			}
			cur = node[i]
		default:
			t.Fatalf("%s: cannot walk %q into %T", file, step, cur)
		}
	}
	prop, ok := cur.(map[string]any)
	if !ok {
		t.Fatalf("%s: %v is not a property object", file, path)
	}
	vals, ok := prop["enum"].([]any)
	if !ok {
		t.Fatalf("%s: %v has no enum - the check would pass for the wrong reason", file, path)
	}
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		out = append(out, fmt.Sprint(v))
	}
	return out
}
