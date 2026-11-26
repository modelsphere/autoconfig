package v1alpha1

import (
	"context"
	"os"
	"strings"
	"testing"

	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextinstall "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	celschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/yaml"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
)

// The CRD's validation rules are the API's contract with the console and with decision-gen:
// they are what makes a bad ModelFleet fail at `kubectl apply` instead of at the gateway. The
// other test here only checks that they COMPILE. These evaluate them, against the generated
// CRD, using the API server's own validator — so a rule that compiles and then accepts
// everything, or rejects everything, is caught here rather than on a cluster.
//
// Validation that the API server does structurally (MinItems, MaxLength, Minimum/Maximum) is
// exercised the same way, because those markers are just as easy to drop by accident and just
// as invisible in Go.

// fleetValidator builds the API server's CEL validator plus the structural schema, from the
// CRD this repository ships.
func fleetValidator(t *testing.T) (*celschema.Validator, *structuralschema.Structural) {
	t.Helper()
	scheme := runtime.NewScheme()
	apiextinstall.Install(scheme)

	b, err := os.ReadFile("../../deploy/helm/autoconfig/crds/routing.modelsphere.dev_modelfleets.yaml")
	if err != nil {
		t.Fatalf("read CRD: %v (run `make manifests`)", err)
	}
	var v1crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(b, &v1crd); err != nil {
		t.Fatal(err)
	}
	var crd apiext.CustomResourceDefinition
	if err := scheme.Convert(&v1crd, &crd, nil); err != nil {
		t.Fatal(err)
	}
	// Conversion to the internal type hoists a schema shared by every version to
	// spec.validation, leaving the per-version Schema nil - which is why the API server
	// reported the bad CEL rule under "spec.validation.openAPIV3Schema" rather than under the
	// version. Look in both places.
	var props *apiext.JSONSchemaProps
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Name == GroupVersion.Version && crd.Spec.Versions[i].Schema != nil {
			props = crd.Spec.Versions[i].Schema.OpenAPIV3Schema
		}
	}
	if props == nil && crd.Spec.Validation != nil {
		props = crd.Spec.Validation.OpenAPIV3Schema
	}
	if props == nil {
		t.Fatalf("no schema for version %s", GroupVersion.Version)
	}
	ss, err := structuralschema.NewStructural(props)
	if err != nil {
		t.Fatalf("structural schema: %v", err)
	}
	return celschema.NewValidator(ss, true, celconfig.PerCallLimit), ss
}

// validate runs both the CEL rules and reports their errors, as the API server would on a
// create. Returns the joined error messages, empty when the object is accepted.
func validate(t *testing.T, val *celschema.Validator, ss *structuralschema.Structural, obj map[string]any) string {
	t.Helper()
	errs, _ := val.Validate(context.Background(), field.NewPath(""), ss, obj, nil, celconfig.RuntimeCELCostBudget)
	msgs := make([]string, 0, len(errs))
	for _, e := range errs {
		msgs = append(msgs, e.Error())
	}
	return strings.Join(msgs, "; ")
}

// fleetObj is the smallest ModelFleet the schema accepts, as an unstructured map. Each test
// mutates one thing, so a rule that fires on the wrong input shows up as the baseline failing.
func fleetObj() map[string]any {
	return map[string]any{
		"apiVersion": GroupVersion.String(),
		"kind":       "ModelFleet",
		"metadata":   map[string]any{"name": "glm-5.3", "namespace": "llm-route"},
		"spec": map[string]any{
			"modelName":     "glm-5.3",
			"defaultMember": "h100",
			"gateway": map[string]any{
				"namespace":     "higress-system",
				"authConsumers": []any{"ops"},
			},
			"members": []any{
				map[string]any{
					"name":     "h100",
					"hardware": map[string]any{"vendor": "nvidia", "model": "H100", "replicas": int64(2)},
					"backend":  map[string]any{"modelRouteRef": "glm-53-h100"},
				},
			},
		},
	}
}

func TestCELBaselineIsAccepted(t *testing.T) {
	val, ss := fleetValidator(t)
	if msg := validate(t, val, ss, fleetObj()); msg != "" {
		t.Fatalf("the baseline object was rejected, so every other case here is meaningless: %s", msg)
	}
}

// defaultMember is what the plugin falls back to when nothing is selectable. A name that is
// not a member leaves the gateway resolving `default` against a table that does not contain
// it, which is a 500 rather than a degraded fleet - and the plugin validates at the gateway,
// not here, so this has to be caught at apply time.
func TestCELDefaultMemberMustNameAMember(t *testing.T) {
	val, ss := fleetValidator(t)
	o := fleetObj()
	o["spec"].(map[string]any)["defaultMember"] = "not-a-member"
	msg := validate(t, val, ss, o)
	if msg == "" {
		t.Fatal("a defaultMember naming nothing was accepted")
	}
	if !strings.Contains(msg, "defaultMember") {
		t.Fatalf("the message does not say which field is wrong: %s", msg)
	}
}

// Stickiness has no landing place in ai-backend-router: the plugin has WeightedRandom and no
// hash picker. Accepting the field would mean configuring a switch that never takes effect.
// defaultMember is optional now, so the rule has to skip an absent one WITHOUT also letting
// a wrong one through - the two are one `||` apart, and getting it backwards turns the
// strictest check in this CRD into a no-op.
func TestCELDefaultMemberMayBeOmitted(t *testing.T) {
	val, ss := fleetValidator(t)
	o := fleetObj()
	delete(o["spec"].(map[string]any), "defaultMember")
	if msg := validate(t, val, ss, o); msg != "" {
		t.Fatalf("an omitted defaultMember was rejected: %s", msg)
	}
	// ...and a stated-but-wrong one is still refused.
	o["spec"].(map[string]any)["defaultMember"] = "not-a-member"
	if msg := validate(t, val, ss, o); msg == "" {
		t.Fatal("making the field optional also stopped it checking the stated value")
	}
}

func TestCELStickinessIsRefused(t *testing.T) {
	val, ss := fleetValidator(t)
	o := fleetObj()
	o["spec"].(map[string]any)["trafficPolicy"] = map[string]any{
		"members":    []any{map[string]any{"name": "h100", "weight": int64(10000)}},
		"stickiness": map[string]any{"header": "x-session-id"},
	}
	if msg := validate(t, val, ss, o); msg == "" {
		t.Fatal("trafficPolicy.stickiness was accepted")
	}
	// ...and the same policy without it is fine, or the rule is simply rejecting policies.
	o["spec"].(map[string]any)["trafficPolicy"] = map[string]any{
		"members": []any{map[string]any{"name": "h100", "weight": int64(10000)}},
	}
	if msg := validate(t, val, ss, o); msg != "" {
		t.Fatalf("a plain traffic policy was rejected: %s", msg)
	}
}

// ["*"] means "no authentication on this route". Mixed with consumer names it reads as if the
// named ones were special, while in fact the route would be wide open - two different
// intentions in one list.
func TestCELPublicSentinelCannotBeMixedWithNames(t *testing.T) {
	val, ss := fleetValidator(t)
	o := fleetObj()
	gw := o["spec"].(map[string]any)["gateway"].(map[string]any)

	gw["authConsumers"] = []any{"*", "ops"}
	if msg := validate(t, val, ss, o); msg == "" {
		t.Fatal(`authConsumers: ["*", "ops"] was accepted`)
	}
	// Each on its own is legitimate: the sentinel alone, and names alone.
	for _, allow := range []any{[]any{"*"}, []any{"ops", "tenant-a"}} {
		gw["authConsumers"] = allow
		if msg := validate(t, val, ss, o); msg != "" {
			t.Fatalf("authConsumers %v was rejected: %s", allow, msg)
		}
	}
}

// A plugin this controller renders itself cannot be passed through - but that is enforced by
// the reconciler, not by a rule here, because the API server leaves a map with
// x-kubernetes-preserve-unknown-fields values out of the CEL type system and a rule naming it
// makes the whole CRD fail to install. This pins the fact that extraPlugins is NOT validated
// at apply time, so nobody adds such a rule back without hitting this test first.
func TestCELExtraPluginsIsNotValidatedHere(t *testing.T) {
	val, ss := fleetValidator(t)
	o := fleetObj()
	o["spec"].(map[string]any)["gateway"].(map[string]any)["extraPlugins"] = map[string]any{
		"ai-proxy": map[string]any{"provider": map[string]any{"apiTokens": []any{"sk-anything"}}},
	}
	if msg := validate(t, val, ss, o); msg != "" {
		t.Fatalf("extraPlugins is being validated by a CEL rule after all (%s) - that rule "+
			"cannot exist: see the field's doc and TestGeneratedCRDsAreAcceptedByTheAPIServer", msg)
	}
}
