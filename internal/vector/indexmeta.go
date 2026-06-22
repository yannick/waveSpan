package vector

import "fmt"

// IndexSpec mirrors the VectorIndex CRD spec (design/12). The approximate block is captured but
// inert in M9 (ANN lands in M10).
type IndexSpec struct {
	Name                 string
	Collection           string
	Label                string
	Property             string
	Dimensions           int
	Dtype                string
	Metric               string
	ExactEnabled         bool
	Approximate          map[string]any
	Visibility           string
	ReplicationPolicyRef string
}

// IndexMeta is the resolved, in-process index metadata used by the exact-search procedure.
type IndexMeta struct {
	Name         string
	Collection   string
	Label        string
	Property     string
	Metric       Metric
	Dimensions   int
	ExactEnabled bool
	Approximate  map[string]any // inert in M9
}

// ParseVectorIndexSpec resolves a CRD spec to IndexMeta. dimensions must be > 0 (CRD validation,
// design/12).
func ParseVectorIndexSpec(spec IndexSpec) (*IndexMeta, error) {
	if spec.Dimensions <= 0 {
		return nil, fmt.Errorf("vector: index %q has invalid dimensions %d (must be > 0)", spec.Name, spec.Dimensions)
	}
	collection := spec.Collection
	if collection == "" {
		collection = spec.Name // default the collection to the index name
	}
	return &IndexMeta{
		Name: spec.Name, Collection: collection, Label: spec.Label, Property: spec.Property,
		Metric: ParseMetric(spec.Metric), Dimensions: spec.Dimensions, ExactEnabled: spec.ExactEnabled,
		Approximate: spec.Approximate,
	}, nil
}
