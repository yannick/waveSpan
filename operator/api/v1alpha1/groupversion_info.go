// Package v1alpha1 contains the WaveSpan operator CRD API types (group db.wavespan.io, version
// v1alpha1; design/12). The operator depends only on these generated API types and the node image
// — never on data-node internals (design/17 forbidden dependency).
// +kubebuilder:object:generate=true
// +groupName=db.wavespan.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion is the API group/version for WaveSpan CRDs.
var GroupVersion = schema.GroupVersion{Group: "db.wavespan.io", Version: "v1alpha1"}

// SchemeBuilder registers the API types with a scheme.
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

// AddToScheme adds the API types to a scheme.
var AddToScheme = SchemeBuilder.AddToScheme
