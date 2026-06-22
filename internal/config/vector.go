package config

import (
	"strconv"
	"strings"
)

// VectorIndexConfig declares a vector index available for vector.searchExact (design/08, design/12).
type VectorIndexConfig struct {
	Name         string `yaml:"name"`
	Collection   string `yaml:"collection"`
	Metric       string `yaml:"metric"`
	Dimensions   int    `yaml:"dimensions"`
	Label        string `yaml:"label"`
	Property     string `yaml:"property"`
	ExactEnabled bool   `yaml:"exactEnabled"`
}

// applyVectorEnv parses WAVESPAN_VECTOR_INDEXES=name:collection:metric:dims,... overrides.
func (c *Config) applyVectorEnv(get func(string) (string, bool)) {
	v, ok := get("WAVESPAN_VECTOR_INDEXES")
	if !ok {
		return
	}
	var out []VectorIndexConfig
	for _, spec := range strings.Split(v, ",") {
		parts := strings.Split(strings.TrimSpace(spec), ":")
		if len(parts) < 4 || parts[0] == "" {
			continue
		}
		dims, _ := strconv.Atoi(parts[3])
		out = append(out, VectorIndexConfig{
			Name: parts[0], Collection: parts[1], Metric: parts[2], Dimensions: dims, ExactEnabled: true,
		})
	}
	if len(out) > 0 {
		c.VectorIndexes = out
	}
}
