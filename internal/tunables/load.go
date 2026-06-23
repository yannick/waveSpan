package tunables

import (
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// EnvPrefix is the prefix for per-tunable environment overrides, e.g.
// WAVESPAN_TUNABLE_STORAGE_ENGINE_WRITE_BUFFER_SIZE=128MiB.
const EnvPrefix = "WAVESPAN_TUNABLE_"

// EnvName returns the canonical environment-variable name for a tunable key. The dotted, camelCase
// key is converted to SCREAMING_SNAKE_CASE so it is unambiguous and shell-friendly:
//
//	storage.engine.writeBufferSize -> WAVESPAN_TUNABLE_STORAGE_ENGINE_WRITE_BUFFER_SIZE
func EnvName(key string) string {
	var b strings.Builder
	b.WriteString(EnvPrefix)
	for i, seg := range strings.Split(key, ".") {
		if i > 0 {
			b.WriteByte('_')
		}
		b.WriteString(camelToSnake(seg))
	}
	return strings.ToUpper(b.String())
}

// camelToSnake inserts underscores at word boundaries, treating runs of capitals (acronyms like
// "SSTables") and digit-led words ("h2Read") as single words: writeBufferSize -> write_Buffer_Size,
// maxOpenSSTables -> max_Open_SS_Tables, h2ReadIdleTimeout -> h2_Read_Idle_Timeout.
func camelToSnake(s string) string {
	rs := []rune(s)
	var b strings.Builder
	isUpper := func(r rune) bool { return r >= 'A' && r <= 'Z' }
	isLower := func(r rune) bool { return r >= 'a' && r <= 'z' }
	isDigit := func(r rune) bool { return r >= '0' && r <= '9' }
	for i, r := range rs {
		if i > 0 && isUpper(r) {
			prev := rs[i-1]
			nextLower := i+1 < len(rs) && isLower(rs[i+1])
			if isLower(prev) || isDigit(prev) || (isUpper(prev) && nextLower) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(r)
	}
	return b.String()
}

// Load builds the default registry, layers the YAML file's `tunables:` subtree (koanf, k8s ConfigMap)
// on top of the built-in defaults, then layers WAVESPAN_TUNABLE_* environment overrides on top of
// that. A nil env reads the process environment. Unknown keys and unparseable values are errors
// (fail fast, TS-002), so a typo in a ConfigMap or env var is caught at startup rather than ignored.
func Load(path string, env map[string]string) (*Registry, error) {
	r := Default()

	if path != "" {
		if err := loadFile(r, path); err != nil {
			return nil, err
		}
	}
	if err := loadEnv(r, env); err != nil {
		return nil, err
	}
	return r, nil
}

func loadFile(r *Registry, path string) error {
	k := koanf.New(".")
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return fmt.Errorf("tunables: load %s: %w", path, err)
	}
	sub := k.Cut("tunables") // the `tunables:` subtree; empty if absent
	for key, val := range sub.All() {
		if err := r.Set(key, fmt.Sprintf("%v", val), FromFile, 0); err != nil {
			return fmt.Errorf("tunables: in %s: %w", path, err)
		}
	}
	return nil
}

func loadEnv(r *Registry, env map[string]string) error {
	get := os.LookupEnv
	if env != nil {
		get = func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	}
	for _, p := range r.All() {
		if v, ok := get(EnvName(p.Key)); ok {
			if err := r.Set(p.Key, v, FromEnv, 0); err != nil {
				return fmt.Errorf("tunables: env %s: %w", EnvName(p.Key), err)
			}
		}
	}
	return nil
}
