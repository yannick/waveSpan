package config

import "testing"

func TestReplicationFactorScopes(t *testing.T) {
	cases := []struct {
		factor     string
		everywhere bool
		scope      GlobalScope
	}{
		{"", false, GlobalScopeDefault},
		{"5", false, GlobalScopeDefault},
		{"all", true, GlobalScopeLocalOnly},
		{"All", true, GlobalScopeLocalOnly},
		{"global", true, GlobalScopeGlobal},
		{"GLOBAL", true, GlobalScopeGlobal},
	}
	for _, c := range cases {
		n := NamespaceConfig{Name: "ns", ReplicationFactor: c.factor}
		if n.ReplicateEverywhere() != c.everywhere {
			t.Errorf("factor %q: ReplicateEverywhere=%v want %v", c.factor, n.ReplicateEverywhere(), c.everywhere)
		}
		if n.Scope() != c.scope {
			t.Errorf("factor %q: Scope=%v want %v", c.factor, n.Scope(), c.scope)
		}
	}
}

func TestEverywhereAndLocalOnlySets(t *testing.T) {
	c := &Config{Namespaces: []NamespaceConfig{
		{Name: "ref", ReplicationFactor: "all"},
		{Name: "cfg", ReplicationFactor: "global"},
		{Name: "data"},
	}}
	ev := c.EverywhereNamespaces()
	if !ev["ref"] || !ev["cfg"] || ev["data"] {
		t.Fatalf("everywhere set = %v (want ref+cfg)", ev)
	}
	lo := c.LocalOnlyNamespaces()
	if !lo["ref"] || lo["cfg"] || lo["data"] {
		t.Fatalf("local-only set = %v (want only ref; global+default cross)", lo)
	}
}

func TestReplicateGlobalEnv(t *testing.T) {
	env := map[string]string{"WAVESPAN_REPLICATE_GLOBAL_NAMESPACES": "ref,cfg"}
	c := &Config{}
	c.applyGlobalEnv(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if c.LocalOnlyNamespaces()["ref"] {
		t.Fatal("a global namespace must not be local-only")
	}
	if !c.EverywhereNamespaces()["ref"] || !c.EverywhereNamespaces()["cfg"] {
		t.Fatalf("global namespaces should replicate everywhere: %v", c.EverywhereNamespaces())
	}
	if c.Namespaces[0].Scope() != GlobalScopeGlobal {
		t.Fatalf("env global ns scope = %v", c.Namespaces[0].Scope())
	}
}
