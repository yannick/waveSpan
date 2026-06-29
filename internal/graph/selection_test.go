package graph

import (
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func TestGraphOfKey(t *testing.T) {
	val := &wavespanv1.Value{Value: &wavespanv1.Value_StringValue{StringValue: "x"}}
	cases := map[string][]byte{
		"node":   NodeKey("g", "n1"),
		"edge":   EdgeKey("g", "e1"),
		"label":  LabelKey("g", "Person", "n1"),
		"prop":   PropKey("g", "Person", "age", val, "n1"),
		"outAdj": OutAdjKey("g", "n1", "KNOWS", "n2", "e1"),
		"inAdj":  InAdjKey("g", "n2", "KNOWS", "n1", "e1"),
	}
	for name, k := range cases {
		g, ok := GraphOfKey(k)
		if !ok || g != "g" {
			t.Fatalf("%s: g=%q ok=%v, want g/true", name, g, ok)
		}
	}
	if _, ok := GraphOfKey([]byte{'z', 0x01, 'g'}); ok {
		t.Fatal("unknown leading byte must decode ok=false")
	}
	if _, ok := GraphOfKey(nil); ok {
		t.Fatal("empty key must decode ok=false")
	}
}
