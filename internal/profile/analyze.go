// Package profile captures Go runtime profiles (CPU, heap, block, mutex, goroutine) from every node
// during a benchmark run and renders a cross-node breakdown of where CPU, allocations, and — most
// importantly for latency — OFF-CPU blocking time go.
package profile

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/google/pprof/profile"
)

// FuncStat is one function's aggregated flat (leaf) and cumulative (anywhere-in-stack) value.
type FuncStat struct {
	Function string `json:"function"`
	Flat     int64  `json:"flat"`
	Cum      int64  `json:"cum"`
}

// Analysis is the aggregated view of a single profile.
type Analysis struct {
	Kind  string     `json:"kind"`  // "cpu", "alloc", "block", "mutex", "goroutine"
	Unit  string     `json:"unit"`  // value unit, e.g. "nanoseconds", "bytes", "count"
	Total int64      `json:"total"` // total cumulative value across all samples (sum of leaf flat)
	Flat  []FuncStat `json:"flat"`  // functions sorted by flat, descending
	Cum   []FuncStat `json:"cum"`   // functions sorted by cum, descending
}

// valuePreference picks which sample-value column to aggregate for each profile kind: the one that
// best represents the cost — wall-time for CPU, delay for block/mutex, bytes allocated for heap.
var valuePreference = map[string][]string{
	"cpu":       {"cpu", "samples"},
	"block":     {"delay", "contentions"},
	"mutex":     {"delay", "contentions"},
	"alloc":     {"alloc_space", "inuse_space", "alloc_objects"},
	"goroutine": {"goroutine", "samples"},
}

// Analyze parses a raw pprof profile and aggregates per-function flat/cum for the kind's preferred
// value column. When focus is non-empty, only samples whose stack contains a matching frame are
// counted — used for block/mutex to isolate REQUEST-path blocking from long-lived idle background
// loops (repair/anti-entropy/evictor tickers), which otherwise dominate a block profile while doing
// no actual work. Samples whose stack contains an exclude frame are dropped even when focused —
// used to drop the pprof capture handlers' own blocking (they sleep the whole capture window
// inside ServeHTTP).
func Analyze(kind string, raw []byte, focus, exclude []string) (*Analysis, error) {
	p, err := profile.Parse(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse %s profile: %w", kind, err)
	}
	vi, unit := valueIndex(p, valuePreference[kind])
	if vi < 0 {
		return nil, fmt.Errorf("%s profile has no usable value column", kind)
	}

	stats := map[string]*FuncStat{}
	get := func(name string) *FuncStat {
		s, ok := stats[name]
		if !ok {
			s = &FuncStat{Function: name}
			stats[name] = s
		}
		return s
	}
	var total int64
	for _, s := range p.Sample {
		v := s.Value[vi]
		if v == 0 || len(s.Location) == 0 {
			continue
		}
		if len(focus) > 0 && !sampleHasFrame(s, focus) {
			continue
		}
		if len(exclude) > 0 && sampleHasFrame(s, exclude) {
			continue
		}
		total += v
		if leaf := leafFunc(s.Location[0]); leaf != "" {
			get(leaf).Flat += v
		}
		seen := map[string]bool{}
		for _, loc := range s.Location {
			for _, ln := range loc.Line {
				if ln.Function == nil {
					continue
				}
				name := ln.Function.Name
				if !seen[name] {
					seen[name] = true
					get(name).Cum += v
				}
			}
		}
	}
	return &Analysis{Kind: kind, Unit: unit, Total: total, Flat: sortByFlat(stats), Cum: sortByCum(stats)}, nil
}

// sampleHasFrame reports whether any frame in the sample's stack matches a focus substring.
func sampleHasFrame(s *profile.Sample, focus []string) bool {
	for _, loc := range s.Location {
		for _, ln := range loc.Line {
			if ln.Function == nil {
				continue
			}
			for _, f := range focus {
				if strings.Contains(ln.Function.Name, f) {
					return true
				}
			}
		}
	}
	return false
}

func leafFunc(loc *profile.Location) string {
	if len(loc.Line) == 0 || loc.Line[0].Function == nil {
		return ""
	}
	return loc.Line[0].Function.Name
}

// valueIndex returns the sample-value column matching the first preferred type name (else the last
// column, which by pprof convention is the most meaningful), plus its unit.
func valueIndex(p *profile.Profile, prefer []string) (int, string) {
	for _, want := range prefer {
		for i, st := range p.SampleType {
			if st.Type == want {
				return i, st.Unit
			}
		}
	}
	if n := len(p.SampleType); n > 0 {
		return n - 1, p.SampleType[n-1].Unit
	}
	return -1, ""
}

func statSlice(m map[string]*FuncStat) []FuncStat {
	out := make([]FuncStat, 0, len(m))
	for _, s := range m {
		out = append(out, *s)
	}
	return out
}

func sortByCum(m map[string]*FuncStat) []FuncStat {
	out := statSlice(m)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Cum != out[j].Cum {
			return out[i].Cum > out[j].Cum
		}
		return out[i].Function < out[j].Function
	})
	return out
}

func sortByFlat(m map[string]*FuncStat) []FuncStat {
	out := statSlice(m)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Flat != out[j].Flat {
			return out[i].Flat > out[j].Flat
		}
		return out[i].Function < out[j].Function
	})
	return out
}
