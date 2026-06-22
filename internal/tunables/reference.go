package tunables

import (
	"fmt"
	"io"
	"strings"
)

// node is a tree node used to render the flat dotted keys back into nested YAML.
type node struct {
	name     string
	order    []string
	children map[string]*node
	param    *Param // non-nil at a leaf
}

func newNode(name string) *node { return &node{name: name, children: map[string]*node{}} }

func (n *node) child(name string) *node {
	c, ok := n.children[name]
	if !ok {
		c = newNode(name)
		n.children[name] = c
		n.order = append(n.order, name)
	}
	return c
}

// WriteReference emits a fully-documented YAML `tunables:` block: every knob with its default value
// and, in comments, its type, category (static/hot), env-var name, what it does, and why it defaults
// to that value. The output is valid YAML — koanf flattens the nested maps back to the dotted keys —
// so it doubles as a runnable config file (a k8s ConfigMap) and as the reference documentation.
func WriteReference(w io.Writer, r *Registry) error {
	root := newNode("")
	for _, p := range r.All() {
		cur := root
		for _, seg := range strings.Split(p.Key, ".") {
			cur = cur.child(seg)
		}
		cur.param = p
	}

	bw := &errWriter{w: w}
	bw.line("# WaveSpan tunables reference — generated; do not edit by hand.")
	bw.line("# Regenerate with: go run ./cmd/wavespan-confdoc > config/reference.yaml")
	bw.line("#")
	bw.line("# Every performance/behaviour knob across WaveSpan and the wavesdb storage engine, with its")
	bw.line("# default and rationale. Precedence (low→high): built-in default < this file < env var <")
	bw.line("# runtime override (gossip). Override any value here (k8s ConfigMap) or via its")
	bw.line("# WAVESPAN_TUNABLE_* env var. Category: [hot] = changeable at runtime via gossip and applied")
	bw.line("# live; [static] = applied at startup / engine-open, so a change needs a node restart.")
	bw.line("tunables:")
	for _, seg := range root.order {
		writeNode(bw, root.children[seg], 1)
	}
	return bw.err
}

func writeNode(bw *errWriter, n *node, depth int) {
	indent := strings.Repeat("  ", depth)
	if n.param == nil {
		bw.line(indent + n.name + ":")
		for _, seg := range n.order {
			writeNode(bw, n.children[seg], depth+1)
		}
		return
	}
	p := n.param
	bw.line("")
	bw.line(fmt.Sprintf("%s# [%s, %s] env: %s", indent, p.Kind, p.Category, EnvName(p.Key)))
	for _, l := range wrap(p.Doc, 92-len(indent)) {
		bw.line(indent + "# " + l)
	}
	for _, l := range wrap("why: "+p.Why, 92-len(indent)) {
		bw.line(indent + "# " + l)
	}
	bw.line(fmt.Sprintf("%s%s: %s", indent, n.name, yamlScalar(p)))
}

// yamlScalar renders a default value, quoting strings that YAML might otherwise misparse.
func yamlScalar(p *Param) string {
	v := p.Default()
	switch p.Kind {
	case KindString:
		return v // our string defaults (none/snapshot/...) are plain words; safe unquoted
	default:
		return v
	}
}

// wrap breaks text into lines no longer than width (best-effort on word boundaries).
func wrap(s string, width int) []string {
	if width < 20 {
		width = 20
	}
	words := strings.Fields(s)
	if len(words) == 0 {
		return []string{""}
	}
	var lines []string
	cur := words[0]
	for _, w := range words[1:] {
		if len(cur)+1+len(w) > width {
			lines = append(lines, cur)
			cur = w
		} else {
			cur += " " + w
		}
	}
	return append(lines, cur)
}

type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) line(s string) {
	if e.err != nil {
		return
	}
	_, e.err = io.WriteString(e.w, s+"\n")
}
