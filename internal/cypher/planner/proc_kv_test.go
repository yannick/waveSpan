package planner

import (
	"testing"

	"github.com/cwire/wavespan/internal/cypher/parser"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func init() {
	RegisterFunction("test.echo", func(_ *Executor, args []*wavespanv1.Value, _ bindingRow) (*wavespanv1.Value, error) {
		return args[0], nil
	})
}

func runQuery(t *testing.T, e *Executor, q string) *Result {
	t.Helper()
	ast, err := parser.Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	res, err := e.Execute(ast)
	if err != nil {
		t.Fatalf("execute %q: %v", q, err)
	}
	return res
}

func TestScalarFunctionEvaluated(t *testing.T) {
	e := &Executor{}
	res := runQuery(t, e, "RETURN test.echo('hi') AS v")
	if got := res.Rows[0]["v"].GetStringValue(); got != "hi" {
		t.Fatalf("got %q", got)
	}
}

func TestUnknownFunctionIsHardError(t *testing.T) {
	e := &Executor{}
	ast, _ := parser.Parse("RETURN nope.fn() AS v")
	if _, err := e.Execute(ast); err == nil {
		t.Fatal("unknown function must be a hard error")
	}
}
