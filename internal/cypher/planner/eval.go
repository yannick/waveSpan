package planner

import (
	"strconv"

	"github.com/cwire/wavespan/internal/cypher/parser"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func vInt(i int64) *wavespanv1.Value {
	return &wavespanv1.Value{Value: &wavespanv1.Value_IntValue{IntValue: i}}
}
func vFloat(f float64) *wavespanv1.Value {
	return &wavespanv1.Value{Value: &wavespanv1.Value_DoubleValue{DoubleValue: f}}
}
func vStr(s string) *wavespanv1.Value {
	return &wavespanv1.Value{Value: &wavespanv1.Value_StringValue{StringValue: s}}
}
func vBool(b bool) *wavespanv1.Value {
	return &wavespanv1.Value{Value: &wavespanv1.Value_BoolValue{BoolValue: b}}
}
func vNull() *wavespanv1.Value { return &wavespanv1.Value{Value: &wavespanv1.Value_Null{Null: true}} }

// evalScalar evaluates an expression to a property Value within a binding row.
func (e *Executor) evalScalar(expr parser.Expr, row bindingRow) *wavespanv1.Value {
	switch x := expr.(type) {
	case *parser.Literal:
		return literalValue(x)
	case *parser.Parameter:
		if v, ok := e.Params[x.Name]; ok {
			return v
		}
		return vNull()
	case *parser.Variable:
		return bindingToValue(row[x.Name])
	case *parser.PropertyAccess:
		return propOf(row[x.Variable], x.Property)
	case *parser.UnaryExpr:
		return e.evalUnary(x, row)
	case *parser.BinaryExpr:
		return e.evalBinary(x, row)
	}
	return vNull()
}

func (e *Executor) evalUnary(x *parser.UnaryExpr, row bindingRow) *wavespanv1.Value {
	v := e.evalScalar(x.Expr, row)
	switch x.Op {
	case "NOT":
		return vBool(!valueTruthy(v))
	case "-":
		if iv, ok := v.GetValue().(*wavespanv1.Value_IntValue); ok {
			return vInt(-iv.IntValue)
		}
		if fv, ok := v.GetValue().(*wavespanv1.Value_DoubleValue); ok {
			return vFloat(-fv.DoubleValue)
		}
	}
	return vNull()
}

func (e *Executor) evalBinary(x *parser.BinaryExpr, row bindingRow) *wavespanv1.Value {
	switch x.Op {
	case "AND":
		return vBool(valueTruthy(e.evalScalar(x.Left, row)) && valueTruthy(e.evalScalar(x.Right, row)))
	case "OR":
		return vBool(valueTruthy(e.evalScalar(x.Left, row)) || valueTruthy(e.evalScalar(x.Right, row)))
	}
	l, r := e.evalScalar(x.Left, row), e.evalScalar(x.Right, row)
	switch x.Op {
	case "=":
		return vBool(compareValues(l, r) == 0)
	case "<>":
		return vBool(compareValues(l, r) != 0)
	case "<":
		return vBool(compareValues(l, r) < 0)
	case ">":
		return vBool(compareValues(l, r) > 0)
	case "<=":
		return vBool(compareValues(l, r) <= 0)
	case ">=":
		return vBool(compareValues(l, r) >= 0)
	case "+":
		return arith(l, r, func(a, b float64) float64 { return a + b }, func(a, b int64) int64 { return a + b })
	case "-":
		return arith(l, r, func(a, b float64) float64 { return a - b }, func(a, b int64) int64 { return a - b })
	case "*":
		return arith(l, r, func(a, b float64) float64 { return a * b }, func(a, b int64) int64 { return a * b })
	case "/":
		return arith(l, r, func(a, b float64) float64 { return a / b }, func(a, b int64) int64 {
			if b == 0 {
				return 0
			}
			return a / b
		})
	case "IN":
		if r.GetListValue() != nil {
			for _, elem := range r.GetListValue().GetValues() {
				if compareValues(l, elem) == 0 {
					return vBool(true)
				}
			}
		}
		return vBool(false)
	}
	return vNull()
}

func (e *Executor) evalBool(expr parser.Expr, row bindingRow) bool {
	return valueTruthy(e.evalScalar(expr, row))
}

func literalValue(x *parser.Literal) *wavespanv1.Value {
	switch {
	case x.Int != nil:
		return vInt(*x.Int)
	case x.Float != nil:
		return vFloat(*x.Float)
	case x.Str != nil:
		return vStr(*x.Str)
	case x.Bool != nil:
		return vBool(*x.Bool)
	case x.IsNull:
		return vNull()
	case x.List != nil:
		lst := &wavespanv1.ValueList{}
		for _, el := range x.List {
			if lit, ok := el.(*parser.Literal); ok {
				lst.Values = append(lst.Values, literalValue(lit))
			}
		}
		return &wavespanv1.Value{Value: &wavespanv1.Value_ListValue{ListValue: lst}}
	}
	return vNull()
}

func propOf(binding any, prop string) *wavespanv1.Value {
	switch b := binding.(type) {
	case *wavespanv1.NodeRecord:
		if v, ok := b.GetProperties()[prop]; ok {
			return v
		}
	case *wavespanv1.EdgeRecord:
		if v, ok := b.GetProperties()[prop]; ok {
			return v
		}
	}
	return vNull()
}

func bindingToValue(binding any) *wavespanv1.Value {
	switch b := binding.(type) {
	case *wavespanv1.Value:
		return b
	case *wavespanv1.NodeRecord:
		return vStr(b.GetNodeId())
	case *wavespanv1.EdgeRecord:
		return vStr(b.GetEdgeId())
	}
	return vNull()
}

func valueTruthy(v *wavespanv1.Value) bool {
	switch x := v.GetValue().(type) {
	case *wavespanv1.Value_BoolValue:
		return x.BoolValue
	case *wavespanv1.Value_Null:
		return false
	case nil:
		return false
	}
	return true
}

// compareValues orders two values; cross-type comparisons fall back to string form.
func compareValues(a, b *wavespanv1.Value) int {
	af, aok := asFloat(a)
	bf, bok := asFloat(b)
	if aok && bok {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	as, bs := valueKey(a), valueKey(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

func asFloat(v *wavespanv1.Value) (float64, bool) {
	switch x := v.GetValue().(type) {
	case *wavespanv1.Value_IntValue:
		return float64(x.IntValue), true
	case *wavespanv1.Value_DoubleValue:
		return x.DoubleValue, true
	}
	return 0, false
}

func arith(l, r *wavespanv1.Value, ff func(a, b float64) float64, fi func(a, b int64) int64) *wavespanv1.Value {
	li, lok := l.GetValue().(*wavespanv1.Value_IntValue)
	ri, rok := r.GetValue().(*wavespanv1.Value_IntValue)
	if lok && rok {
		return vInt(fi(li.IntValue, ri.IntValue))
	}
	lf, lf2 := asFloat(l)
	rf, rf2 := asFloat(r)
	if lf2 && rf2 {
		return vFloat(ff(lf, rf))
	}
	return vNull()
}

func valueKey(v *wavespanv1.Value) string {
	switch x := v.GetValue().(type) {
	case *wavespanv1.Value_IntValue:
		return "i" + strconv.FormatInt(x.IntValue, 10)
	case *wavespanv1.Value_DoubleValue:
		return "f" + strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *wavespanv1.Value_StringValue:
		return "s" + x.StringValue
	case *wavespanv1.Value_BoolValue:
		return "b" + strconv.FormatBool(x.BoolValue)
	}
	return "n"
}
