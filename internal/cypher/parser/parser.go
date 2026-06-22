package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type parser struct {
	toks []Token
	pos  int
}

// Parse parses a Cypher query in the v1 subset into an AST.
func Parse(input string) (*Query, error) {
	toks, err := Lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	q := &Query{}
	for !p.atEOF() {
		c, err := p.clause()
		if err != nil {
			return nil, err
		}
		q.Clauses = append(q.Clauses, c)
	}
	if len(q.Clauses) == 0 {
		return nil, fmt.Errorf("cypher: empty query")
	}
	return q, nil
}

func (p *parser) peek() Token { return p.toks[p.pos] }
func (p *parser) atEOF() bool { return p.peek().Type == TokEOF }
func (p *parser) next() Token { t := p.toks[p.pos]; p.pos++; return t }

func (p *parser) isKeyword(kw string) bool {
	return p.peek().Type == TokKeyword && p.peek().Val == kw
}

func (p *parser) acceptKeyword(kw string) bool {
	if p.isKeyword(kw) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expectKeyword(kw string) error {
	if !p.acceptKeyword(kw) {
		return fmt.Errorf("cypher: expected %s, got %q at %d", kw, p.peek().Val, p.peek().Pos)
	}
	return nil
}

func (p *parser) isPunct(s string) bool { return p.peek().Type == TokPunct && p.peek().Val == s }

func (p *parser) acceptPunct(s string) bool {
	if p.isPunct(s) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expectPunct(s string) error {
	if !p.acceptPunct(s) {
		return fmt.Errorf("cypher: expected %q, got %q at %d", s, p.peek().Val, p.peek().Pos)
	}
	return nil
}

func (p *parser) clause() (Clause, error) {
	switch {
	case p.acceptKeyword("MATCH"):
		return p.matchClause(false)
	case p.isKeyword("OPTIONAL"):
		p.next()
		if err := p.expectKeyword("MATCH"); err != nil {
			return nil, err
		}
		return p.matchClause(true)
	case p.acceptKeyword("CREATE"):
		return p.createClause()
	case p.acceptKeyword("SET"):
		return p.setClause()
	case p.acceptKeyword("DELETE"):
		return p.deleteClause(false)
	case p.acceptKeyword("RETURN"):
		return p.returnClause()
	case p.acceptKeyword("WITH"):
		return p.withClause()
	case p.acceptKeyword("UNWIND"):
		return p.unwindClause()
	case p.acceptKeyword("CALL"):
		return p.callClause()
	case p.isKeyword("MERGE"), p.isKeyword("REMOVE"), p.isKeyword("DETACH"), p.isKeyword("LOAD"):
		return nil, fmt.Errorf("cypher: %s is recommended-after-core and unsupported in v1", p.peek().Val)
	default:
		return nil, fmt.Errorf("cypher: unexpected token %q at %d", p.peek().Val, p.peek().Pos)
	}
}

func (p *parser) matchClause(optional bool) (Clause, error) {
	patterns, err := p.patternList()
	if err != nil {
		return nil, err
	}
	mc := &MatchClause{Optional: optional, Patterns: patterns}
	if p.acceptKeyword("WHERE") {
		if mc.Where, err = p.expr(); err != nil {
			return nil, err
		}
	}
	return mc, nil
}

func (p *parser) createClause() (Clause, error) {
	patterns, err := p.patternList()
	if err != nil {
		return nil, err
	}
	return &CreateClause{Patterns: patterns}, nil
}

func (p *parser) setClause() (Clause, error) {
	sc := &SetClause{}
	for {
		if p.peek().Type != TokIdent {
			return nil, fmt.Errorf("cypher: SET expects a variable, got %q", p.peek().Val)
		}
		v := p.next().Val
		if err := p.expectPunct("."); err != nil {
			return nil, fmt.Errorf("cypher: SET supports property assignment (var.prop = ...) in v1")
		}
		if p.peek().Type != TokIdent {
			return nil, fmt.Errorf("cypher: SET expects a property name")
		}
		prop := p.next().Val
		if err := p.expectPunct("="); err != nil {
			return nil, err
		}
		val, err := p.expr()
		if err != nil {
			return nil, err
		}
		sc.Items = append(sc.Items, SetItem{Variable: v, Property: prop, Value: val})
		if !p.acceptPunct(",") {
			break
		}
	}
	return sc, nil
}

func (p *parser) deleteClause(detach bool) (Clause, error) {
	dc := &DeleteClause{Detach: detach}
	for {
		if p.peek().Type != TokIdent {
			return nil, fmt.Errorf("cypher: DELETE expects a variable, got %q", p.peek().Val)
		}
		dc.Variables = append(dc.Variables, p.next().Val)
		if !p.acceptPunct(",") {
			break
		}
	}
	return dc, nil
}

func (p *parser) callClause() (Clause, error) {
	if p.peek().Type != TokIdent {
		return nil, fmt.Errorf("cypher: CALL expects a procedure name")
	}
	name := p.next().Val
	for p.acceptPunct(".") {
		if p.peek().Type != TokIdent {
			return nil, fmt.Errorf("cypher: malformed procedure name")
		}
		name += "." + p.next().Val
	}
	if !strings.HasPrefix(name, "vector.") {
		return nil, fmt.Errorf("cypher: procedure %s is unsupported in v1 (only vector.* procedures)", name)
	}
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	cc := &CallClause{Procedure: name}
	if !p.isPunct(")") {
		for {
			arg, err := p.expr()
			if err != nil {
				return nil, err
			}
			cc.Args = append(cc.Args, arg)
			if !p.acceptPunct(",") {
				break
			}
		}
	}
	if err := p.expectPunct(")"); err != nil {
		return nil, err
	}
	if p.acceptKeyword("YIELD") {
		for {
			if p.peek().Type != TokIdent {
				return nil, fmt.Errorf("cypher: YIELD expects a column name")
			}
			cc.Yields = append(cc.Yields, p.next().Val)
			if !p.acceptPunct(",") {
				break
			}
		}
	}
	return cc, nil
}

func (p *parser) unwindClause() (Clause, error) {
	e, err := p.expr()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}
	if p.peek().Type != TokIdent {
		return nil, fmt.Errorf("cypher: UNWIND ... AS expects a variable")
	}
	return &UnwindClause{Expr: e, Alias: p.next().Val}, nil
}

func (p *parser) returnClause() (Clause, error) {
	rc := &ReturnClause{}
	rc.Distinct = p.acceptKeyword("DISTINCT")
	items, err := p.returnItems()
	if err != nil {
		return nil, err
	}
	rc.Items = items
	if rc.OrderBy, rc.Skip, rc.Limit, err = p.orderSkipLimit(); err != nil {
		return nil, err
	}
	return rc, nil
}

func (p *parser) withClause() (Clause, error) {
	wc := &WithClause{}
	wc.Distinct = p.acceptKeyword("DISTINCT")
	items, err := p.returnItems()
	if err != nil {
		return nil, err
	}
	wc.Items = items
	if wc.OrderBy, wc.Skip, wc.Limit, err = p.orderSkipLimit(); err != nil {
		return nil, err
	}
	if p.acceptKeyword("WHERE") {
		if wc.Where, err = p.expr(); err != nil {
			return nil, err
		}
	}
	return wc, nil
}

func (p *parser) returnItems() ([]ReturnItem, error) {
	var items []ReturnItem
	for {
		e, err := p.expr()
		if err != nil {
			return nil, err
		}
		item := ReturnItem{Expr: e}
		if p.acceptKeyword("AS") {
			if p.peek().Type != TokIdent {
				return nil, fmt.Errorf("cypher: AS expects an alias")
			}
			item.Alias = p.next().Val
		}
		items = append(items, item)
		if !p.acceptPunct(",") {
			break
		}
	}
	return items, nil
}

func (p *parser) orderSkipLimit() ([]SortItem, Expr, Expr, error) {
	var order []SortItem
	var skip, limit Expr
	if p.acceptKeyword("ORDER") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, nil, nil, err
		}
		for {
			e, err := p.expr()
			if err != nil {
				return nil, nil, nil, err
			}
			si := SortItem{Expr: e}
			if p.acceptKeyword("DESC") {
				si.Desc = true
			} else {
				p.acceptKeyword("ASC")
			}
			order = append(order, si)
			if !p.acceptPunct(",") {
				break
			}
		}
	}
	if p.acceptKeyword("SKIP") {
		e, err := p.expr()
		if err != nil {
			return nil, nil, nil, err
		}
		skip = e
	}
	if p.acceptKeyword("LIMIT") {
		e, err := p.expr()
		if err != nil {
			return nil, nil, nil, err
		}
		limit = e
	}
	return order, skip, limit, nil
}

// --- patterns ---

func (p *parser) patternList() ([]PatternPart, error) {
	var parts []PatternPart
	for {
		part, err := p.patternPart()
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
		if !p.acceptPunct(",") {
			break
		}
	}
	return parts, nil
}

func (p *parser) patternPart() (PatternPart, error) {
	var part PatternPart
	n, err := p.nodePattern()
	if err != nil {
		return part, err
	}
	part.Node = n
	for p.isPunct("-") || p.isPunct("<-") {
		rel, err := p.relPattern()
		if err != nil {
			return part, err
		}
		nn, err := p.nodePattern()
		if err != nil {
			return part, err
		}
		part.Rels = append(part.Rels, rel)
		part.Nodes = append(part.Nodes, nn)
	}
	return part, nil
}

func (p *parser) nodePattern() (NodePattern, error) {
	var n NodePattern
	if err := p.expectPunct("("); err != nil {
		return n, err
	}
	if p.peek().Type == TokIdent {
		n.Variable = p.next().Val
	}
	for p.acceptPunct(":") {
		if p.peek().Type != TokIdent && p.peek().Type != TokKeyword {
			return n, fmt.Errorf("cypher: expected label after ':'")
		}
		n.Labels = append(n.Labels, p.next().Val)
	}
	if p.isPunct("{") {
		props, err := p.properties()
		if err != nil {
			return n, err
		}
		n.Properties = props
	}
	if err := p.expectPunct(")"); err != nil {
		return n, err
	}
	return n, nil
}

func (p *parser) relPattern() (RelPattern, error) {
	var r RelPattern
	leftArrow := p.acceptPunct("<-")
	if !leftArrow {
		if err := p.expectPunct("-"); err != nil {
			return r, err
		}
	}
	if p.acceptPunct("[") {
		if p.peek().Type == TokIdent {
			r.Variable = p.next().Val
		}
		if p.acceptPunct(":") {
			for {
				if p.peek().Type != TokIdent && p.peek().Type != TokKeyword {
					return r, fmt.Errorf("cypher: expected relationship type after ':'")
				}
				r.Types = append(r.Types, p.next().Val)
				if !p.acceptPunct("|") {
					break
				}
			}
		}
		if p.isPunct("{") {
			props, err := p.properties()
			if err != nil {
				return r, err
			}
			r.Properties = props
		}
		if err := p.expectPunct("]"); err != nil {
			return r, err
		}
	}
	rightArrow := p.acceptPunct("->")
	if !rightArrow {
		if err := p.expectPunct("-"); err != nil {
			return r, err
		}
	}
	switch {
	case leftArrow && !rightArrow:
		r.Direction = DirIn
	case !leftArrow && rightArrow:
		r.Direction = DirOut
	default:
		r.Direction = DirBoth
	}
	return r, nil
}

func (p *parser) properties() (map[string]Expr, error) {
	if err := p.expectPunct("{"); err != nil {
		return nil, err
	}
	props := map[string]Expr{}
	if p.acceptPunct("}") {
		return props, nil
	}
	for {
		if p.peek().Type != TokIdent && p.peek().Type != TokKeyword {
			return nil, fmt.Errorf("cypher: expected property key")
		}
		key := p.next().Val
		if err := p.expectPunct(":"); err != nil {
			return nil, err
		}
		v, err := p.expr()
		if err != nil {
			return nil, err
		}
		props[key] = v
		if !p.acceptPunct(",") {
			break
		}
	}
	return props, p.expectPunct("}")
}

// --- expressions (precedence climbing) ---

func (p *parser) expr() (Expr, error) { return p.orExpr() }

func (p *parser) orExpr() (Expr, error) {
	left, err := p.andExpr()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("OR") {
		right, err := p.andExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: "OR", Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) andExpr() (Expr, error) {
	left, err := p.notExpr()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("AND") {
		right, err := p.notExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: "AND", Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) notExpr() (Expr, error) {
	if p.acceptKeyword("NOT") {
		e, err := p.notExpr()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "NOT", Expr: e}, nil
	}
	return p.comparison()
}

var compareOps = map[string]bool{"=": true, "<>": true, "<": true, ">": true, "<=": true, ">=": true}

func (p *parser) comparison() (Expr, error) {
	left, err := p.additive()
	if err != nil {
		return nil, err
	}
	if p.peek().Type == TokPunct && compareOps[p.peek().Val] {
		op := p.next().Val
		right, err := p.additive()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Op: op, Left: left, Right: right}, nil
	}
	if p.acceptKeyword("IN") {
		right, err := p.additive()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Op: "IN", Left: left, Right: right}, nil
	}
	return left, nil
}

func (p *parser) additive() (Expr, error) {
	left, err := p.multiplicative()
	if err != nil {
		return nil, err
	}
	for p.isPunct("+") || p.isPunct("-") {
		op := p.next().Val
		right, err := p.multiplicative()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) multiplicative() (Expr, error) {
	left, err := p.primary()
	if err != nil {
		return nil, err
	}
	for p.isPunct("*") || p.isPunct("/") {
		op := p.next().Val
		right, err := p.primary()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) primary() (Expr, error) {
	t := p.peek()
	switch t.Type {
	case TokInt:
		p.next()
		n, _ := strconv.ParseInt(t.Val, 10, 64)
		return &Literal{Int: &n}, nil
	case TokFloat:
		p.next()
		f, _ := strconv.ParseFloat(t.Val, 64)
		return &Literal{Float: &f}, nil
	case TokString:
		p.next()
		s := t.Val
		return &Literal{Str: &s}, nil
	case TokParam:
		p.next()
		return &Parameter{Name: t.Val}, nil
	case TokKeyword:
		switch t.Val {
		case "TRUE", "FALSE":
			p.next()
			b := t.Val == "TRUE"
			return &Literal{Bool: &b}, nil
		case "NULL":
			p.next()
			return &Literal{IsNull: true}, nil
		}
		return nil, fmt.Errorf("cypher: unexpected keyword %q in expression", t.Val)
	case TokIdent:
		p.next()
		if p.acceptPunct(".") {
			if p.peek().Type != TokIdent {
				return nil, fmt.Errorf("cypher: expected property name after '.'")
			}
			return &PropertyAccess{Variable: t.Val, Property: p.next().Val}, nil
		}
		return &Variable{Name: t.Val}, nil
	case TokPunct:
		switch t.Val {
		case "(":
			p.next()
			e, err := p.expr()
			if err != nil {
				return nil, err
			}
			return e, p.expectPunct(")")
		case "[":
			return p.listLiteral()
		case "{":
			m, err := p.properties()
			if err != nil {
				return nil, err
			}
			return &Literal{Map: m}, nil
		case "-":
			p.next()
			e, err := p.primary()
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{Op: "-", Expr: e}, nil
		}
	}
	return nil, fmt.Errorf("cypher: unexpected token %q at %d in expression", t.Val, t.Pos)
}

func (p *parser) listLiteral() (Expr, error) {
	if err := p.expectPunct("["); err != nil {
		return nil, err
	}
	lit := &Literal{List: []Expr{}}
	if p.acceptPunct("]") {
		return lit, nil
	}
	for {
		e, err := p.expr()
		if err != nil {
			return nil, err
		}
		lit.List = append(lit.List, e)
		if !p.acceptPunct(",") {
			break
		}
	}
	return lit, p.expectPunct("]")
}
