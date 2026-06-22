package parser

// Query is a parsed Cypher query in the v1 subset (design/07): an ordered list of clauses.
type Query struct {
	Clauses []Clause
}

// Clause is a reading, updating, projecting, or unwind clause.
type Clause interface{ isClause() }

// MatchClause is MATCH / OPTIONAL MATCH with an optional WHERE.
type MatchClause struct {
	Optional bool
	Patterns []PatternPart
	Where    Expr
}

// CreateClause creates nodes/edges.
type CreateClause struct {
	Patterns []PatternPart
}

// SetItem assigns target.Property = Value (or target = map, simplified to property set in v1).
type SetItem struct {
	Variable string
	Property string
	Value    Expr
}

// SetClause is SET a.x = 1, b.y = 2.
type SetClause struct{ Items []SetItem }

// DeleteClause is DELETE n (DETACH unsupported in v1, rejected at parse time).
type DeleteClause struct {
	Variables []string
	Detach    bool
}

// UnwindClause is UNWIND <list> AS var.
type UnwindClause struct {
	Expr  Expr
	Alias string
}

// ReturnItem is an expression with an optional alias.
type ReturnItem struct {
	Expr  Expr
	Alias string
}

// SortItem is an ORDER BY key.
type SortItem struct {
	Expr Expr
	Desc bool
}

// ReturnClause is RETURN [DISTINCT] items [ORDER BY] [SKIP] [LIMIT].
type ReturnClause struct {
	Distinct bool
	Items    []ReturnItem
	OrderBy  []SortItem
	Skip     Expr
	Limit    Expr
}

// WithClause is WITH items [WHERE] [ORDER BY] [SKIP] [LIMIT].
type WithClause struct {
	Distinct bool
	Items    []ReturnItem
	Where    Expr
	OrderBy  []SortItem
	Skip     Expr
	Limit    Expr
}

// CallClause is CALL proc.name(args) [YIELD cols]. v1 only allows vector.* procedures.
type CallClause struct {
	Procedure string
	Args      []Expr
	Yields    []string
}

func (*MatchClause) isClause()  {}
func (*CreateClause) isClause() {}
func (*SetClause) isClause()    {}
func (*DeleteClause) isClause() {}
func (*UnwindClause) isClause() {}
func (*ReturnClause) isClause() {}
func (*WithClause) isClause()   {}
func (*CallClause) isClause()   {}

// Direction of a relationship pattern.
type Direction int

// Relationship directions.
const (
	DirOut  Direction = iota // -[]->
	DirIn                    // <-[]-
	DirBoth                  // -[]-
)

// NodePattern is (var:Label {props}).
type NodePattern struct {
	Variable   string
	Labels     []string
	Properties map[string]Expr
}

// RelPattern is -[var:TYPE {props}]->.
type RelPattern struct {
	Variable   string
	Types      []string
	Direction  Direction
	Properties map[string]Expr
}

// PatternPart is a node followed by zero or more (relationship, node) hops.
type PatternPart struct {
	Node  NodePattern
	Rels  []RelPattern
	Nodes []NodePattern // Nodes[i] is the node reached by Rels[i]
}

// Expr is an expression.
type Expr interface{ isExpr() }

// Variable references a bound variable.
type Variable struct{ Name string }

// PropertyAccess is variable.property.
type PropertyAccess struct {
	Variable string
	Property string
}

// Literal is a constant value.
type Literal struct {
	// exactly one is set
	Int    *int64
	Float  *float64
	Str    *string
	Bool   *bool
	IsNull bool
	List   []Expr
	Map    map[string]Expr
}

// Parameter is $name.
type Parameter struct{ Name string }

// BinaryExpr is left <op> right (comparisons, AND/OR, arithmetic).
type BinaryExpr struct {
	Op    string
	Left  Expr
	Right Expr
}

// UnaryExpr is NOT expr / -expr.
type UnaryExpr struct {
	Op   string
	Expr Expr
}

func (*Variable) isExpr()       {}
func (*PropertyAccess) isExpr() {}
func (*Literal) isExpr()        {}
func (*Parameter) isExpr()      {}
func (*BinaryExpr) isExpr()     {}
func (*UnaryExpr) isExpr()      {}
