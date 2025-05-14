package insert

import (
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/insert"
	"github.com/viant/sqlparser/node"
)

type Kind string

const (
	// PlaceholderToken is the bind‐marker
	Placeholder = "?"

	// ParameterizedValuesOnly indicates that this INSERT
	// uses placeholders only in its VALUES clause.
	ParameterizedValuesOnly Kind = "ParameterizedValuesOnlyInsert"
)

var placeholderValue *insert.Value = makePlaceholderValue()

type Statement struct {
	*insert.Statement
	Kind                      Kind
	ValuesOnlyPlaceholdersCnt int
	PlaceholderValue          *insert.Value
}

// NewStatement allocates a Statement with a pre-built placeholder node.
func NewStatement(base *insert.Statement) *Statement {
	return &Statement{
		Statement: base,
	}
}

// ValuesCnt returns the logical row-count: either the literal Values slice
// length or the placeholder count.
func (s *Statement) ValuesCnt() int {
	if s.Kind == ParameterizedValuesOnly {
		return s.ValuesOnlyPlaceholdersCnt
	}
	return len(s.Values)
}

// ValueAt returns either the real literal for index i, or the shared
// PlaceholderValue if in "values‐only" mode.
func (s *Statement) ValueAt(i int) *insert.Value {
	if s.Kind == ParameterizedValuesOnly {
		return s.PlaceholderValue
	}
	return s.Values[i]
}

// PrepareValuesOnly mutates this Statement to values-only mode.
// Call this only on a fresh/unshared Statement.
func (s *Statement) PrepareValuesOnly(placeholderCount int) {
	s.Kind = ParameterizedValuesOnly
	s.ValuesOnlyPlaceholdersCnt = placeholderCount
	s.Values = nil
	s.PlaceholderValue = placeholderValue
}

// CloneForValuesOnly returns a shallow clone prepared for values-only usage.
// It copies only the top-level struct and fields we’ll change.
func (s *Statement) CloneForValuesOnly(placeholderCount int) *Statement {
	dup := *s
	dup.ValuesOnlyPlaceholdersCnt = placeholderCount
	return &dup
}

func makePlaceholderValue() *insert.Value {
	operand := expr.NewPlaceholder(Placeholder)
	value := &insert.Value{Expr: operand,
		Span: node.Span{Begin: uint32(0), End: uint32(1)},
		Raw:  Placeholder}

	return value
}
