package aerospike

import (
	"context"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/sqlparser"
	"github.com/viant/x"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

// Statement abstraction implements database/sql driver.Statement interface
type Statement struct {
	client *as.Client
	//BaseURL    string
	SQL   string
	Kind  sqlparser.Kind
	types *x.Registry
	//query      *query.Select
	//recordType reflect.Type
	//mapper     map[int]int
	numInput int
}

// Exec executes statements
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("not supported")
}

// ExecContext executes statements
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	switch s.Kind {
	case sqlparser.KindRegisterSet:
		return s.handleRegisterSet(args)
	case sqlparser.KindSelect:
		return nil, fmt.Errorf("unsupported query type: %v", s.Kind)
	}
	return nil, nil //TODO error - unsupported kind?
}

// Query runs query
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("not supported")
}

// QueryContext runs query
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return nil, fmt.Errorf("not supported")
}

// NumInput returns numinput
func (s *Statement) NumInput() int {
	return s.numInput
}

func (s *Statement) Close() error {
	return nil
}

// //////////
// /////////
func (s *Statement) checkQueryParameters() {
	//this is very basic parameter detection, need to be improved
	// TODO
	query := strings.ToLower(s.SQL)
	count := checkQueryParameters(query)
	s.numInput = count
}

// TODO
func checkQueryParameters(query string) int {
	count := 0
	inQuote := false
	for i, c := range query {
		switch c {
		case '\'':
			if i > 1 && inQuote && query[i-1] == '\\' {
				continue
			}
			inQuote = !inQuote
		case '?', '@':
			if !inQuote {
				count++
			}
		}
	}
	return count
}

// TODO
func (s *Statement) handleRegisterSet(args []driver.NamedValue) (driver.Result, error) {
	register, err := sqlparser.ParseRegisterSet(s.SQL)
	if err != nil {
		return nil, err
	}
	spec := strings.TrimSpace(register.Spec)
	var rType reflect.Type
	if spec == "?" {
		rType = reflect.TypeOf(args[0].Value)
		if rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
		}
	} else {
		aType := xreflect.NewType(register.Name, xreflect.WithTypeDefinition(spec))
		if rType, err = aType.LoadType(xreflect.NewTypes()); err != nil {
			return nil, err
		}
	}
	aType := x.NewType(rType, x.WithName(register.Name))
	aType.PkgPath = ""
	if register.Global {
		Register(aType)
	}
	s.types.Register(aType)
	return &result{}, nil
}

// CheckNamedValue checks supported globalTypes (all for now)
func (s *Statement) CheckNamedValue(named *driver.NamedValue) error {
	return nil
}
