package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/x"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

// Statement abstraction implements database/sql driver.Statement interface
type Statement struct {
	client *as.Client
	cfg    *Config
	//BaseURL    string
	SQL        string
	Kind       sqlparser.Kind
	types      *x.Registry
	query      *query.Select
	recordType reflect.Type
	//mapper     map[int]int
	numInput  int
	set       string
	mapBin    string
	namespace string
	pkValues  []interface{}
	keyValues []interface{}
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

	switch s.Kind {
	case sqlparser.KindSelect:
	default:
		return nil, fmt.Errorf("unsupported query type: %v", s.Kind)
	}

	s.set = sqlparser.Stringify(s.query.From.X)
	if index := strings.Index(s.set, "."); index != -1 {
		s.namespace = s.set[:index]
		s.set = s.set[index+1:]
	}

	return s.executeSelect(ctx, args)
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

func (s *Statement) prepareSelect(SQL string) error {
	var err error
	if s.query, err = sqlparser.ParseQuery(SQL); err != nil {
		return err
	}
	return nil
}

// ///
func (s *Statement) executeSelect(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	//var err error
	if aType := s.types.Lookup(s.set); aType != nil {
		s.recordType = aType.Type
	} else { //TODO
		//if s.recordType, err = s.autodetectType(ctx, resources); err != nil {
		//	return nil, err
		//}
	}

	aMapper, err := newMapper(s.recordType, s.query.List)
	if err != nil {
		return nil, err
	}

	row := reflect.New(s.recordType).Interface()
	rows := &Rows{
		zeroRecord: unsafe.Slice((*byte)(xunsafe.AsPointer(row)), s.recordType.Size()),
		record:     row,
		recordType: s.recordType,
		mapper:     aMapper,
		query:      s.query,
	}

	if err := s.updateCriteria(err, args); err != nil {
		return nil, err
	}

	keys, err := s.buildKeys()
	if err != nil {
		return nil, err
	}

	switch len(keys) {
	case 0:
		return nil, fmt.Errorf("unsupported scaning")
	case 1:
		record, err := s.client.Get(as.NewPolicy(), keys[0])
		if err != nil {
			if IsKeyNotFound(err) {
				return rows, nil
			}
			return nil, err
		}
		rows.records = append(rows.records, record)
	default:
		rows.records, err = s.client.BatchGet(as.NewBatchPolicy(), keys)
		if err != nil {
			return nil, err
		}
	}

	//s.client.BatchGet(nil, []*as.Key{key}) // TODO

	// s.client.Query(nil, nil, nil) // TODO needs secondary index ?

	//
	//if criteria != "" {
	//	if err = rows.initCriteria(s.query.Qualify, args); err != nil {
	//		return nil, err
	//	}
	//}

	return rows, nil
}

func (s *Statement) updateCriteria(err error, args []driver.NamedValue) error {

	if qualify := s.query.Qualify; qualify != nil {
		binary, ok := qualify.X.(*expr.Binary)
		if !ok {
			return fmt.Errorf("unsupported expr type: %T", qualify.X)
		}
		err := binary.Walk(func(ident node.Node, values expr.Values, operator, parentOperator string) error {
			if parentOperator != "" && strings.ToUpper(parentOperator) != "AND" {
				return fmt.Errorf("unuspported logical operator: %s", parentOperator)
			}
			name := strings.ToLower(sqlparser.Stringify(ident))
			var exprValues = values.Values(func(idx int) interface{} {
				return args[idx].Value
			})
			switch name {
			case "pk":
				s.pkValues = exprValues
			case "key":
				s.keyValues = exprValues
			default:
				return fmt.Errorf("unuppred criteria column: %s", name)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return err
}

func (s *Statement) buildKeys() ([]*as.Key, error) {
	var result = make([]*as.Key, len(s.pkValues))
	for i, item := range s.pkValues {
		key, err := as.NewKey(s.namespace, s.set, item)
		if err != nil {
			return nil, err
		}
		result[i] = key
	}
	return result, nil
}

func extractColumnValue(binary *expr.Binary, args []driver.NamedValue) ([]interface{}, error) {

	value := extractValue(binary.X)
	if value == nil {
		value = extractValue(binary.Y)
	}
	switch actual := value.(type) {
	case *expr.Placeholder:
		return []interface{}{args[0].Value}, nil
	case *expr.Literal:
		switch actual.Kind {
		case "int":
			v, err := strconv.Atoi(actual.Value)
			if err != nil {
				return nil, err
			}
			return []interface{}{v}, nil
		case "null":
			return []interface{}{nil}, nil
		case "string":
			return []interface{}{strings.Trim(actual.Value, "'")}, nil
		case "numeric":
			v, err := strconv.ParseFloat(actual.Value, 64)
			if err != nil {
				return nil, err
			}
			return []interface{}{v}, nil

		}
		return []interface{}{actual.Value}, nil
		//	case *expr.Parenthesis:
	}
	return nil, fmt.Errorf("not yet supported")
}

func extractColumn(binary *expr.Binary) string {
	if name := extractName(binary.X); name != "" {
		return name
	}
	return extractName(binary.Y)
}

func extractName(n node.Node) string {
	if ret, ok := n.(*expr.Selector); ok {
		return sqlparser.Stringify(ret)
	}
	if ret, ok := n.(*expr.Ident); ok {
		return ret.Name
	}
	return ""
}

func extractValue(n node.Node) node.Node {
	if ret, ok := n.(*expr.Placeholder); ok {
		return ret
	}
	if ret, ok := n.(*expr.Parenthesis); ok {
		return ret
	}
	if ret, ok := n.(*expr.Literal); ok {
		return ret
	}
	return nil
}

// IsKeyNotFound returns true if key not found error.
func IsKeyNotFound(err error) bool {
	if err == nil {
		return false
	}
	aeroError, ok := err.(types.AerospikeError)
	if !ok {
		err = errors.Unwrap(err)
		if err == nil {
			return false
		}
		if aeroError, ok = err.(types.AerospikeError); !ok {
			return false
		}

	}
	return aeroError.ResultCode() == types.KEY_NOT_FOUND_ERROR
}
