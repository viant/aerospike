package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v4"
	"github.com/aerospike/aerospike-client-go/v4/types"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/x"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
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
	filter     *as.Filter
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

// /////////
func (s *Statement) checkQueryParameters() {
	//this is very basic parameter detection, need to be improved
	// TODO
	aQuery := strings.ToLower(s.SQL)
	count := checkQueryParameters(aQuery)
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

func (s *Statement) executeSelect(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	//var err error
	if aType := s.types.Lookup(s.set); aType != nil {
		s.recordType = aType.Type
	} else {
		return nil, fmt.Errorf("executeselect: unable to lookup type with name %s", s.set)
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
		if s.query.Qualify != nil {
			//use query call
		} else {
			rows.rowsReader, err = s.client.ScanAll(as.NewScanPolicy(), s.namespace, s.set)
			if err != nil {
				return nil, fmt.Errorf("executeselect: unable to scan set %s due to %w", s.set, err)
			}
		}
	case 1:

		var record *as.Record
		bins := make([]string, len(aMapper.fields))
		for i, field := range aMapper.fields {
			bins[i] = field.Name
		}

		if s.query.List.IsStarExpr() {
			record, err = s.client.Get(as.NewPolicy(), keys[0])
		} else {
			record, err = s.client.Get(as.NewPolicy(), keys[0], bins...)
		}

		if err != nil {
			if IsKeyNotFound(err) {
				rows.rowsReader = newRowsReader([]*as.Record{})
				return rows, nil
			}
			return nil, err
		}

		rows.rowsReader = newRowsReader([]*as.Record{record})
	default:
		records, err := s.client.BatchGet(as.NewBatchPolicy(), keys)
		results := make([]*as.Record, 0)
		for i, _ := range records {
			if records[i] != nil {
				results = append(results, records[i])
			}
		}

		rows.rowsReader = newRowsReader(results)
		if err != nil {
			if IsKeyNotFound(err) {
				return rows, nil
			}
			return nil, err
		}
	}
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
				switch strings.ToLower(operator) {
				case "between":
					if len(exprValues) != 2 {
						return fmt.Errorf("invalid criteria values")
					}
					exprVal0, ok := exprValues[0].(expr.Value)
					if !ok {
						return fmt.Errorf("invalid criteria type expected %T but had %T", expr.Value{}, exprValues[0])
					}
					exprVal1, ok := exprValues[1].(expr.Value)
					if !ok {
						return fmt.Errorf("invalid criteria type expected %T but had %T", expr.Value{}, exprValues[1])
					}

					from, ok := exprVal0.AsInt()
					if !ok {
						return fmt.Errorf("unable to get int value from criteria value %v", exprVal0)
					}
					to, ok := exprVal1.AsInt()
					if !ok {
						return fmt.Errorf("unable to get int value from criteria value %v", exprVal1)
					}

					s.filter = as.NewRangeFilter(name, int64(from), int64(to))
					//Filter add range operator
				case "like":
					//contain
				case "=":
					//equal criteri
				}
				//you may still use aerospike query with index
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
