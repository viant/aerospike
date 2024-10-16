package aerospike

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/viant/sqlparser/index"
	"github.com/viant/sqlparser/table"

	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/aerospike/aerospike-client-go/v6/types"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/delete"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/insert"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlparser/update"
	"github.com/viant/x"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

type collectionType string

func (c collectionType) IsMap() bool {
	return c == collectionTypeMap
}

func (c collectionType) IsArray() bool {
	return c == collectionTypeArray
}

const (
	collectionTypeMap   collectionType = "map"
	collectionTypeArray collectionType = "array"
)

// Statement abstraction implements database/sql driver.Statement interface
type Statement struct {
	client *as.Client
	cfg    *Config
	//BaseURL    string
	SQL              string
	kind             sqlparser.Kind
	sets             *registry
	query            *query.Select
	insert           *insert.Statement
	update           *update.Statement
	delete           *delete.Statement
	truncate         *table.Truncate
	createIndex      *index.Create
	dropIndex        *index.Drop
	mapper           *mapper
	filter           *as.Filter
	mapRangeFilter   *rangeBinFilter
	arrayRangeFilter *rangeBinFilter
	recordType       reflect.Type
	falsePredicate   bool
	record           interface{}
	numInput         int
	set              string
	source           string
	componentKey     string

	collectionBin        string
	collectionType       collectionType
	namespace            string
	pkValues             []interface{}
	mapKeyValues         []interface{}
	arrayIndexValues     []int
	secondaryIndexValues []interface{}
	lastInsertID         *int64
	affected             int64
}

// Exec executes statements
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("not supported")
}

// ExecContext executes statements
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	ret := &result{totalRows: s.affected}

	switch s.kind {
	case sqlparser.KindRegisterSet:
		return s.handleRegisterSet(args)
	case sqlparser.KindInsert:
		if err := s.handleInsert(args); err != nil {
			return nil, err
		}
	case sqlparser.KindUpdate:
		if err := s.handleUpdate(args); err != nil {
			return nil, err
		}
	case sqlparser.KindDelete:
		return nil, s.handleDelete(args)
	case sqlparser.KindSelect:
		return nil, fmt.Errorf("unsupported parameterizedQuery type: %v", s.kind)
	case sqlparser.KindDropIndex:
		return s.handleDropIndex(args)
	case sqlparser.KindCreateIndex:
		return s.handleCreateIndex(args)
	case sqlparser.KindTruncateTable:
		return s.handleTruncateTable(args)
	}

	if s.lastInsertID != nil {
		ret.lastInsertedID = *s.lastInsertID
		ret.hasLastInsertedID = true
	}
	return ret, nil
}

// Query runs parameterizedQuery
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("not supported")
}

// QueryContext runs parameterizedQuery
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	switch s.kind {
	case sqlparser.KindSelect:
	default:
		return nil, fmt.Errorf("unsupported parameterizedQuery type: %v", s.kind)
	}
	return s.executeSelect(ctx, args)
}

func (s *Statement) setSet(source string) {
	source = strings.ReplaceAll(source, "`", "")
	s.set = source
	s.source = source
	if index := strings.Index(source, "."); index != -1 {
		s.namespace = source[:index]
		s.set = source[index+1:]
		source = s.set
	}
	if index := strings.Index(source, "/"); index != -1 {
		s.collectionBin = source[index+1:]
		s.set = source[:index]
	}
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
		return nil, fmt.Errorf("unable to parse set definition: %s due to: %w", s.SQL, err)
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
			return nil, fmt.Errorf("unable to register set %s and spec %s due to: %w", register.Name, spec, err)
		}
	}
	aSet := &set{
		xType:  x.NewType(rType, x.WithName(register.Name)),
		ttlSec: register.TTL, //TODO use TTL with WritePolicy
	}
	if register.Global {
		if err := registerSet(aSet); err != nil {
			return nil, err
		}
	}

	err = s.sets.Register(aSet)
	if err != nil {
		return nil, err
	}

	return &result{}, nil
}

// CheckNamedValue checks supported globalTypes (all for now)
func (s *Statement) CheckNamedValue(named *driver.NamedValue) error {
	return nil
}

func (s *Statement) prepareDelete(sql string) error {
	var err error
	if s.delete, err = sqlparser.ParseDelete(sql); err != nil {
		return err
	}
	s.setSet(sqlparser.Stringify(s.delete.Target.X))
	return nil
}

func (s *Statement) setRecordType(aSet *set) error {
	if aSet.xType == nil {
		return fmt.Errorf("setrecordtype: unable to lookup type with name %s", s.set)
	}

	if aSet.xType.Type == nil {
		return fmt.Errorf("setrecordtype: rtype is nil for type with name %s", s.set)
	}

	s.recordType = aSet.xType.Type
	return nil
}

func (s *Statement) updateCriteria(qualify *expr.Qualify, args []driver.NamedValue, includeFilter bool) error {
	if qualify == nil {
		return nil
	}
	binary, ok := qualify.X.(*expr.Binary)
	if !ok {
		return fmt.Errorf("unsupported expr type: %T", qualify.X)
	}
	pkName := "-"
	if s.mapper != nil && len(s.mapper.pk) == 1 {
		pkName = strings.ToLower(s.mapper.pk[0].Column())
	}
	keyName := "--"
	if s.mapper != nil && len(s.mapper.mapKey) == 1 {
		keyName = strings.ToLower(s.mapper.mapKey[0].Column())
	}

	arrayIndex := "---"
	if s.mapper != nil && s.mapper.arrayIndex != nil {
		arrayIndex = strings.ToLower(s.mapper.arrayIndex.Column())
	}

	indexName := "----"
	if s.mapper != nil && s.mapper.secondaryIndex != nil {
		indexName = strings.ToLower(s.mapper.secondaryIndex.Column())
	}
	isMultiInPk := len(s.mapper.pk) > 1
	isMultiInKey := len(s.mapper.mapKey) > 1
	isSecondaryIndexKey := s.mapper.secondaryIndex != nil
	if binary.Op == "=" {
		if leftLiteral, ok := binary.X.(*expr.Literal); ok {
			if rightLiteral, ok := binary.Y.(*expr.Literal); ok {
				s.falsePredicate = !(leftLiteral.Value == rightLiteral.Value)
				return nil
			}
		}
	}
	idx := 0
	err := binary.Walk(func(ident node.Node, values *expr.Values, operator, parentOperator string) error {
		if parentOperator != "" && strings.ToUpper(parentOperator) != "AND" {
			return fmt.Errorf("unuspported logical operator: %s", parentOperator)
		}
		values.Idx = idx
		name := strings.ToLower(sqlparser.Stringify(ident))
		if idx := strings.Index(name, "."); idx != -1 {
			name = name[idx+1:]
		}
		var exprValues = values.Values(func(idx int) interface{} {
			return args[idx].Value
		})
		idx = values.Idx
		//TODO add support for multi in (col1,col2) IN((?, ?), (?, ?))
		if isMultiInPk {

		}
		//TODO add support for multi in (col1,col2) IN((?, ?), (?, ?))
		if isMultiInKey {

		}
		if isSecondaryIndexKey {
			if name == s.mapper.secondaryIndex.Column() {
				s.secondaryIndexValues = exprValues
				return nil
			}
		}
		switch name {
		case indexName:
			s.secondaryIndexValues = exprValues
		case "pk", pkName:
			s.pkValues = exprValues
		case arrayIndex, "index":
			switch strings.ToLower(operator) {
			case "=", "in":
				for _, item := range exprValues {
					i, ok := item.(int)
					if !ok {
						return fmt.Errorf("unable to get int value from criteria value %v", item)
					}
					s.arrayIndexValues = append(s.arrayIndexValues, i)
				}
			case "between":
				filter, err := s.buildRangeFilter(exprValues, name)
				if err != nil {
					return err
				}
				s.arrayRangeFilter = filter
			default:
				return fmt.Errorf("unsupported operator of a mapbin mapKey: %s", operator)
			}

		case "key", keyName:
			switch strings.ToLower(operator) {
			case "=", "in":
				s.mapKeyValues = exprValues
			case "between":
				filter, err := s.buildRangeFilter(exprValues, name)
				if err != nil {
					return err
				}
				s.mapRangeFilter = filter
			default:
				return fmt.Errorf("unsupported operator of a mapbin mapKey: %s", operator)
			}
		default:
			if !includeFilter {
				return fmt.Errorf("unsupported criteria: %s", name)
			}
			switch strings.ToLower(operator) {
			case "between": //TODO use asfilter
				if len(exprValues) != 2 {
					return fmt.Errorf("invalid criteria values")
				}
				from, ok := exprValues[0].(int)
				if !ok {
					return fmt.Errorf("unable to get int value from criteria value %v", exprValues[0])
				}

				to, ok := exprValues[1].(int)
				if !ok {
					return fmt.Errorf("unable to get int value from criteria value %v", exprValues[1])
				}

				s.filter = as.NewRangeFilter(name, int64(from), int64(to))
				//Filter add range operator
			case "=", "in":
			default:
				return fmt.Errorf("unsupported operator of a mapbin mapKey: %s", operator)
				//equal criteria
			}
			//you may still use aerospike parameterizedQuery with secondaryIndex
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Statement) buildRangeFilter(exprValues []interface{}, name string) (*rangeBinFilter, error) {
	if len(exprValues) != 2 {
		return nil, fmt.Errorf("invalid criteria - between expects 2 values")
	}

	from, ok := exprValues[0].(int)
	if !ok {
		return nil, fmt.Errorf("unable to get int value from criteria value %v", exprValues[0])
	}

	to, ok := exprValues[1].(int)
	if !ok {
		return nil, fmt.Errorf("unable to get int value from criteria value %v", exprValues[1])
	}

	filter := &rangeBinFilter{
		name:  name,
		begin: from,
		end:   to,
	}
	return filter, nil
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

func (s *Statement) setTypeBasedMapper() error {
	var err error
	if s.set == "" {
		return nil
	}
	aSet, err := s.lookupSet()
	if err != nil {
		return fmt.Errorf("executeselect: unable to lookup set with name %s, %w", s.set, err)
	}
	err = s.setRecordType(aSet)
	if err != nil {
		return err
	}

	s.record = reflect.New(s.recordType).Interface()
	if aSet.typeBasedMapper == nil {
		if s.mapper, err = newTypeBasedMapper(s.recordType); err != nil {
			return err
		}
		aSet.typeBasedMapper = s.mapper
	} else {
		s.mapper = aSet.typeBasedMapper
	}

	if len(s.mapper.mapKey) > 0 {
		s.collectionType = collectionTypeMap
	} else if s.mapper.arrayIndex != nil {
		s.collectionType = collectionTypeArray
	}

	return nil
}

func (s *Statement) handleDelete(args []driver.NamedValue) error {
	if s.delete.Qualify == nil {
		return s.client.Truncate(nil, s.namespace, s.set, nil)
	}
	if isDryRun("delete") {
		return nil
	}
	//TODO add support for single/batch delete
	return fmt.Errorf("not yet supported")
}

func (s *Statement) getKey(fields []*field, bins map[string]interface{}) interface{} {
	if len(fields) == 1 {
		return bins[fields[0].Column()]
	}
	builder := strings.Builder{}
	for i, key := range fields {
		if i > 0 {
			builder.WriteString(":")
		}
		builder.WriteString(fmt.Sprintf("%v", bins[key.Column()]))
	}
	return nil
}

// IsKeyNotFound returns true if mapKey not found error.
func IsKeyNotFound(err error) bool {
	if err == nil {
		return false
	}

	aeroError, ok := err.(*as.AerospikeError)
	if !ok {
		err = errors.Unwrap(err)
		if err == nil {
			return false
		}
		if aeroError, ok = err.(*as.AerospikeError); !ok {
			return false
		}

	}
	return aeroError.ResultCode == types.KEY_NOT_FOUND_ERROR
}

type rangeBinFilter struct {
	name  string
	begin int
	end   int
}
