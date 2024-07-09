package sql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

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
	SQL          string
	kind         sqlparser.Kind
	types        *x.Registry
	query        *query.Select
	insert       *insert.Statement
	update       *update.Statement
	delete       *delete.Statement
	mapper       *mapper
	filter       *as.Filter
	mapBinFilter *MapBinFilter
	recordType   reflect.Type
	record       interface{}
	numInput     int
	set          string
	mapBin       string
	namespace    string
	pkValues     []interface{}
	keyValues    []interface{}
}

// Exec executes statements
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("not supported")
}

// ExecContext executes statements
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	switch s.kind {
	case sqlparser.KindRegisterSet:
		return s.handleRegisterSet(args)
	case sqlparser.KindInsert:
		return nil, s.handleInsert(args)
	case sqlparser.KindUpdate:
		return nil, s.handleUpdate(args)
	case sqlparser.KindDelete:
		return nil, s.handleDelete(args)
	case sqlparser.KindMerge:

	case sqlparser.KindSelect:
		return nil, fmt.Errorf("unsupported query type: %v", s.kind)
	}
	return nil, nil //TODO error - unsupported kind?
}

// Query runs query
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("not supported")
}

// QueryContext runs query
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	switch s.kind {
	case sqlparser.KindSelect:
	default:
		return nil, fmt.Errorf("unsupported query type: %v", s.kind)
	}
	return s.executeSelect(ctx, args)
}

func (s *Statement) setSet(set string) {
	s.set = set
	if index := strings.Index(set, "."); index != -1 {
		s.namespace = set[:index]
		s.set = set[index+1:]
		set = s.set
	}
	if index := strings.Index(set, "$"); index != -1 {
		s.mapBin = set[index+1:]
		s.set = set[:index]
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
	s.setSet(sqlparser.Stringify(s.query.From.X))

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

func (s *Statement) executeSelect(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	//var err error
	if aType := s.types.Lookup(s.set); aType != nil {
		s.recordType = aType.Type
	} else {
		return nil, fmt.Errorf("executeselect: unable to lookup type with name %s", s.set)
	}
	aMapper, err := newQueryMapper(s.recordType, s.query.List) //TODO add s.mapper as parameter, don't create mapper again
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
	if err := s.updateCriteria(s.query.Qualify, args, true); err != nil {
		return nil, err
	}
	keys, err := s.buildKeys()
	if err != nil {
		return nil, err
	}
	switch len(keys) {
	case 0:
		if s.query.Qualify != nil {
			return nil, fmt.Errorf("executeselect: unsupported query without key")
			//use query call
		} else {
			recordset, err := s.client.ScanAll(as.NewScanPolicy(), s.namespace, s.set)
			if err != nil {
				return nil, fmt.Errorf("executeselect: unable to scan set %s due to %w", s.set, err)
			}

			rows.rowsReader = &RowsScanReader{Recordset: recordset}

		}
	case 1:
		var record *as.Record
		bins := make([]string, len(aMapper.fields))
		for i, field := range aMapper.fields {
			bins[i] = field.Column()
		}

		if s.mapBin != "" && (s.mapBinFilter != nil || len(s.keyValues) > 0) {
			var op *as.Operation
			var result *as.Record

			switch {
			case s.mapBinFilter != nil && len(s.keyValues) > 0:
				return nil, fmt.Errorf("unsupported criteria combination: key values list and key range")
			case s.mapBinFilter != nil:
				op = as.MapGetByKeyRangeOp(s.mapBin, s.mapBinFilter.begin, s.mapBinFilter.end+1, as.MapReturnType.VALUE)
			case len(s.keyValues) == 1:
				op = as.MapGetByKeyOp(s.mapBin, s.keyValues[0], as.MapReturnType.VALUE)
			case len(s.keyValues) > 1:
				op = as.MapGetByKeyListOp(s.mapBin, s.keyValues, as.MapReturnType.VALUE)
			}

			result, err = s.client.Operate(nil, keys[0], op)
			if err != nil {
				if IsKeyNotFound(err) {
					rows.rowsReader = newRowsReader([]*as.Record{})
					return rows, nil
				}
				return nil, err
			}

			values, _ := result.Bins[s.mapBin]
			var records []interface{}
			if values != nil {
				switch {
				case len(s.keyValues) == 1:
					records = []interface{}{values}
				default:
					records, _ = values.([]interface{})
				}

			}
			rows.rowsReader = newInterfaceReader(records)
			return rows, nil
		}

		if s.mapBin != "" {
			bins = append(bins, s.mapBin)
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

		if s.mapBin != "" {
			records := make([]*as.Record, 0)
			if err = s.handleBinResult(record, &records); err != nil {
				return nil, err
			}
			rows.rowsReader = newRowsReader(records)
			return rows, nil
		}

		rows.rowsReader = newRowsReader([]*as.Record{record})
	default:
		// TODO user BatchGetOperate
		// func (clnt *Client) BatchGetOperate(policy *BatchPolicy, keys []*Key, ops ...*Operation) ([]*Record, Error) {
		records, err := s.client.BatchGet(as.NewBatchPolicy(), keys)
		recs := make([]*as.Record, 0)
		switch s.mapBin {
		case "":
			for i := range records {
				if records[i] != nil {
					recs = append(recs, records[i])
				}
			}
		default:
			for i := range records {
				if records[i] != nil {
					e := s.handleBinResult(records[i], &recs)
					if e != nil {
						return nil, err
					}
				}
			}
		}

		rows.rowsReader = newRowsReader(recs)

		if err != nil {
			if IsKeyNotFound(err) {
				return rows, nil
			}
			return nil, err
		}
	}
	return rows, nil
}

func (s *Statement) handleBinResult(record *as.Record, records *[]*as.Record) error {
	if mapBin, ok := record.Bins[s.mapBin]; ok {
		mapBinMap, ok := mapBin.(map[interface{}]interface{})
		if !ok {
			return fmt.Errorf("invalid map bin value: %v", mapBin)
		}
		var filter = map[interface{}]bool{}
		if len(s.keyValues) > 0 {
			for _, key := range s.keyValues {
				filter[key] = true
			}
		}
		for mapKey, v := range mapBinMap {
			if len(filter) > 0 {
				if _, ok := filter[mapKey]; !ok {
					continue
				}
			}

			// TODO add support for BatchGet and/or ScanAll -> BatchGetOperate
			if s.mapBinFilter != nil {
				mapBinKeyInt, ok := mapKey.(int)
				if !ok {
					return fmt.Errorf("unsupported type for between operator - got: %T expected %T", mapKey, mapBinKeyInt)
				}
				if mapBinKeyInt < s.mapBinFilter.begin || mapBinKeyInt > s.mapBinFilter.end {
					continue
				}
			}

			entry, ok := v.(map[interface{}]interface{})
			if !ok {
				return fmt.Errorf("invalid map bin entry value: %v", v)
			}
			var record = &as.Record{Bins: map[string]interface{}{}}
			for k, value := range entry {
				key, _ := k.(string)
				record.Bins[key] = value
			}
			*records = append(*records, record)
		}
	}
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
	if s.mapper != nil && len(s.mapper.pk) > 1 {
		pkName = s.mapper.pk[0].Column()
	}
	keyName := "--"
	if s.mapper != nil && len(s.mapper.key) > 1 {
		keyName = s.mapper.key[0].Column()
	}
	isMultiInPk := len(s.mapper.pk) > 1
	isMultiInKey := len(s.mapper.key) > 1

	idx := 0
	err := binary.Walk(func(ident node.Node, values *expr.Values, operator, parentOperator string) error {
		if parentOperator != "" && strings.ToUpper(parentOperator) != "AND" {
			return fmt.Errorf("unuspported logical operator: %s", parentOperator)
		}
		values.Idx = idx
		name := strings.ToLower(sqlparser.Stringify(ident))
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
		switch name {
		case "pk", pkName:
			s.pkValues = exprValues
		case "key", keyName:

			switch strings.ToLower(operator) {
			case "=", "in":
				s.keyValues = exprValues
			case "between":
				if len(exprValues) != 2 {
					return fmt.Errorf("invalid criteria - between expects 2 values")
				}

				from, ok := exprValues[0].(int)
				if !ok {
					return fmt.Errorf("unable to get int value from criteria value %v", exprValues[0])
				}

				to, ok := exprValues[1].(int)
				if !ok {
					return fmt.Errorf("unable to get int value from criteria value %v", exprValues[1])
				}

				s.mapBinFilter = &MapBinFilter{
					name:  name,
					begin: from,
					end:   to,
				}
				//Filter add range operator
			//case "like":
			default:
				return fmt.Errorf("unsupported operator of a mapbin key: %s", operator)
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
			case "like":
				//contain
			case "=":
				//equal criteria
			}
			//you may still use aerospike query with index
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
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

func (s *Statement) updateSetMapper() error {
	var err error
	if s.set == "" {
		return nil
	}
	if aType := s.types.Lookup(s.set); aType != nil {
		s.recordType = aType.Type
		s.record = reflect.New(s.recordType).Interface()
	} else {
		return fmt.Errorf("executeselect: unable to lookup type with name %s", s.set)
	}
	if s.recordType != nil {
		if s.mapper, err = newTypeBaseMapper(s.recordType); err != nil {
			return err
		}
	}
	return err
}

func (s *Statement) handleDelete(args []driver.NamedValue) error {
	if s.delete.Qualify == nil {
		return s.client.Truncate(nil, s.namespace, s.set, nil)
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

// IsKeyNotFound returns true if key not found error.
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

type MapBinFilter struct {
	name  string
	begin int
	end   int
}
