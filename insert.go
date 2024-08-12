package aerospike

import (
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"sync"
)

func (s *Statement) prepareInsert(sql string) error {
	var err error
	if s.insert, err = sqlparser.ParseInsert(sql); err != nil {
		return err
	}
	s.setSet(sqlparser.Stringify(s.insert.Target.X))
	return nil
}

func (s *Statement) handleMapLoad(args []driver.NamedValue) error {
	batchCount := len(s.insert.Values) / len(s.insert.Columns)
	var groups = make(map[interface{}]map[interface{}]map[interface{}]interface{})
	argIndex := 0

	for b := 0; b < batchCount; b++ {
		bins, err := s.populateInsertBins(args, &argIndex)
		if err != nil {
			return err
		}
		keyValue := s.getKey(s.mapper.pk, bins)
		group, ok := groups[keyValue]
		if !ok {
			group = make(map[interface{}]map[interface{}]interface{})
			groups[keyValue] = group
		}
		mapKey := s.getKey(s.mapper.key, bins)
		values := make(map[interface{}]interface{}, len(bins))
		for k, v := range bins {
			values[k] = v
		}
		groups[keyValue][mapKey] = values
	}

	isMerge := len(s.insert.OnDuplicateKeyUpdate) > 0
	if isMerge {
		return s.handleMapMerge(groups)
	}

	aSet, err := s.lookupSet()
	if err != nil {
		return err
	}

	for keyValue, group := range groups {
		key, err := as.NewKey(s.namespace, s.set, keyValue)
		if err != nil {
			return err
		}
		writePolicy := as.NewWritePolicy(0, aSet.ttlSec)
		writePolicy.SendKey = true
		var ops []*as.Operation
		var values = make(map[interface{}]interface{}, len(group))
		for k, v := range group {
			values[k] = v
		}
		mapPolicy := as.DefaultMapPolicy()
		ops = append(ops, as.MapPutItemsOp(mapPolicy, s.mapBin, values))
		if _, err = s.client.Operate(writePolicy, key, ops...); err != nil {
			return err
		}

	}
	return nil
}

func (s *Statement) handleMapMerge(groups map[interface{}]map[interface{}]map[interface{}]interface{}) error {
	addColumn, subColumn, err := s.identifyAddSubColumn()
	if err != nil {
		return err
	}
	wg := sync.WaitGroup{}
	var rateLimiter = make(chan bool, min(s.cfg.concurrency, len(groups)))
	for recKey := range groups {
		group := groups[recKey]
		rateLimiter <- true
		wg.Add(1)
		key := recKey
		go func(recKey interface{}, group map[interface{}]map[interface{}]interface{}) {
			defer func() {
				wg.Done()
				<-rateLimiter
			}()
			if e := s.mergeMaps(recKey, group, addColumn, subColumn); e != nil {
				err = e
			}
		}(key, group)
	}
	wg.Wait()
	return err
}

func (s *Statement) mergeMaps(recKey interface{}, group map[interface{}]map[interface{}]interface{}, addColumn map[string]bool, subColumn map[string]bool) error {
	var err error
	key, err := as.NewKey(s.namespace, s.set, recKey)
	if err != nil {
		return err
	}
	var op []*as.Operation
	var createOp []*as.Operation
	for groupKey, bins := range group {
		mapKey := as.CtxMapKey(as.NewValue(groupKey))
		mapPolicy := as.DefaultMapPolicy()
		createOnly := as.NewMapPolicy(as.MapOrder.UNORDERED, as.MapWriteMode.CREATE_ONLY)
		createOp = append(createOp, as.MapPutOp(createOnly, s.mapBin, groupKey, map[interface{}]interface{}{}))
		for col, value := range bins {
			column := col.(string)
			columnValue := as.NewStringValue(column)
			if addColumn[column] {
				createOp = append(createOp, as.MapPutOp(createOnly, s.mapBin, columnValue, 0, mapKey))
				op = append(op, as.MapIncrementOp(mapPolicy, s.mapBin, columnValue, value, mapKey))
			} else if subColumn[column] {
				createOp = append(createOp, as.MapPutOp(createOnly, s.mapBin, columnValue, 0, mapKey))
				op = append(op, as.MapDecrementOp(mapPolicy, s.mapBin, columnValue, value, mapKey))
			} else {
				op = append(op, as.MapPutOp(mapPolicy, s.mapBin, columnValue, value, mapKey))
			}
		}
	}

	aSet, err := s.lookupSet()
	if err != nil {
		return err
	}

	writePolicy := as.NewWritePolicy(0, aSet.ttlSec)
	writePolicy.SendKey = true

	_, _ = s.client.Operate(writePolicy, key, createOp...)
	if _, err := s.client.Operate(writePolicy, key, op...); err != nil {
		return err
	}
	return nil
}

func (s *Statement) identifyAddSubColumn() (map[string]bool, map[string]bool, error) {
	var addColumn = map[string]bool{}
	var subColumn = map[string]bool{}
	for _, item := range s.insert.OnDuplicateKeyUpdate {
		column := sqlparser.Stringify(item.Column)
		aField := s.mapper.getField(column)
		if aField == nil {
			return nil, nil, fmt.Errorf("unable to find field %v in type %T", column, s.recordType)
		}
		if item.IsExpr() {
			binary := item.Expr.(*expr.Binary)
			switch binary.Op {
			case "+", "-":
				ident := binary.Ident()
				sel := binary.SelectorIdent(s.insert.Alias)
				if ident == nil || sel == nil {
					return nil, nil, fmt.Errorf("invalid binary expression: %v", item.Column)
				}
				if ident.Name != sel.Name {
					return nil, nil, fmt.Errorf("invalid expr %v, column has to be the same %v", sqlparser.Stringify(binary), item.Column)
				}
				if binary.Op == "+" {
					addColumn[aField.Column()] = true
				} else if binary.Op == "-" {
					subColumn[aField.Column()] = true
				}
			default:
				return nil, nil, fmt.Errorf("unsupported update column operator: %s, supported(+,-)", binary.Op)
			}
		}
	}
	return addColumn, subColumn, nil
}

func (s *Statement) handleListInsert(args []driver.NamedValue, itemCount int) error {
	var argIndex int
	var operations = map[interface{}][]*as.Operation{}

	aSet, err := s.lookupSet()
	if err != nil {
		return err
	}
	for i := 0; i < itemCount; i++ {
		bins, err := s.populateInsertBins(args, &argIndex)
		if err != nil {
			return err
		}
		keyValue := s.getKey(s.mapper.pk, bins)
		delete(bins, s.mapper.pk[0].Column())
		operations[keyValue] = append(operations[keyValue], as.ListAppendOp(s.listBin, bins))
	}
	for keyValue, operations := range operations {
		key, err := as.NewKey(s.namespace, s.set, keyValue)
		if err != nil {
			return err
		}
		operations = append(operations, as.PutOp(as.NewBin(s.mapper.pk[0].Column(), keyValue)))
		writePolicy := as.NewWritePolicy(0, aSet.ttlSec)
		writePolicy.SendKey = true
		ret, err := s.client.Operate(writePolicy, key, operations...)
		if err != nil {
			return err
		}
		rawSize, ok := ret.Bins[s.listBin]
		if !ok {
			return fmt.Errorf("failed to insert list value")
		}
		switch actual := rawSize.(type) {
		case int:
			lastInsertID := int64(actual - 1)
			s.lastInsertID = &lastInsertID
		case []interface{}:
			lastInsertID := int64(actual[len(actual)-1].(int))
			s.lastInsertID = &lastInsertID
		}
	}
	return nil
}

func (s *Statement) handleInsert(args []driver.NamedValue) error {
	if s.insert == nil {
		return fmt.Errorf("insert statement is not initialized")
	}

	if len(s.mapper.pk) == 0 {
		return fmt.Errorf("unable to find primary key field")
	}
	isMerge := len(s.insert.OnDuplicateKeyUpdate) > 0

	batchCount := len(s.insert.Values) / len(s.insert.Columns)
	s.affected = int64(batchCount)
	//if batchCount > 1 { //TODO check impact on regular insert
	if s.mapBin != "" {
		if len(s.mapper.key) == 0 {
			return fmt.Errorf("unable to find map key field")
		}
		return s.handleMapLoad(args)
	}
	//}

	if s.listBin != "" {
		return s.handleListInsert(args, batchCount)
	}

	aSet, err := s.lookupSet()
	if err != nil {
		return err
	}
	argIndex := 0
	for b := 0; b < batchCount; b++ {
		bins, err := s.populateInsertBins(args, &argIndex)
		if err != nil {
			return err
		}
		keyValue := s.getKey(s.mapper.pk, bins)
		key, err := as.NewKey(s.namespace, s.set, keyValue)
		if err != nil {
			return err
		}
		writePolicy := as.NewWritePolicy(0, aSet.ttlSec)
		writePolicy.SendKey = true

		if s.mapBin != "" {
			return s.handleMapInsert(bins, err, writePolicy, key)
		}
		if isMerge {
			if err := s.handleMerge(bins, writePolicy, key); err != nil {
				return err
			}
			continue
		}
		writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		if err = s.client.Put(writePolicy, key, bins); err != nil {
			return err
		}
	}
	return nil
}

func (s *Statement) handleMapInsert(bins map[string]interface{}, err error, writePolicy *as.WritePolicy, key *as.Key) error {
	mapKey := s.getKey(s.mapper.key, bins)
	ops := []*as.Operation{
		as.MapPutOp(as.DefaultMapPolicy(), s.mapBin, mapKey, bins),
	}
	_, err = s.client.Operate(writePolicy, key, ops...)
	return err
}

func (s *Statement) handleMerge(bins map[string]interface{}, writePolicy *as.WritePolicy, key *as.Key) error {
	addColumn, subColumn, err := s.identifyAddSubColumn()
	if err != nil {
		return err
	}
	var ops []*as.Operation
	for column, value := range bins {
		if addColumn[column] {
			ops = append(ops, as.AddOp(as.NewBin(column, value)))
		} else if subColumn[column] {
			ops = append(ops, as.AddOp(as.NewBin(column, negate(value))))
		} else {
			ops = append(ops, as.PutOp(as.NewBin(column, value)))
		}
	}
	_, err = s.client.Operate(writePolicy, key, ops...)
	return err
}

func (s *Statement) populateInsertBins(args []driver.NamedValue, argIndex *int) (map[string]interface{}, error) {
	bins := make(map[string]interface{})
	for i, column := range s.insert.Columns {
		aField := s.mapper.getField(column)
		if aField == nil {
			return nil, fmt.Errorf("unable to find field %v in type %T", column, s.recordType)
		}
		columnValue := s.insert.Values[i]
		var value interface{}
		if columnValue.IsPlaceholder() {
			value = args[*argIndex].Value
			*argIndex++
		} else {
			val, err := columnValue.Value()
			if err != nil {
				return nil, err
			}
			value = val.Value

		}
		value, err := aField.ensureValidValueType(value)
		if err != nil {
			return nil, err
		}
		bins[aField.Column()] = value
	}
	return bins, nil
}
