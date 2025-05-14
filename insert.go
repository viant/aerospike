package aerospike

import (
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	ainsert "github.com/viant/aerospike/insert"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"strings"
	"sync"
)

func (s *Statement) prepareInsert(sql string, c *connection) error {

	if c.insertCache == nil {
		parsed, err := sqlparser.ParseInsert(sql)
		if err != nil {
			return fmt.Errorf("prepareInsert parse: %w", err)
		}

		s.insert = &ainsert.Statement{Statement: parsed}
		s.setSet(sqlparser.Stringify(parsed.Target.X))
		return nil
	}

	var stmt *ainsert.Statement
	kind, key, placeholderCount := extractKindAndKey(sql)

	cachedStmt, isCached := c.insertCache.Get(key)

	if isCached {
		stmt = cachedStmt.(*ainsert.Statement)
	} else {
		parsed, err := sqlparser.ParseInsert(sql)
		if err != nil {
			return fmt.Errorf("prepareInsert parse failed: %w", err)
		}
		stmt = ainsert.NewStatement(parsed)
	}

	if kind == ainsert.ParameterizedValuesOnly {
		if isCached {
			stmt = stmt.CloneForValuesOnly(placeholderCount)
		} else {
			stmt.PrepareValuesOnly(placeholderCount)
		}
	}

	if !isCached {
		c.insertCache.Add(key, stmt)
	}

	s.insert = stmt
	s.setSet(sqlparser.Stringify(stmt.Target.X))
	return nil
}

func extractKindAndKey(sql string) (ainsert.Kind, string, int) {

	up := strings.ToUpper(sql)
	idx := strings.LastIndex(up, "VALUES")
	if idx < 0 {
		return "", sql, 0
	}

	valuesStartIdx := idx + len("VALUES")

	ending := "?)"
	valuesEndIdx := strings.LastIndex(sql, ending)
	if valuesEndIdx == -1 {
		return "", sql, 0
	}

	valuesEndIdx += len(ending)

	if !isPlaceholderList(sql[valuesStartIdx:valuesEndIdx]) {
		return "", sql, 0
	}

	placeholderCount := strings.Count(sql[valuesStartIdx:valuesEndIdx], "?")
	allPlaceholderCount := strings.Count(sql, "?")
	if placeholderCount != allPlaceholderCount {
		return "", sql, 0
	}

	// that’s intentional - one key per all placeholders numbers
	key := sql[:valuesStartIdx] + "#PLACEHOLDERS#" + sql[valuesEndIdx:]

	return ainsert.ParameterizedValuesOnly, key, placeholderCount
}

func isPlaceholderList(s string) bool {
	for _, r := range s {
		switch r {
		case '(', ')', ',', '?', ' ', '\t', '\n', '\r':
			// ok
		default:
			return false
		}
	}
	return true
}

func (s *Statement) handleMapLoad(args []driver.NamedValue) error {

	batchCount := s.insert.ValuesCnt() / len(s.insert.Columns)
	var groups = make(map[interface{}][]map[interface{}]map[interface{}]interface{})
	argIndex := 0

	for b := 0; b < batchCount; b++ {
		bins, err := s.populateInsertBins(args, &argIndex)
		if err != nil {
			return err
		}
		keyValue := s.getKey(s.mapper.pk, bins)
		group, ok := groups[keyValue]
		if !ok {
			group = make([]map[interface{}]map[interface{}]interface{}, 0)
			groups[keyValue] = group
		}
		mapKey := s.getKey(s.mapper.mapKey, bins)
		values := make(map[interface{}]interface{}, len(bins))
		for k, v := range bins {
			values[k] = v
		}

		assigned := false
		for _, records := range groups[keyValue] {
			if _, ok := records[mapKey]; !ok {
				records[mapKey] = values
				assigned = true
				break
			}
		}

		if !assigned {
			groups[keyValue] = append(group, map[interface{}]map[interface{}]interface{}{mapKey: values})
		}

	}

	isMerge := len(s.insert.OnDuplicateKeyUpdate) > 0
	if isMerge {
		return s.handleMapMerge(groups)
	}

	aSet, err := s.lookupSet()
	if err != nil {
		return err
	}

	for keyValue, groupSet := range groups {
		for _, group := range groupSet {
			key, err := as.NewKey(s.namespace, s.set, keyValue)
			if err != nil {
				return err
			}

			writePolicy := s.writePolicy(aSet, true)

			var ops []*as.Operation
			mapPolicy := as.DefaultMapPolicy()
			var values = make(map[interface{}]interface{}, len(group))
			if s.mapper.component != nil {
				if err := s.ensureMapOfSlice(group, key); err != nil {
					return err
				}
				for k, v := range group {
					indexLiteral, ok := v[s.mapper.arrayIndex.Column()]
					if !ok {
						return fmt.Errorf("unable to find list secondaryIndex")
					}
					idx, ok := indexLiteral.(int)
					if !ok {
						return fmt.Errorf("invalid list secondaryIndex: %v", indexLiteral)
					}
					componentValue, ok := v[s.mapper.component.Column()]
					key := as.CtxMapKey(as.NewValue(k))
					ops = append(ops, as.ListSetOp(s.collectionBin, idx, as.NewValue(componentValue), key))
				}

				if _, err = s.client.Operate(nil, key, ops...); err != nil {
					return err
				}
				continue
			} else {

				for k, v := range group {
					values[k] = v
				}
				ops = append(ops, as.MapPutItemsOp(mapPolicy, s.collectionBin, values))
			}
			if _, err = s.client.Operate(writePolicy, key, ops...); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Statement) ensureMapOfSlice(group map[interface{}]map[interface{}]interface{}, key *as.Key) error {
	aSet, err := s.lookupSet()
	if err != nil {
		return err
	}
	newEntryPolicy := as.NewMapPolicyWithFlags(as.MapOrder.UNORDERED, as.MapWriteFlagsCreateOnly|as.MapWriteFlagsNoFail)
	var ops []*as.Operation
	var values = map[interface{}]interface{}{}
	for mapKey, _ := range group {
		values[mapKey] = s.mapper.newSlice()
	}
	ops = append(ops, as.MapPutItemsOp(newEntryPolicy, s.collectionBin, values))
	writePolicy := s.writePolicy(aSet, true)

	writePolicy.SendKey = true
	if _, err := s.client.Operate(writePolicy, key, ops...); err != nil {
		return err
	}
	return nil
}

func (s *Statement) handleMapMerge(groups map[interface{}][]map[interface{}]map[interface{}]interface{}) error {
	addColumn, subColumn, err := s.identifyAddSubColumn()
	if s.cfg.concurrency <= 1 {
		for recKey := range groups {
			groupSet := groups[recKey]
			if e := s.mergeMaps(recKey, groupSet, addColumn, subColumn); e != nil {
				err = e
			}
		}
		return err
	}
	if err != nil {
		return err
	}
	wg := sync.WaitGroup{}
	var rateLimiter = make(chan bool, min(s.cfg.concurrency, len(groups)))
	for recKey := range groups {
		groupSet := groups[recKey]
		rateLimiter <- true
		wg.Add(1)
		key := recKey
		go func(recKey interface{}, groupSet []map[interface{}]map[interface{}]interface{}) {
			defer func() {
				wg.Done()
				<-rateLimiter
			}()
			if e := s.mergeMaps(recKey, groupSet, addColumn, subColumn); e != nil {
				err = e
			}
		}(key, groupSet)
	}
	wg.Wait()
	return err
}

func (s *Statement) mergeMaps(recKey interface{}, groupSet []map[interface{}]map[interface{}]interface{}, addColumn map[string]bool, subColumn map[string]bool) error {
	var err error
	key, err := as.NewKey(s.namespace, s.set, recKey)
	if err != nil {
		return err
	}

	var slice interface{}
	if s.mapper.component != nil {
		slice = s.mapper.newSlice()
	}

	for _, group := range groupSet {
		var ops []*as.Operation
		var createOp []*as.Operation

		for groupKey, bins := range group {
			mapKey := as.CtxMapKey(as.NewValue(groupKey))
			createOnly := as.NewMapPolicyWithFlags(as.MapOrder.UNORDERED, as.MapWriteFlagsCreateOnly|as.MapWriteFlagsNoFail)
			if s.mapper.component != nil {
				createOp = append(createOp, as.MapPutOp(createOnly, s.collectionBin, groupKey, slice))
			} else {
				createOp = append(createOp, as.MapPutOp(createOnly, s.collectionBin, groupKey, map[interface{}]interface{}{}))
			}

			if s.mapper.component != nil {
				key := as.CtxMapKey(as.NewValue(groupKey))
				arrayKey := bins[s.mapper.arrayIndex.Column()]
				idx := arrayKey.(int)
				componentValue, ok := bins[s.mapper.component.Column()]
				if !ok {
					return fmt.Errorf("unable to find component value")
				}

				if addColumn[s.mapper.component.Column()] {
					ops = append(ops, as.ListIncrementOp(s.collectionBin, idx, as.NewValue(componentValue), key))
				} else if subColumn[s.mapper.component.Column()] {
					switch actual := componentValue.(type) {
					case int64:
						componentValue = -actual
					case int:
						componentValue = -actual
					case float64:
						componentValue = -actual
					}
					ops = append(ops, as.ListIncrementOp(s.collectionBin, idx, as.NewValue(componentValue), key))
				} else {
					ops = append(ops, as.ListSetOp(s.collectionBin, idx, as.NewValue(componentValue), key))
				}

			} else {
				mapPolicy := as.DefaultMapPolicy()

				for col, value := range bins {
					column := col.(string)
					columnValue := as.NewStringValue(column)

					if addColumn[column] {
						createOp = append(createOp, as.MapPutOp(createOnly, s.collectionBin, columnValue, s.mapper.columnZeroValue(column), mapKey))
						ops = append(ops, as.MapIncrementOp(mapPolicy, s.collectionBin, columnValue, value, mapKey))
					} else if subColumn[column] {
						createOp = append(createOp, as.MapPutOp(createOnly, s.collectionBin, columnValue, s.mapper.columnZeroValue(column), mapKey))
						ops = append(ops, as.MapDecrementOp(mapPolicy, s.collectionBin, columnValue, value, mapKey))
					} else {
						ops = append(ops, as.MapPutOp(mapPolicy, s.collectionBin, columnValue, value, mapKey))
					}
				}

			}
		}
		aSet, err := s.lookupSet()
		if err != nil {
			return err
		}
		writePolicy := s.writePolicy(aSet, true)

		if _, err = s.client.Operate(writePolicy, key, createOp...); err != nil {
			return err
		}
		if _, err = s.client.Operate(writePolicy, key, ops...); err != nil {
			return err
		}
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
		operations[keyValue] = append(operations[keyValue], as.ListAppendOp(s.collectionBin, bins))
	}
	for keyValue, operations := range operations {
		key, err := as.NewKey(s.namespace, s.set, keyValue)
		if err != nil {
			return err
		}
		operations = append(operations, as.PutOp(as.NewBin(s.mapper.pk[0].Column(), keyValue)))
		writePolicy := s.writePolicy(aSet, true)

		ret, err := s.client.Operate(writePolicy, key, operations...)
		if err != nil {
			return err
		}
		rawSize, ok := ret.Bins[s.collectionBin]
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
		return fmt.Errorf("unable to find primary mapKey field")
	}
	isMerge := len(s.insert.OnDuplicateKeyUpdate) > 0
	batchCount := s.insert.ValuesCnt() / len(s.insert.Columns)
	mod := s.insert.ValuesCnt() % len(s.insert.Columns)
	if mod != 0 {
		return fmt.Errorf("invalid insert values count: %v, expected multiple of %v", s.insert.ValuesCnt(), len(s.insert.Columns))
	}
	if s.writeLimiter != nil {
		defer s.writeLimiter.release()
		s.writeLimiter.acquire()
	}

	s.affected = int64(batchCount)
	if isDryRun("insert") {
		return nil
	}
	//if batchCount > 1 { //TODO check impact on regular insert
	if s.collectionType.IsMap() {
		if len(s.mapper.mapKey) == 0 {
			return fmt.Errorf("unable to find map mapKey field")
		}
		return s.handleMapLoad(args)
	}
	//}

	if s.collectionType.IsArray() {
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
		writePolicy := s.writePolicy(aSet, true)

		if s.collectionBin != "" {
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
	mapKey := s.getKey(s.mapper.mapKey, bins)
	ops := []*as.Operation{
		as.MapPutOp(as.DefaultMapPolicy(), s.collectionBin, mapKey, bins),
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
		columnValue := s.insert.ValueAt(i)
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
