package aerospike

import (
	"context"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/x"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

func (s *Statement) prepareSelect(SQL string) error {
	var err error
	if s.query, err = sqlparser.ParseQuery(SQL); err != nil {
		return err
	}

	setName := sqlparser.Stringify(s.query.From.X)
	if rawExpr, ok := s.query.From.X.(*expr.Raw); ok {
		if innerQuery, ok := rawExpr.X.(*query.Select); ok {
			setName = sqlparser.Stringify(innerQuery.From.X)
			if s.query.Qualify == nil {
				s.query.Qualify = innerQuery.Qualify
			}
		}
	}
	s.setSet(setName)
	return s.registerMetaSets()
}

func (s *Statement) registerMetaSets() error {
	switch strings.ToLower(s.namespace) {
	case "information_schema":
		switch strings.ToLower(s.set) {
		case "schemata":
			s.sets.register(&set{
				xType:  x.NewType(reflect.TypeOf(catalog{}), x.WithName("schemata")),
				ttlSec: 0,
			})
		case "tables":
			s.sets.register(&set{
				xType:  x.NewType(reflect.TypeOf(tableInfo{}), x.WithName("tables")),
				ttlSec: 0,
			})
		case "columns":
			s.sets.register(&set{
				xType:  x.NewType(reflect.TypeOf(tableColumn{}), x.WithName("columns")),
				ttlSec: 0,
			})
		case "processlist":
			s.sets.register(&set{
				xType:  x.NewType(reflect.TypeOf(processList{}), x.WithName("processlist")),
				ttlSec: 0,
			})
		case "serverinfo":
			s.sets.register(&set{
				xType:  x.NewType(reflect.TypeOf(serverInfo{}), x.WithName("serverinfo")),
				ttlSec: 0,
			})
		default:
			return fmt.Errorf("unsupported InformationSchema: %v", s.set)
		}
	}
	return nil
}

func (s *Statement) executeSelect(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	aSet, err := s.lookupSet()
	if err != nil {
		return nil, err
	}
	err = s.setRecordType(aSet)
	if err != nil {
		return nil, err
	}

	aMapper := aSet.lookupQueryMapper(s.SQL)
	if aMapper == nil {
		aMapper, err = newQueryMapper(s.recordType, s.query.List, aSet.typeBasedMapper)
		if err != nil {
			return nil, err
		}
		aSet.registerQueryMapper(s.SQL, aMapper)
	}
	row := reflect.New(s.recordType).Interface()
	rows := &Rows{
		zeroRecord: unsafe.Slice((*byte)(xunsafe.AsPointer(row)), s.recordType.Size()),
		record:     row,
		recordType: s.recordType,
		mapper:     aMapper,
		query:      s.query,
		ctx:        ctx,
	}
	if err := s.updateCriteria(s.query.Qualify, args, true); err != nil {
		return nil, err
	}

	if s.falsePredicate {
		rows.rowsReader = newRowsReader([]*as.Record{})
		return rows, nil
	}

	bins := s.mapper.expandBins()

	if len(s.secondaryIndexValues) > 0 {
		stmt := as.NewStatement(s.namespace, s.set, bins...)
		if s.mapRangeFilter != nil {
			from := s.mapRangeFilter.begin
			to := s.mapRangeFilter.end
			// Set a filter to query the secondary secondaryIndex
			if err = stmt.SetFilter(as.NewRangeFilter(s.mapper.secondaryIndex.Name, int64(from), int64(to))); err != nil {
				return nil, err
			}
		} else {
			// Set a filter to query the secondary secondaryIndex
			if err = stmt.SetFilter(as.NewEqualFilter(s.mapper.secondaryIndex.Column(), s.secondaryIndexValues[0])); err != nil {
				return nil, err
			}
		}
		// Execute the query
		recordset, err := s.client.Query(nil, stmt)
		if err != nil {
			return nil, err
		}
		rows.rowsReader = &RowsScanReader{Recordset: recordset}
		return rows, nil
	}

	keys, err := s.buildKeys()
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(s.namespace) {
	case "information_schema":
		return s.handleInformationSchema(ctx, keys, rows)
	}

	switch len(keys) {
	case 0:
		if s.query.Qualify != nil {
			return nil, fmt.Errorf("executeselect: unsupported parameterizedQuery without mapKey")
			//use parameterizedQuery call
		} else {
			recordset, err := s.client.ScanAll(as.NewScanPolicy(), s.namespace, s.set)
			if err != nil {
				return nil, fmt.Errorf("executeselect: unable to scan set %s due to %w", s.set, err)
			}

			rows.rowsReader = &RowsScanReader{Recordset: recordset}
		}
	case 1:
		if s.collectionType.IsMap() {
			return s.handleMapQuery(keys, rows)
		}
		if s.collectionType.IsArray() {
			return s.handleListQuery(keys, rows)
		}

		var record *as.Record
		if s.query.List.IsStarExpr() {
			record, err = s.client.Get(as.NewPolicy(), keys[0])
		} else {
			record, err = s.client.Get(as.NewPolicy(), keys[0], bins...)
		}
		if err != nil {
			return handleNotFoundError(err, rows)
		}
		rows.rowsReader = newRowsReader([]*as.Record{record})
	default:
		records, err := s.client.BatchGet(as.NewBatchPolicy(), keys)
		recs := make([]*as.Record, 0)
		if s.collectionType.IsMap() {
			for i := range records {
				if records[i] != nil {
					e := s.handleMapBinResult(records[i], &recs)
					if e != nil {
						return nil, err
					}
				}
			}
			rows.rowsReader = newRowsReader(recs)
			return rows, nil

		} else if s.collectionType.IsArray() {
			for i := range records {
				if records[i] != nil {
					e := s.handleListBinResult(records[i], &recs, true)
					if e != nil {
						return nil, err
					}
				}
			}
			rows.rowsReader = newRowsReader(recs)
			return rows, nil
		} else {
			for i := range records {
				if records[i] != nil {
					recs = append(recs, records[i])
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

func (s *Statement) lookupSet() (*set, error) {
	aSet := s.sets.Lookup(s.source)
	if aSet == nil {
		aSet = s.sets.Lookup(s.set)
		if aSet == nil {
			return nil, fmt.Errorf("executeselect: unable to lookup set with name %s", s.set)
		}
	}
	return aSet, nil
}

func (s *Statement) handleMapQuery(keys []*as.Key, rows *Rows) (driver.Rows, error) {

	if s.mapper.component != nil {
		return s.handleMapListQuery(keys, rows)
	}

	var err error
	var op []*as.Operation
	if s.mapRangeFilter != nil || len(s.mapKeyValues) > 0 {
		switch {
		case s.mapRangeFilter != nil && len(s.mapKeyValues) > 0:
			return nil, fmt.Errorf("unsupported criteria combination: mapKey values list and mapKey range")
		case s.mapRangeFilter != nil:
			op = append(op, as.MapGetByKeyRangeOp(s.collectionBin, s.mapRangeFilter.begin, s.mapRangeFilter.end+1, as.MapReturnType.KEY_VALUE))
		case len(s.mapKeyValues) == 1:
			op = append(op, as.MapGetByKeyOp(s.collectionBin, s.mapKeyValues[0], as.MapReturnType.KEY_VALUE))
		case len(s.mapKeyValues) > 1:
			op = append(op, as.MapGetByKeyListOp(s.collectionBin, s.mapKeyValues, as.MapReturnType.KEY_VALUE))
		}
		result, err := s.client.Operate(nil, keys[0], op...)
		if err != nil {
			return handleNotFoundError(err, rows)
		}
		values, ok := result.Bins[s.collectionBin]
		if !ok {
			rows.rowsReader = newRowsReader([]*as.Record{})
			return rows, nil
		}
		pairs, ok := values.([]as.MapPair)
		if !ok {
			rows.rowsReader = newRowsReader([]*as.Record{})
			return rows, nil
		}
		recs, verr := s.convertMapPairsToRecords(pairs)
		if verr != nil {
			return nil, verr
		}
		rows.rowsReader = newRowsReader(recs)
		return rows, nil
	}
	record, err := s.client.Get(as.NewPolicy(), keys[0], s.mapper.pk[0].Column(), s.collectionBin)
	if err != nil {
		return handleNotFoundError(err, rows)
	}
	records := make([]*as.Record, 0)
	if err = s.handleMapBinResult(record, &records); err != nil {
		return nil, err
	}
	rows.rowsReader = newRowsReader(records)
	return rows, nil
}

func (s *Statement) handleMapListQuery(keys []*as.Key, rows *Rows) (driver.Rows, error) {
	var err error
	var op []*as.Operation
	if s.mapRangeFilter != nil || len(s.mapKeyValues) > 0 {
		switch {
		case s.mapRangeFilter != nil && len(s.mapKeyValues) > 0:
			return nil, fmt.Errorf("unsupported criteria combination: mapKey values list and mapKey range")
		case s.mapRangeFilter != nil:

			op = append(op, as.MapGetByKeyRangeOp(s.collectionBin, s.mapRangeFilter.begin, s.mapRangeFilter.end+1, as.MapReturnType.VALUE))
		case len(s.mapKeyValues) == 1:
			op = append(op, as.MapGetByKeyOp(s.collectionBin, s.mapKeyValues[0], as.MapReturnType.KEY_VALUE))
		case len(s.mapKeyValues) > 1:
			op = append(op, as.MapGetByKeyListOp(s.collectionBin, s.mapKeyValues, as.MapReturnType.KEY_VALUE))
		}
		result, err := s.client.Operate(nil, keys[0], op...)
		if err != nil {
			return handleNotFoundError(err, rows)
		}
		values, ok := result.Bins[s.collectionBin]
		if !ok {
			rows.rowsReader = newRowsReader([]*as.Record{})
			return rows, nil
		}
		pairs, ok := values.([]as.MapPair)
		if !ok {
			rows.rowsReader = newRowsReader([]*as.Record{})
			return rows, nil
		}
		recs, verr := s.convertMapSlicePairsToRecords(keys, pairs)
		if verr != nil {
			return nil, verr
		}
		rows.rowsReader = newRowsReader(recs)
		return rows, nil
	}
	record, err := s.client.Get(as.NewPolicy(), keys[0], s.mapper.pk[0].Column(), s.collectionBin)
	if err != nil {
		return handleNotFoundError(err, rows)
	}
	records := make([]*as.Record, 0)

	if err = s.handleBinMapArrayComponentResult(record, &records); err != nil {
		return nil, err
	}
	rows.rowsReader = newRowsReader(records)
	return rows, nil
}

func (s *Statement) convertMapSlicePairsToRecords(keys []*as.Key, pairs []as.MapPair) ([]*as.Record, error) {
	var records []*as.Record
	var whiteListedIndexes = map[int]bool{}
	if s.arrayIndexValues != nil {
		for _, key := range s.arrayIndexValues {
			whiteListedIndexes[key] = true
		}
	}

	for _, pair := range pairs {
		items, ok := pair.Value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("unsupported map component %v value type: %T", s.mapper.component.Column(), pair.Value)
		}
		for i, item := range items {
			if s.arrayRangeFilter != nil {
				if i < s.arrayRangeFilter.begin {
					continue
				}
				if i > s.arrayRangeFilter.end {
					break
				}
			} else if len(whiteListedIndexes) > 0 {
				if _, ok := whiteListedIndexes[i]; !ok {
					continue
				}
			}

			record := &as.Record{Bins: map[string]interface{}{}}
			record.Bins[s.mapper.mapKey[0].Column()] = pair.Key
			record.Bins[s.mapper.pk[0].Column()] = keys[0].Value()
			record.Bins[s.mapper.component.Column()] = item
			record.Bins[s.mapper.arrayIndex.Column()] = i
			records = append(records, record)
		}
	}
	return records, nil
}

func (s *Statement) convertMapPairsToRecords(pairs []as.MapPair) ([]*as.Record, error) {
	var records []*as.Record

	for _, pair := range pairs {
		items, ok := pair.Value.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("unable to convert map pairs to records - unsupported type: %T, expected: %T", pair.Value, items)
		}
		record := &as.Record{Bins: map[string]interface{}{}}
		for k, value := range items {
			key := k.(string)
			record.Bins[key] = value
		}

		records = append(records, record)
	}
	return records, nil
}

func handleNotFoundError(err error, rows *Rows) (driver.Rows, error) {
	if IsKeyNotFound(err) {
		rows.rowsReader = newRowsReader([]*as.Record{})
		return rows, nil
	}
	return nil, err
}

func (s *Statement) handleListQuery(keys []*as.Key, rows *Rows) (driver.Rows, error) {
	var err error
	var funcColumn string
	var op []*as.Operation
	if len(rows.mapper.aggregateColumn) > 0 { //for only one  aggregation func

		for col, call := range rows.mapper.aggregateColumn {
			if funcColumn != "" {
				return nil, fmt.Errorf("unsupported multiple aggregation functions: %s", sqlparser.Stringify(call))
			}
			fName := sqlparser.Stringify(call.X)
			switch strings.ToLower(fName) {
			case "count":
				op = append(op, as.ListSizeOp(s.collectionBin))
				funcColumn = col
			default:
				return nil, fmt.Errorf("unsupported aggregation function: %s", fName)
			}
			funcColumn = col
		}
	}

	if len(s.arrayIndexValues) > 0 {
		switch {
		case s.arrayRangeFilter != nil && len(s.arrayIndexValues) > 0:
			return nil, fmt.Errorf("unsupported criteria combination: mapKey rawValues list and mapKey range")
		case s.arrayRangeFilter != nil:
			return nil, fmt.Errorf("unsupported criteria combination: mapKey rawValues list and mapKey range")
		case len(s.arrayIndexValues) == 1:
			op = append(op, as.ListGetOp(s.collectionBin, s.arrayIndexValues[0]))
		case len(s.arrayIndexValues) > 1:
			for j := range s.arrayIndexValues {
				op = append(op, as.ListGetOp(s.collectionBin, s.arrayIndexValues[j]))
			}
		}

		result, err := s.client.Operate(nil, keys[0], op...)
		if err != nil {
			return handleNotFoundError(err, rows)
		}

		rawValues, ok := result.Bins[s.collectionBin]
		if funcColumn != "" {
			rows.rowsReader = newRowsReader([]*as.Record{{Bins: map[string]interface{}{funcColumn: rawValues}}})
			return rows, nil
		}
		var values []interface{}
		if ok {
			values = rawValues.([]interface{})
		}
		var records []*as.Record
		for j, value := range values {
			if value == nil {
				continue
			}
			properties := value.(map[interface{}]interface{})
			aRecord := &as.Record{Bins: map[string]interface{}{}}
			for k, v := range properties {
				aRecord.Bins[k.(string)] = v
			}
			aRecord.Bins[s.mapper.mapKey[0].Column()] = s.arrayIndexValues[j]
			aRecord.Bins[s.mapper.pk[0].Column()] = s.pkValues[0]
			records = append(records, aRecord)
		}
		//THIS IS BROKEN fix me !!!
		rows.rowsReader = newRowsReader(records)
		return rows, nil
	}

	record, err := s.client.Get(as.NewPolicy(), keys[0], s.collectionBin, s.mapper.pk[0].Column())
	if err != nil {
		return handleNotFoundError(err, rows)
	}
	records := make([]*as.Record, 0)
	if err = s.handleListBinResult(record, &records, true); err != nil {
		return nil, err
	}
	rows.rowsReader = newRowsReader(records)
	return rows, nil
}

func (s *Statement) handleListBinResult(record *as.Record, records *[]*as.Record, applyFilter bool) error {
	if mapBin, ok := record.Bins[s.collectionBin]; ok {
		listBinSlice, ok := mapBin.([]interface{})
		if !ok {
			return fmt.Errorf("invalid map bin value: %v", mapBin)
		}
		var filter = map[interface{}]bool{}
		if len(s.arrayIndexValues) > 0 && applyFilter {
			for _, key := range s.arrayIndexValues {
				filter[key] = true
			}
		}
		for index, v := range listBinSlice {
			if len(filter) > 0 {
				if _, ok := filter[index]; !ok {

				}
			}
			if s.arrayRangeFilter != nil && applyFilter {
				if index < s.arrayRangeFilter.begin || index > s.arrayRangeFilter.end {
					continue
				}
			}
			var itemRecord = &as.Record{Bins: map[string]interface{}{}}
			properties := v.(map[interface{}]interface{})
			for k, v := range properties {
				itemRecord.Bins[k.(string)] = v
			}
			if _, ok := itemRecord.Bins[s.mapper.arrayIndex.Column()]; !ok {
				itemRecord.Bins[s.mapper.arrayIndex.Column()] = index
			}
			itemRecord.Bins[s.mapper.pk[0].Column()] = record.Key.Value()
			*records = append(*records, itemRecord)
		}
	}
	return nil
}

func (s *Statement) handleMapBinResult(record *as.Record, records *[]*as.Record) error {
	if mapBin, ok := record.Bins[s.collectionBin]; ok {
		mapBinMap, ok := mapBin.(map[interface{}]interface{})
		if !ok {
			return fmt.Errorf("invalid map bin value: %v", mapBin)
		}
		var filter = map[interface{}]bool{}
		if len(s.mapKeyValues) > 0 {
			for _, key := range s.mapKeyValues {
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
			if s.mapRangeFilter != nil {
				mapBinKeyInt, ok := mapKey.(int)
				if !ok {
					return fmt.Errorf("unsupported type for between operator - got: %T expected %T", mapKey, mapBinKeyInt)
				}
				if mapBinKeyInt < s.mapRangeFilter.begin || mapBinKeyInt > s.mapRangeFilter.end {
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

func (s *Statement) handleBinMapArrayComponentResult(record *as.Record, records *[]*as.Record) error {
	if mapBin, ok := record.Bins[s.collectionBin]; ok {
		mapBinMap, ok := mapBin.(map[interface{}]interface{})
		if !ok {
			return fmt.Errorf("invalid map bin value: %v", mapBin)
		}
		var filter = map[interface{}]bool{}
		if len(s.mapKeyValues) > 0 {
			for _, key := range s.mapKeyValues {
				filter[key] = true
			}
		}
		mapKeyName := s.mapper.mapKey[0].Column()
		pkName := s.mapper.pk[0].Column()
		componentName := s.mapper.component.Column()
		listIndexName := s.mapper.arrayIndex.Column()
		for mapKey, mapValue := range mapBinMap {
			entry, ok := mapValue.([]interface{})
			if !ok {
				return fmt.Errorf("invalid map bin entry value: %v", mapValue)
			}
			for idx, value := range entry {
				var rec = &as.Record{Bins: map[string]interface{}{}}
				rec.Bins[pkName] = record.Key.Value()
				rec.Bins[mapKeyName] = mapKey
				rec.Bins[listIndexName] = idx
				rec.Bins[componentName] = value
				*records = append(*records, rec)
			}
		}

	}
	return nil
}
