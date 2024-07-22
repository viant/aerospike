package aerospike

import (
	"context"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/viant/sqlparser"
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

	s.setSet(sqlparser.Stringify(s.query.From.X))
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
				xType:  x.NewType(reflect.TypeOf(table{}), x.WithName("tables")),
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
		default:
			return fmt.Errorf("unsupported InformationSchema: %v", s.set)
		}
	}
	return nil
}

func (s *Statement) executeSelect(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	aSet := s.sets.Lookup(s.set)
	if aSet == nil {
		return nil, fmt.Errorf("executeselect: unable to lookup set with name %s", s.set)
	}

	err := s.setRecordType(aSet)
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
	}
	if err := s.updateCriteria(s.query.Qualify, args, true); err != nil {
		return nil, err
	}
	keys, err := s.buildKeys()
	if err != nil {
		return nil, err
	}
	bins := s.mapper.expandBins()
	switch strings.ToLower(s.namespace) {
	case "information_schema":
		return s.handleInformationSchema(ctx, keys, rows)
	}

	switch len(keys) {
	case 0:
		if s.query.Qualify != nil {
			return nil, fmt.Errorf("executeselect: unsupported parameterizedQuery without key")
			//use parameterizedQuery call
		} else {
			recordset, err := s.client.ScanAll(as.NewScanPolicy(), s.namespace, s.set)
			if err != nil {
				return nil, fmt.Errorf("executeselect: unable to scan set %s due to %w", s.set, err)
			}

			rows.rowsReader = &RowsScanReader{Recordset: recordset}
		}
	case 1:
		if s.mapBin != "" {
			return s.handleMapQuery(keys, rows)
		}
		if s.listBin != "" {
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
		if s.mapBin != "" {
			for i := range records {
				if records[i] != nil {
					e := s.handleBinResult(records[i], &recs)
					if e != nil {
						return nil, err
					}
				}
			}
			rows.rowsReader = newRowsReader(recs)
			return rows, nil

		} else if s.listBin != "" {
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

func (s *Statement) handleMapQuery(keys []*as.Key, rows *Rows) (driver.Rows, error) {
	var err error
	var op []*as.Operation
	if s.rangeFilter != nil || len(s.keyValues) > 0 {
		switch {
		case s.rangeFilter != nil && len(s.keyValues) > 0:
			return nil, fmt.Errorf("unsupported criteria combination: key values list and key range")
		case s.rangeFilter != nil:
			op = append(op, as.MapGetByKeyRangeOp(s.mapBin, s.rangeFilter.begin, s.rangeFilter.end+1, as.MapReturnType.VALUE))
		case len(s.keyValues) == 1:
			op = append(op, as.MapGetByKeyOp(s.mapBin, s.keyValues[0], as.MapReturnType.VALUE))
		case len(s.keyValues) > 1:
			op = append(op, as.MapGetByKeyListOp(s.mapBin, s.keyValues, as.MapReturnType.VALUE))
		}
		result, err := s.client.Operate(nil, keys[0], op...)
		if err != nil {
			return handleNotFoundError(err, rows)
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
	}
	record, err := s.client.Get(as.NewPolicy(), keys[0], s.mapper.pk[0].Column(), s.mapBin)
	if err != nil {
		return handleNotFoundError(err, rows)
	}
	records := make([]*as.Record, 0)
	if err = s.handleBinResult(record, &records); err != nil {
		return nil, err
	}
	rows.rowsReader = newRowsReader(records)
	return rows, nil
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
				op = append(op, as.ListSizeOp(s.listBin))
				funcColumn = col
			default:
				return nil, fmt.Errorf("unsupported aggregation function: %s", fName)
			}
			funcColumn = col
		}
	}

	if len(s.keyValues) > 0 {
		switch {
		case s.rangeFilter != nil && len(s.keyValues) > 0:
			return nil, fmt.Errorf("unsupported criteria combination: key rawValues list and key range")
		case s.rangeFilter != nil:
			return nil, fmt.Errorf("unsupported criteria combination: key rawValues list and key range")
		case len(s.keyValues) == 1:
			op = append(op, as.ListGetOp(s.listBin, s.keyValues[0].(int)))
		case len(s.keyValues) > 1:
			for j := range s.keyValues {
				op = append(op, as.ListGetOp(s.listBin, s.keyValues[j].(int)))
			}
		}
	}
	if len(op) > 0 {
		result, err := s.client.Operate(nil, keys[0], op...)
		if err != nil {
			if IsKeyNotFound(err) {
				rows.rowsReader = newRowsReader([]*as.Record{})
				return rows, nil
			}
			return nil, err
		}

		rawValues, ok := result.Bins[s.listBin]
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
			aRecord.Bins[s.mapper.key[0].Column()] = s.keyValues[j]
			aRecord.Bins[s.mapper.pk[0].Column()] = s.pkValues[0]
			records = append(records, aRecord)
		}
		rows.rowsReader = newRowsReader(records)
		return rows, nil
	}

	record, err := s.client.Get(as.NewPolicy(), keys[0], s.listBin, s.mapper.pk[0].Column())
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
	if mapBin, ok := record.Bins[s.listBin]; ok {
		listBinSlice, ok := mapBin.([]interface{})
		if !ok {
			return fmt.Errorf("invalid map bin value: %v", mapBin)
		}
		var filter = map[interface{}]bool{}
		if len(s.keyValues) > 0 && applyFilter {
			for _, key := range s.keyValues {
				filter[key] = true
			}
		}
		for index, v := range listBinSlice {
			if len(filter) > 0 {
				if _, ok := filter[index]; !ok {

				}
			}
			if s.rangeFilter != nil && applyFilter {
				if index < s.rangeFilter.begin || index > s.rangeFilter.end {
					continue
				}
			}
			var itemRecord = &as.Record{Bins: map[string]interface{}{}}
			properties := v.(map[interface{}]interface{})
			for k, v := range properties {
				itemRecord.Bins[k.(string)] = v
			}
			if _, ok := itemRecord.Bins[s.mapper.key[0].Column()]; !ok {
				itemRecord.Bins[s.mapper.key[0].Column()] = index
			}
			itemRecord.Bins[s.mapper.pk[0].Column()] = record.Key.Value()
			*records = append(*records, itemRecord)
		}
	}
	return nil
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
			if s.rangeFilter != nil {
				mapBinKeyInt, ok := mapKey.(int)
				if !ok {
					return fmt.Errorf("unsupported type for between operator - got: %T expected %T", mapKey, mapBinKeyInt)
				}
				if mapBinKeyInt < s.rangeFilter.begin || mapBinKeyInt > s.rangeFilter.end {
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
