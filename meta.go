package aerospike

import (
	"context"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"reflect"
	"strings"
)

type catalog struct {
	SchemaName string `sqlx:"schema_name" aerospike:"schema_name,pk=true"`
}

type table struct {
	TableSchema string `sqlx:"table_schema" aerospike:"table_schema"`
	TableName   string `sqlx:"table_name" aerospike:"table_name,pk=true"`
}

type tableColumn struct {
	TableSchema            string `sqlx:"table_schema" aerospike:"table_schema"`
	TableName              string `sqlx:"table_name" aerospike:"table_name,pk=true"`
	ColumnName             string `sqlx:"column_name" aerospike:"column_name"`
	OrdinalPosition        int    `sqlx:"ordinal_position" aerospike:"ordinal_position"`
	ColumnComment          string `sqlx:"column_comment" aerospike:"column_comment"`
	DataType               string `sqlx:"data_type" aerospike:"data_type"`
	CharacterMaximumLength int    `sqlx:"character_maximum_length" aerospike:"character_maximum_length"`
	NumericPrecision       int    `sqlx:"numeric_precision" aerospike:"numeric_precision"`
	NumericScale           int    `sqlx:"numeric_scale" aerospike:"numeric_scale"`
	IsNullable             string `sqlx:"is_nullable" aerospike:"is_nullable"`
	ColumnDefault          string `sqlx:"column_default" aerospike:"column_default"`
	ColumnKey              string `sqlx:"column_key" aerospike:"column_key"`
	IsAutoIncrement        int    `sqlx:"is_autoincrement" aerospike:"is_autoincrement"`
}

func (s *Statement) handleInformationSchema(ctx context.Context, keys []*as.Key, rows *Rows) (driver.Rows, error) {
	switch strings.ToLower(s.set) {
	case "schemata":
		return s.handleSchemaSet(ctx, keys, rows)
	case "tables":
		return s.handleTableSet(ctx, keys, rows)
	case "columns":
		return s.handleTableInfo(ctx, keys, rows)

	}

	return nil, fmt.Errorf("unsupported InformationSchema: %v", s.set)
}

func (s *Statement) handleSchemaSet(ctx context.Context, keys []*as.Key, rows *Rows) (driver.Rows, error) {
	indexedKeys := map[string]bool{}
	for _, key := range keys {
		indexedKeys[key.Value().GetObject().(string)] = true
	}

	aNodes := s.client.Cluster().GetNodes()
	if len(aNodes) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}
	aNode := aNodes[0]
	policy := as.NewInfoPolicy()
	namespaceMap, err := aNode.RequestInfo(policy, "namespaces")
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve namespaces: %v", err)
	}
	var namespaces = []string{}
	values, _ := namespaceMap["namespaces"]
	if !strings.Contains(values, s.cfg.namespace) {
		values += ";" + s.cfg.namespace
	}
	for _, candidate := range strings.Split(values, ";") {
		if len(indexedKeys) > 0 && !indexedKeys[candidate] {
			continue
		}
		namespaces = append(namespaces, candidate)
	}
	recs := make([]*as.Record, 0)
	for _, namespace := range namespaces {
		rec := &as.Record{
			Bins: as.BinMap{
				"schema_name": namespace,
			},
		}
		recs = append(recs, rec)
	}
	rows.rowsReader = newRowsReader(recs)
	return rows, nil
}

func (s *Statement) handleTableInfo(ctx context.Context, keys []*as.Key, rows *Rows) (driver.Rows, error) {
	indexedKeys := map[string]bool{}
	for _, key := range keys {
		indexedKeys[key.Value().GetObject().(string)] = true
	}

	var qualified []*set
	recs := make([]*as.Record, 0)
	for _, item := range s.sets.sets() {
		if len(indexedKeys) > 0 && !indexedKeys[item] {
			continue
		}
		aSet := s.sets.Lookup(item)
		qualified = append(qualified, aSet)
	}
	for _, aSet := range qualified {
		xType := aSet.xType.Type
		for i := 0; i < xType.NumField(); i++ {
			aField := xType.Field(i)
			aTag, err := ParseTag(aField.Tag.Get("aerospike"))
			if err != nil {
				return nil, err
			}
			if aTag.Name == "" {
				aTag.Name = aField.Name
			}
			rec := &as.Record{
				Bins: as.BinMap{
					"table_schema":             s.cfg.namespace,
					"table_name":               aSet.xType.Name,
					"column_name":              aTag.Name,
					"ordinal_position":         i,
					"column_comment":           "",
					"data_type":                aField.Type.String(),
					"character_maximum_length": 0,
					"numeric_precision":        0,
					"numeric_scale":            0,
					"is_nullable":              aField.Type.Kind() == reflect.Ptr,
					"column_default":           "",
					"column_key":               "",
					"is_autoincrement":         0,
				},
			}
			recs = append(recs, rec)
		}
	}
	rows.rowsReader = newRowsReader(recs)
	return rows, nil
}

func (s *Statement) handleTableSet(ctx context.Context, keys []*as.Key, rows *Rows) (driver.Rows, error) {
	indexedKeys := map[string]bool{}
	for _, key := range keys {
		indexedKeys[key.Value().GetObject().(string)] = true
	}

	var qualified []*set
	recs := make([]*as.Record, 0)
	for _, item := range s.sets.sets() {
		if len(indexedKeys) > 0 && !indexedKeys[item] {
			continue
		}
		aSet := s.sets.Lookup(item)
		qualified = append(qualified, aSet)
	}

	for _, aSet := range qualified {
		rec := &as.Record{
			Bins: as.BinMap{
				"table_schema": s.cfg.namespace,
				"table_name":   aSet.xType.Name,
			},
		}
		recs = append(recs, rec)
	}

	rows.rowsReader = newRowsReader(recs)
	return rows, nil
}
