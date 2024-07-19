package aerospike

import (
	"context"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"strings"
)

type catalog struct {
	SchemaName string `sqlx:"schema_name" aerospike:"schema_name,pk=true"`
}

type table struct {
	TableSchema string `sqlx:"table_schema" aerospike:"table_schema"`
	TableName   string `sqlx:"table_name" aerospike:"table_name"`
}

func (s *Statement) handleInformationSchema(ctx context.Context, keys []*as.Key, rows *Rows) (driver.Rows, error) {
	switch strings.ToLower(s.set) {
	case "schemata":
		return s.handleSchemaSet(ctx, keys, rows)
	case "tables":
		return s.handleTableSet(ctx, keys, rows)
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

func (s *Statement) handleTableSet(ctx context.Context, keys []*as.Key, rows *Rows) (driver.Rows, error) {
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
	namespaceMap, err := aNode.RequestInfo(policy, "sets")
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve namespaces: %v", err)
	}
	values, _ := namespaceMap["sets"]

	/* example value with \n added
	ns=test:set=AAA01:objects=2:tombstones=0:memory_data_bytes=0:device_data_bytes=544:truncate_lut=0:sindexes=1:index_populating=false:disable-eviction=false:enable-index=false:stop-writes-count=0;
	ns=test:set=AAA02:objects=10:tombstones=0:memory_data_bytes=0:device_data_bytes=1120:truncate_lut=0:sindexes=1:index_populating=false:disable-eviction=false:enable-index=false:stop-writes-count=0;
	*/

	// TODO add sets from registries
	// TODO what to do if registry vs db set collision?
	recs := make([]*as.Record, 0)
	//for _, line := range strings.Split(values, ";") {
	//	parts := strings.Split(line, ":")
	//	if len(parts) < 2 {
	//		continue
	//	}
	//
	//	namespace := ""
	//	idx := strings.Index(parts[0], "=")
	//	if idx+1 <= len(parts[0])-1 {
	//		namespace = parts[0][idx+1:]
	//	}
	//
	//	set := ""
	//	idx = strings.Index(parts[1], "=")
	//	if idx+1 <= len(parts[1])-1 {
	//		set = parts[1][idx+1:]
	//	}
	//
	//	rec := &as.Record{
	//		Bins: as.BinMap{
	//			"table_schema": namespace,
	//			"table_name":   set,
	//		},
	//	}
	//	recs = append(recs, rec)
	//}

	for _, item := range s.sets.sets() {
		rec := &as.Record{
			Bins: as.BinMap{
				"table_schema": s.namespace,
				"table_name":   item,
			},
		}
		recs = append(recs, rec)
	}

	rows.rowsReader = newRowsReader(recs)
	return rows, nil
}
