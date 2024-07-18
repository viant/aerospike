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

func (s *Statement) handleInformationSchema(ctx context.Context, keys []*as.Key, rows *Rows) (driver.Rows, error) {
	switch strings.ToLower(s.set) {
	case "schemata":
		return s.handleSchemaSet(ctx, keys, rows)
	case "table_info": //
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
