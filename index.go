package aerospike

import (
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/viant/sqlparser"
	"strings"
	"time"
)

func (s *Statement) handleDropIndex(args []driver.NamedValue) (driver.Result, error) {
	writePolicy := as.NewWritePolicy(0, 0)
	err := s.client.DropIndex(writePolicy, s.dropIndex.Schema, s.dropIndex.Table, s.dropIndex.Name)
	if s.dropIndex.IfExists {
		return &result{}, nil
	}
	return &result{}, err
}

func (s *Statement) prepareDropIndex(SQL string) error {
	dropIndex, err := sqlparser.ParseDropIndex(SQL)
	if err != nil {
		return err
	}
	s.dropIndex = dropIndex
	if s.dropIndex.Schema == "" {
		s.dropIndex.Schema = s.namespace
	}
	return nil
}

func (s *Statement) prepareCreateIndex(SQL string) error {
	createIndex, err := sqlparser.ParseCreateIndex(SQL)
	if err != nil {
		return err
	}
	s.createIndex = createIndex
	if s.createIndex.Schema == "" {
		s.createIndex.Schema = s.namespace
	}
	return nil
}

func (s *Statement) handleCreateIndex(args []driver.NamedValue) (driver.Result, error) {
	var indexType as.IndexType
	switch strings.ToLower(s.createIndex.Type) {
	case "numeric":
		indexType = as.NUMERIC
	case "string":
		indexType = as.STRING
	case "geo2dsphere":
		indexType = as.GEO2DSPHERE
	}
	if len(s.createIndex.Columns) != 1 {
		return nil, fmt.Errorf("unsupported secondaryIndex columns: %v", s.createIndex.Columns)
	}
	if indexType == "" {
		return nil, fmt.Errorf("unsupported secondaryIndex type: %v", s.createIndex.Type)
	}
	writePolicy := as.NewWritePolicy(0, 0)

	task, err := s.client.CreateIndex(writePolicy, s.createIndex.Schema, s.createIndex.Table, s.createIndex.Name, s.createIndex.Columns[0].Name, indexType)

	for i := 0; i < 1000; i++ {
		ok, err := task.IsDone()
		if ok || err != nil {
			if err == nil {
				err = <-task.OnComplete()
			}
			return &result{}, err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return &result{}, err
}
