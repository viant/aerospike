package aerospike

import (
	"database/sql/driver"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/viant/sqlparser"
)

func (s *Statement) parseTruncateTable(sql string) error {
	var err error
	if s.truncate, err = sqlparser.ParseTruncateTable(sql); err != nil {
		return err
	}
	return nil
}

func (s *Statement) handleTruncateTable(args []driver.NamedValue) (driver.Result, error) {
	policy := as.NewWritePolicy(0, 0)
	err := s.client.Truncate(policy, s.namespace, s.set, nil)
	return nil, err
}
