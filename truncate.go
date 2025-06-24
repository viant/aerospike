package aerospike

import (
	"context"
	"database/sql/driver"
	"github.com/viant/sqlparser"
)

func (s *Statement) parseTruncateTable(sql string) error {
	var err error
	if s.truncate, err = sqlparser.ParseTruncateTable(sql); err != nil {
		return err
	}
	return nil
}

func (s *Statement) handleTruncateTable(ctx context.Context) (driver.Result, error) {
	err := s.truncateWithCtx(ctx, nil, s.namespace, s.set, nil)
	return &result{}, err
}
