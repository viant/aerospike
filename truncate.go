package aerospike

import "github.com/viant/sqlparser"

func (s *Statement) parseTruncateTable(sql string) error {
	var err error
	if s.truncate, err = sqlparser.ParseTruncateTable(sql); err != nil {
		return err
	}
	return nil
}
