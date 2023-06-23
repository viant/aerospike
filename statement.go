package aerospike

import (
	"context"
	"database/sql/driver"
	"fmt"
)

//Statement abstraction implements database/sql driver.Statement interface
type Statement struct{}

//Exec executes statements
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("not supported")
}

//ExecContext executes statements
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return nil, fmt.Errorf("not supported")
}

//Query runs query
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("not supported")
}

//QueryContext runs query
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return nil, fmt.Errorf("not supported")
}

//NumInput returns numinput
func (s *Statement) NumInput() int {
	return 0
}

func (s *Statement) Close() error {
	return nil
}
