package aerospike

import (
	"database/sql/driver"
	"reflect"
)

type Rows struct {
}

// Columns returns query columns
func (r *Rows) Columns() []string {
	return nil
}

// Close closes rows
func (r *Rows) Close() error {
	return nil
}

// Next moves to next row
func (r *Rows) Next(dest []driver.Value) error {
	return nil
}

// ColumnTypeScanType returns column scan type
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	return nil
}

// ColumnTypeDatabaseTypeName returns column database type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return ""
}

// ColumnTypeNullable returns if column is nullable
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return false, false
}
