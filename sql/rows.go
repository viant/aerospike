package sql

import (
	"database/sql/driver"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/xunsafe"
	"io"
	"reflect"
	"unsafe"
)

type Rows struct {
	//criteria       *iexpr.Bool
	//scope          *igo.Scope
	//recordSelector *exec.Selector
	recordType  reflect.Type
	mapper      *mapper
	query       *query.Select
	recordIndex int
	zeroRecord  []byte
	record      interface{}
	///
	//recordset     *as.Recordset //TODO DELETE
	records       []*as.Record
	processedRows uint64
}

// Columns returns query columns
func (r *Rows) Columns() []string {
	var result []string
	if r.query.List.IsStarExpr() {
		for i := range r.mapper.fields {
			result = append(result, r.recordType.Field(i).Name)
		}
		return result
	}
	for _, column := range r.query.List {
		switch actual := column.Expr.(type) {
		case expr.Ident:
			result = append(result, actual.Name)
		default:
			return nil
		}
	}
	return result
}

// Close closes rows
func (r *Rows) Close() error {
	return nil
}

// Next moves to next row
func (r *Rows) Next(dest []driver.Value) error {
	if r.recordIndex >= len(r.records) {
		return io.EOF
	}

	record := r.records[r.recordIndex]
	r.recordIndex++

	copy(unsafe.Slice((*byte)(xunsafe.AsPointer(r.record)), r.recordType.Size()), r.zeroRecord)

	//TODO copy data from record to r.record
	ptr := xunsafe.AsPointer(r.record)

	for i, aField := range r.mapper.fields {

		value, ok := record.Bins[aField.tag.Name]
		if !ok {
			continue
		}
		aField.Set(ptr, value)
		dest[i] = aField.Value(ptr)
	}
	r.processedRows++ //TODO
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
