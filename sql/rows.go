package sql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/viant/sqlparser/query"
	"github.com/viant/structology"
	"github.com/viant/xunsafe"
	"io"
	"reflect"
	"unsafe"
)

type Rows struct {
	recordType    reflect.Type
	mapper        *mapper
	query         *query.Select
	zeroRecord    []byte
	record        interface{}
	rowsReader    rowsIterator
	processedRows uint64
}

// Columns returns query columns
func (r *Rows) Columns() []string {
	if r.mapper == nil {
		return nil
	}
	var result []string
	for _, aField := range r.mapper.fields {
		result = append(result, aField.tag.Name)
	}
	return result
}

// Close closes rows
func (r *Rows) Close() error {
	return nil
}

// Next moves to next row
func (r *Rows) Next(dest []driver.Value) error {
	record, err := r.rowsReader.Read()
	if errors.Is(err, as.ErrRecordsetClosed) {
		return io.EOF
	}
	if err != nil {
		return err
	}

	//reset record with nil, or 0 values
	copy(unsafe.Slice((*byte)(xunsafe.AsPointer(r.record)), r.recordType.Size()), r.zeroRecord)
	ptr := xunsafe.AsPointer(r.record)
	if err := r.transferBinValues(dest, record, ptr); err != nil {
		return err
	}
	r.processedRows++
	return nil
}

func (r *Rows) transferBinValues(dest []driver.Value, record *as.Record, ptr unsafe.Pointer) error {
	for i, aField := range r.mapper.fields {
		value, ok := record.Bins[aField.tag.Name]
		if !ok {
			continue
		}
		srcType := reflect.TypeOf(value)
		if srcType.AssignableTo(aField.Type) {
			aField.Set(ptr, value)
		} else {
			if aField.setter == nil {
				aField.setter = structology.LookupSetter(srcType, aField.Type)
			}
			if aField.setter == nil {
				//TODO add support for struct, slice of structs
				return fmt.Errorf("failed to find setter for %v", aField.Type)
			}
			if err := aField.setter(value, aField.Field, ptr); err != nil {
				return err
			}
			dest[i] = aField.Value(ptr)
			continue
		}
		dest[i] = aField.Value(ptr)
	}
	return nil
}

// ColumnTypeScanType returns column scan type
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	if r.mapper == nil {
		return nil
	}
	if index < len(r.mapper.fields) {
		return r.mapper.fields[index].Type
	}
	return nil
}

// ColumnTypeDatabaseTypeName returns column database type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < len(r.mapper.fields) {
		return r.mapper.fields[index].Type.Name()
	}
	return ""
}

// ColumnTypeNullable returns if column is nullable
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index < len(r.mapper.fields) {
		fType := r.mapper.fields[index].Type
		if fType.Kind() == reflect.Ptr {
			return true, true
		}
		return false, true
	}
	return false, false
}
