package sql

import (
	as "github.com/aerospike/aerospike-client-go/v6"
	"io"
)

type rowsIterator interface {
	Read() (record *as.Record, err error)
}

type RowsScanReader struct {
	*as.Recordset
}

func (r *RowsScanReader) Read() (*as.Record, error) {
	if r.Recordset == nil {
		return nil, io.EOF
	}
	channel := r.Recordset.Results()
	result, ok := <-channel
	if !ok || result.Record == nil {
		return nil, io.EOF
	}
	return result.Record, nil
}

type RowsReader struct {
	index   int
	records []*as.Record
}

func (r *RowsReader) Read() (record *as.Record, err error) {
	if r.index >= len(r.records) {
		return nil, io.EOF
	}
	record = r.records[r.index]
	r.index++
	return record, nil
}

func newRowsReader(records []*as.Record) *RowsReader {
	return &RowsReader{
		records: records,
	}
}

func newInterfaceReader(records []interface{}) *RowsReader {
	var asRecords []*as.Record
	for i := range records {
		var binMap = make(map[string]interface{})
		for k, v := range records[i].(map[interface{}]interface{}) {
			key := k.(string)
			binMap[key] = v
		}
		asRecords = append(asRecords, &as.Record{Bins: binMap})
	}
	return newRowsReader(asRecords)
}
