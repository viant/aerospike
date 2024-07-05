package sql

import (
	as "github.com/aerospike/aerospike-client-go/v4"
	"io"
)

type rowsIterator interface {
	Read() (record *as.Record, err error)
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
