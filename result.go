package aerospike

import (
	"errors"
)

var errLastInsertID = errors.New("lastInsertId is not supported")

type result struct {
	totalRows         int64
	lastInsertedID    int64
	hasLastInsertedID bool
}

// LastInsertId returns not supported error
func (r *result) LastInsertId() (int64, error) {
	if r.hasLastInsertedID {
		return r.lastInsertedID, nil
	}
	return 0, errLastInsertID
}

// RowsAffected return affected rows
func (r *result) RowsAffected() (int64, error) {
	return r.totalRows, nil
}
