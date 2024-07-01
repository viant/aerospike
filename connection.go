package aerospike

import (
	"context"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/sqlparser"
	"github.com/viant/x"
)

type connection struct {
	cfg    *Config
	client *as.Client
	types  *x.Registry
}

// Prepare returns a prepared statement, bound to this connection.
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *connection) PrepareContext(ctx context.Context, SQL string) (driver.Stmt, error) {
	kind := sqlparser.ParseKind(SQL)
	if !(kind.IsRegisterSet() || kind.IsSelect()) {
		return nil, fmt.Errorf("unsupported SQL kind: %v", SQL)
	}

	c.types.Merge(globalTypes)
	stmt := &Statement{SQL: SQL, Kind: kind, types: c.types, client: c.client}
	stmt.checkQueryParameters()

	//if kind.IsSelect() {
	//	if err := stmt.prepareSelect(SQL); err != nil {
	//		return nil, err
	//	}
	//}
	return stmt, nil
}

// Ping pings server
func (c *connection) Ping(ctx context.Context) error {
	return nil
}

// Begin starts and returns a new transaction.
func (c *connection) Begin() (driver.Tx, error) {
	return &tx{c}, nil
}

// BeginTx starts and returns a new transaction.
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return &tx{c}, nil
}

// Close closes connection
func (c *connection) Close() error {
	return nil
}

// ResetSession resets session
func (c *connection) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid check is connection is valid
func (c *connection) IsValid() bool {
	return true
}
