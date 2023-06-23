package aerospike

import (
	"context"
	"database/sql/driver"
)

type connection struct {
}

// Prepare returns a prepared statement, bound to this connection.
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *connection) PrepareContext(ctx context.Context, SQL string) (driver.Stmt, error) {
	return &Statement{}, nil
}

//Ping pings server
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

//ResetSession resets session
func (c *connection) ResetSession(ctx context.Context) error {
	return nil
}

//IsValid check is connection is valid
func (c *connection) IsValid() bool {
	return true
}
