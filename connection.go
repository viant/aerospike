package aerospike

import (
	"context"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/viant/sqlparser"
)

type connection struct {
	cfg    *Config
	client *as.Client
	sets   *registry
}

// Prepare returns a prepared statement, bound to this connection.
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *connection) PrepareContext(ctx context.Context, SQL string) (driver.Stmt, error) {
	kind := sqlparser.ParseKind(SQL)
	c.sets.Merge(globalSets)

	stmt := &Statement{
		SQL:       SQL,
		kind:      kind,
		sets:      c.sets,
		client:    c.client,
		cfg:       c.cfg,
		namespace: c.cfg.namespace,
	}
	stmt.checkQueryParameters()

	switch kind {
	case sqlparser.KindSelect:
		if err := stmt.prepareSelect(SQL); err != nil {
			return nil, err
		}
	case sqlparser.KindInsert:
		if err := stmt.prepareInsert(SQL); err != nil {
			return nil, err
		}
	case sqlparser.KindUpdate:
		if err := stmt.prepareUpdate(SQL); err != nil {
			return nil, err
		}
	case sqlparser.KindDelete:
		if err := stmt.prepareDelete(SQL); err != nil {
			return nil, err
		}
	case sqlparser.KindRegisterSet:
	case sqlparser.KindCreateIndex:
		if err := stmt.prepareCreateIndex(SQL); err != nil {
			return nil, err
		}
		return stmt, nil
	case sqlparser.KindDropIndex:
		if err := stmt.prepareDropIndex(SQL); err != nil {
			return nil, err
		}
		return stmt, nil
	default:
		return nil, fmt.Errorf("unsupported kind: %v for DDL: %v", kind, SQL)
	}

	if err := stmt.setTypeBasedMapper(); err != nil {
		return nil, err
	}

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
