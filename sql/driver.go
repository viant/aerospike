package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v4"
	"github.com/viant/x"
)

const (
	scheme = "aerospike"
)

func init() {
	sql.Register(scheme, &Driver{})
}

// Driver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type Driver struct{}

// Open new Connection.
// See https://github.com/viant/aerospike#dsn-data-source-name for how
// the DSN string is formatted
func (d Driver) Open(dsn string) (driver.Conn, error) {
	if dsn == "" {
		return nil, fmt.Errorf("aerospike dsn was empty")
	}

	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	client, err := as.NewClient(cfg.host, cfg.port) //TODO
	if err != nil {
		return nil, err
	}
	/*
		client, err := as.NewClientWithPolicy(cfg.ClientPolicy, cfg.host, cfg.port)
		if err != nil {
			return nil, err
		}
	*/
	return &connection{
		cfg:    cfg,
		client: client,
		types:  x.NewRegistry(),
	}, nil
}
