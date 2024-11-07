package aerospike

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"os"
	"strings"
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

func isDryRun(operation string) bool {
	return strings.Contains(os.Getenv("AEROSPIKE_DRY_RUN"), operation)
}

func isDebugOn() bool {
	return os.Getenv("AEROSPIKE_DEBUG") != ""
}

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

	client, err := as.NewClientWithPolicy(cfg.ClientPolicy, cfg.host, cfg.port)
	if err != nil {
		return nil, err
	}

	limiter := writeLimiter.getLimiter(dsn, cfg.maxConcurrentWrite)
	return &connection{
		cfg:          cfg,
		client:       client,
		sets:         newRegistry(),
		writeLimiter: limiter,
	}, nil
}
