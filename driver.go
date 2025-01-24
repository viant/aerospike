package aerospike

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"os"
	"strings"
	"sync"
)

const (
	scheme = "aerospike"
)

func init() {
	sql.Register(scheme, &Driver{})
}

var connections = make(map[string]*connection)
var mutex = &sync.RWMutex{}

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

	mutex.RLock()
	ret := connections[dsn]
	mutex.RUnlock()
	if ret != nil {
		return ret, nil
	}

	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	clientPolicy := as.NewClientPolicy()

	err = cfg.adjustClientPolicy(clientPolicy)
	if err != nil {
		return nil, err
	}

	client, err := as.NewClientWithPolicy(clientPolicy, cfg.host, cfg.port)
	if err != nil {
		return nil, err
	}

	err = cfg.adjustDefaultPolicy(client.DefaultPolicy)
	if err != nil {
		return nil, err
	}

	err = cfg.adjustDefaultWritePolicy(client.DefaultWritePolicy)
	if err != nil {
		return nil, err
	}

	limiter := writeLimiter.getLimiter(dsn, cfg.maxConcurrentWrite)
	ret = &connection{
		cfg:          cfg,
		client:       client,
		sets:         newRegistry(),
		writeLimiter: limiter,
	}
	if !cfg.disablePool {
		mutex.Lock()
		connections[dsn] = ret
		mutex.Unlock()
	}

	return ret, nil
}
