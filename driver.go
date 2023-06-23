package aerospike

import "database/sql/driver"

// Driver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type Driver struct{}

// Open new Connection.
// See https://github.com/viant/aerospike#dsn-data-source-name for how
// the DSN string is formatted
func (d Driver) Open(dsn string) (driver.Conn, error) {
	//cfg, err := ParseDSN(dsn)
	//if err != nil {
	//	return nil, err
	//}
	//c := &connector{
	//	cfg: cfg,
	//}
	//return c.Connect(context.Background())
	return nil, nil
}
