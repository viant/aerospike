package aerospike

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/viant/sqlparser"
	"runtime"
	"testing"
	"time"
)

type faultConnector struct{ rollbacks *int }

func (c faultConnector) Connect(context.Context) (driver.Conn, error) {
	return &faultConnection{rollbacks: c.rollbacks}, nil
}
func (c faultConnector) Driver() driver.Driver { return faultDriver{} }

type faultDriver struct{}

func (faultDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type faultConnection struct{ rollbacks *int }

func (c *faultConnection) Prepare(string) (driver.Stmt, error) {
	return &Statement{kind: sqlparser.KindTruncateTable}, nil
}
func (*faultConnection) Close() error { return nil }
func (c *faultConnection) Begin() (driver.Tx, error) {
	return &faultTransaction{rollbacks: c.rollbacks}, nil
}

type faultTransaction struct{ rollbacks *int }

func (*faultTransaction) Commit() error     { return errors.New("unexpected commit") }
func (t *faultTransaction) Rollback() error { *t.rollbacks++; return nil }

func TestExecutionRuntimeFaultReleasesTransaction(t *testing.T) {
	count := 0
	db := sql.OpenDB(faultConnector{rollbacks: &count})
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "synthetic runtime-fault statement")
	if err != nil {
		t.Fatal(err)
	}
	// A missing parsed truncate node produces a real nil-dereference fault in
	// the driver, below database/sql's statement/transaction locking boundary.
	result, err := stmt.ExecContext(ctx)
	var fault runtime.Error
	if result != nil || !errors.As(err, &fault) {
		t.Fatalf("result=%v fault=%T", result, err)
	}
	done := make(chan error, 1)
	go func() { done <- tx.Rollback() }()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime fault stranded transaction statement lock")
	}
	if count != 1 {
		t.Fatalf("rollback attempts=%d", count)
	}
}
