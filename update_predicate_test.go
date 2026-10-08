package aerospike

import (
	"database/sql/driver"
	"reflect"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/viant/sqlparser"
)

type matchedTokenRecord struct {
	Code    string `aerospike:"code,pk"`
	Used    bool   `aerospike:"used"`
	Revoked bool   `aerospike:"revoked"`
}

func matchedStatement(t *testing.T) *Statement {
	t.Helper()
	mapper, err := newTypeBasedMapper(reflect.TypeFor[matchedTokenRecord]())
	if err != nil {
		t.Fatal(err)
	}
	return &Statement{mapper: mapper}
}

func TestMatchedUpdateBuildsAtomicBooleanGuards(t *testing.T) {
	statement := matchedStatement(t)
	query, err := sqlparser.ParseUpdate("UPDATE auth_code SET used = ? WHERE code = ? AND used = ? AND revoked = ?")
	if err != nil {
		t.Fatal(err)
	}
	values := []driver.NamedValue{{Ordinal: 1, Value: "synthetic-code"}, {Ordinal: 2, Value: false}, {Ordinal: 3, Value: false}}
	if err = statement.updateCriteria(query.Qualify, values, false); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(statement.pkValues, []interface{}{"synthetic-code"}) {
		t.Fatalf("lost key: %v", statement.pkValues)
	}
	if statement.writeFilter == nil {
		t.Fatal("guarded update lost filter")
	}
	actual, err := statement.writeFilter.Base64()
	if err != nil {
		t.Fatal(err)
	}
	expected, err := as.ExpAnd(as.ExpEq(as.ExpBoolBin("used"), as.ExpBoolVal(false)), as.ExpEq(as.ExpBoolBin("revoked"), as.ExpBoolVal(false))).Base64()
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatal("atomic guard did not preserve both predicates")
	}
	next, err := sqlparser.ParseUpdate("UPDATE auth_code SET used = ? WHERE code = ?")
	if err != nil {
		t.Fatal(err)
	}
	if err = statement.updateCriteria(next.Qualify, values[:1], false); err != nil {
		t.Fatal(err)
	}
	if statement.writeFilter != nil {
		t.Fatal("prepared statement retained a previous execution's filter")
	}
}
func TestMatchedUpdateRejectsUnsupportedGuards(t *testing.T) {
	for _, where := range []string{"code = ? OR used = ?", "code = ? AND used <> ?", "code = ? AND unknown = ?"} {
		t.Run(where, func(t *testing.T) {
			statement := matchedStatement(t)
			query, err := sqlparser.ParseUpdate("UPDATE auth_code SET used = ? WHERE " + where)
			if err != nil {
				t.Fatal(err)
			}
			if err = statement.updateCriteria(query.Qualify, []driver.NamedValue{{Ordinal: 1, Value: "synthetic-code"}, {Ordinal: 2, Value: false}}, false); err == nil {
				t.Fatal("unsupported predicate silently widened write")
			}
		})
	}
}
