package aerospike

import (
	"database/sql/driver"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"reflect"
	"testing"
)

func TestDiscoveryConstantCriteria(t *testing.T) {
	for _, tc := range []struct {
		where               string
		impossible, failure bool
		keys                []interface{}
	}{
		{"1=0", true, false, nil}, {"1=0 AND (pk=?)", true, false, nil},
		{"(pk=?) AND (1=0)", true, false, nil}, {"((1=0) AND (pk=?))", true, false, nil},
		{"1=1 AND (pk=?)", false, false, []interface{}{7}},
		{"(pk=?) AND (1=1)", false, false, []interface{}{7}},
		{"1=0 OR pk=?", false, true, nil}, {"1=0 AND (pk=? OR pk=?)", false, true, nil},
	} {
		t.Run(tc.where, func(t *testing.T) {
			q, err := sqlparser.ParseQuery("SELECT pk FROM session WHERE " + tc.where)
			if err != nil {
				t.Fatal(err)
			}

			s := &Statement{mapper: &mapper{}}
			err = s.updateCriteria(q.Qualify, []driver.NamedValue{{Ordinal: 1, Value: 7}, {Ordinal: 2, Value: 8}}, true)
			if (err != nil) != tc.failure {
				t.Fatalf("error=%v", err)
			}
			if err != nil {
				return
			}
			if s.falsePredicate != tc.impossible || !reflect.DeepEqual(s.pkValues, tc.keys) {
				t.Fatalf("false=%v keys=%v", s.falsePredicate, s.pkValues)
			}
		})
	}
}

func TestDiscoveryCriteriaExecutionReset(t *testing.T) {
	s := &Statement{mapper: &mapper{}}
	for _, tc := range []struct {
		where      string
		key        int
		impossible bool
	}{{"1=0 AND pk=?", 7, true}, {"1=1 AND pk=?", 8, false}, {"pk=?", 9, false}} {
		q, err := sqlparser.ParseQuery("SELECT pk FROM session WHERE " + tc.where)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.updateCriteria(q.Qualify, []driver.NamedValue{{Ordinal: 1, Value: tc.key}}, true); err != nil {
			t.Fatal(err)
		}
		if s.falsePredicate != tc.impossible {
			t.Fatal("stale false predicate")
		}
		if !tc.impossible && !reflect.DeepEqual(s.pkValues, []interface{}{tc.key}) {
			t.Fatalf("stale key=%v", s.pkValues)
		}
	}
}

func TestDiscoveryOuterProjection(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		columns []string
		failure bool
	}{
		{"SELECT * FROM (SELECT user_id,last_seen FROM session) s WHERE 1=0", []string{"user_id", "last_seen"}, false},
		{"SELECT s.* FROM (SELECT user_id AS id,last_seen FROM session) s WHERE 1=0", []string{"user_id AS id", "last_seen"}, false},
		{"SELECT s.id AS owner FROM (SELECT user_id AS id FROM session) s WHERE 1=0", []string{"user_id AS owner"}, false},
		{"SELECT s.missing FROM (SELECT user_id FROM session) s WHERE 1=0", nil, true},
		{"SELECT wrong.* FROM (SELECT user_id FROM session) s WHERE 1=0", nil, true},
		{"SELECT * FROM (SELECT user_id FROM session WHERE user_id=?) s WHERE 1=0", []string{"user_id"}, false},
		{"SELECT * FROM (SELECT user_id FROM session WHERE user_id=?) s WHERE user_id=?", nil, true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			q, err := sqlparser.ParseQuery(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			s := &Statement{query: q}
			name := ""
			raw, ok := q.From.X.(*expr.Raw)
			if !ok {
				t.Fatalf("source=%T", q.From.X)
			}
			err = s.remapInnerQuery(raw, &name)
			if (err != nil) != tc.failure {
				t.Fatalf("error=%v", err)
			}
			if err != nil {
				return
			}
			columns := []string{}
			for _, item := range s.query.List {
				columns = append(columns, sqlparser.Stringify(item))
			}
			if !reflect.DeepEqual(columns, tc.columns) {
				t.Fatalf("columns=%q want%q", columns, tc.columns)
			}
		})
	}
}
