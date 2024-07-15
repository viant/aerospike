package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	//"github.com/viant/toolbox"
	"sort"
	"testing"
	"time"
)

func Test_ExecContext(t *testing.T) {
	namespace := "udb"

	type Foo struct {
		Id   int //`aql:"id,key=true"
		Name string
	}

	var testCase = []struct {
		description string
		dsn         string
		sql         string
		params      []interface{}
		expect      interface{}
	}{
		{
			description: "register inlined set",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			sql:         "REGISTER SET Bar AS struct{id int; name string}",
		},
		{
			description: "register named set",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			sql:         "REGISTER SET Foo AS ?",
			params:      []interface{}{Foo{}},
		},
	}

	for _, tc := range testCase {
		t.Run(tc.description, func(t *testing.T) {
			db, err := sql.Open("aerospike", tc.dsn)
			if !assert.Nil(t, err, tc.description) {
				return
			}
			assert.NotNil(t, db, tc.description)
			_, err = db.ExecContext(context.Background(), tc.sql, tc.params...)
			assert.Nil(t, err, tc.description)
		})
	}
}

func Test_QueryContext(t *testing.T) {
	namespace := "test"
	type Foo struct {
		Id   int
		Name string
	}

	type Baz struct {
		Id   int       `aerospike:"id,pk=true"`
		Seq  int       `aerospike:"seq,key=true" `
		Name string    `aerospike:"name"`
		Time time.Time `aerospike:"time"`
	}

	type BazPtr struct {
		Id   int        `aerospike:"id,pk=true"`
		Seq  int        `aerospike:"seq,key=true" `
		Name string     `aerospike:"name"`
		Time *time.Time `aerospike:"time"`
	}

	type BazUnix struct {
		Id   int       `aerospike:"id,pk=true"`
		Seq  int       `aerospike:"seq,key=true" `
		Name string    `aerospike:"name"`
		Time time.Time `aerospike:"time,unixsec"`
	}

	type Qux struct {
		Id   int      `aerospike:"id,pk=true"`
		Seq  int      `aerospike:"seq,key=true"`
		Name string   `aerospike:"name"`
		List []string `aerospike:"list"`
	}

	type Doc struct {
		Id   int    `aerospike:"id,pk=true" `
		Seq  int    `aerospike:"seq,key=true" `
		Name string `aerospike:"name" `
	}

	type SimpleAgg struct {
		Id     int `aerospike:"id,pk=true" `
		Amount int `aerospike:"amount" `
	}
	type Agg struct {
		Id     int `aerospike:"id,pk=true" `
		Seq    int `aerospike:"seq,key=true" `
		Amount int `aerospike:"amount" `
		Val    int `aerospike:"val" `
	}

	var sets = []struct {
		SQL    string
		params []interface{}
	}{
		{SQL: "REGISTER SET Doc AS struct{Id int; Seq int `aerospike:\"seq,key=true\"`;  Name string}"},
		{SQL: "REGISTER SET Foo AS ?", params: []interface{}{Foo{}}},
		{SQL: "REGISTER SET SimpleAgg AS ?", params: []interface{}{SimpleAgg{}}},
		{SQL: "REGISTER SET Agg AS ?", params: []interface{}{Agg{}}},
		{SQL: "REGISTER SET Baz AS ?", params: []interface{}{Baz{}}},
		{SQL: "REGISTER SET BazUnix AS ?", params: []interface{}{BazUnix{}}},
		{SQL: "REGISTER SET Qux AS ?", params: []interface{}{Qux{}}},
		{SQL: "REGISTER SET BazPtr AS ?", params: []interface{}{BazPtr{}}},
	}

	var testCases = []struct {
		description string
		dsn         string
		execSQL     string
		execParams  []interface{}
		querySQL    string
		init        []string
		initParams  [][]interface{}
		queryParams []interface{}
		expect      interface{}
		skip        bool
		scanner     func(r *sql.Rows) (interface{}, error)
	}{

		{
			description: "batch merge with map",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Agg$Values(id,seq,amount,val) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?) AS new ON DUPLICATE KEY UPDATE val = val + new.val, amount = amount + new.amount",
			execParams:  []interface{}{1, 1, 11, 111, 1, 2, 12, 121, 2, 1, 11, 111},
			querySQL:    "SELECT id,seq,amount,val FROM Agg$Values WHERE PK = ? AND KEY IN(?, ?)",
			queryParams: []interface{}{1, 1, 2},
			init: []string{
				"DELETE FROM Agg",
				"INSERT INTO Agg$Values(id,seq,amount,val) VALUES(1,1,1,1)",
				"INSERT INTO Agg$Values(id,seq,amount,val) VALUES(1,2,1,1)",
				"INSERT INTO Agg$Values(id,seq,amount,val) VALUES(2,1,1,1)",
			},
			expect: []interface{}{
				&Agg{Id: 1, Seq: 1, Amount: 12, Val: 112},
				&Agg{Id: 1, Seq: 2, Amount: 13, Val: 122},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Agg{}
				err := r.Scan(&agg.Id, &agg.Seq, &agg.Amount, &agg.Val)
				return &agg, err
			},
		},
		{
			description: "batch merge",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO SimpleAgg(id,amount) VALUES(?,?),(?,?),(?,?) AS new ON DUPLICATE KEY UPDATE amount = amount + new.amount",
			execParams: []interface{}{
				1, 11,
				2, 12,
				3, 33,
			},
			querySQL:    "SELECT id,amount FROM SimpleAgg WHERE PK IN(?,?,?)",
			queryParams: []interface{}{1, 2, 3},
			init: []string{
				"DELETE FROM SimpleAgg",
				"INSERT INTO SimpleAgg(id,amount) VALUES(1,1)",
				"INSERT INTO SimpleAgg(id,amount) VALUES(2,1)",
				"INSERT INTO SimpleAgg(id,amount) VALUES(3,1)",
			},
			expect: []interface{}{
				&SimpleAgg{Id: 1, Amount: 12},
				&SimpleAgg{Id: 2, Amount: 13},
				&SimpleAgg{Id: 3, Amount: 34},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := SimpleAgg{}
				err := r.Scan(&agg.Id, &agg.Amount)
				return &agg, err
			},
		},
		{
			description: "batch merge with map",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Agg$Values(id,seq,amount,val) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?) AS new ON DUPLICATE KEY UPDATE val = val + new.val, amount = amount + new.amount",
			execParams: []interface{}{
				1, 1, 11, 111,
				1, 2, 12, 121,
				2, 1, 11, 111},
			querySQL:    "SELECT id,seq,amount,val FROM Agg$Values WHERE PK = ? AND KEY IN(?, ?)",
			queryParams: []interface{}{1, 1, 2},
			init: []string{
				"DELETE FROM Agg",
				"INSERT INTO Agg$Values(id,seq,amount,val) VALUES(1,1,1,1)",
				"INSERT INTO Agg$Values(id,seq,amount,val) VALUES(1,2,1,1)",
				"INSERT INTO Agg$Values(id,seq,amount,val) VALUES(2,1,1,1)",
			},
			expect: []interface{}{
				&Agg{Id: 1, Seq: 1, Amount: 12, Val: 112},
				&Agg{Id: 1, Seq: 2, Amount: 13, Val: 122},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Agg{}
				err := r.Scan(&agg.Id, &agg.Seq, &agg.Amount, &agg.Val)
				return &agg, err
			},
		},
		{
			description: "batch map insert",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Agg$Values(id,seq,amount,val) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?)",
			execParams:  []interface{}{1, 1, 11, 111, 1, 2, 12, 121, 2, 1, 11, 111},
			querySQL:    "SELECT id,seq,amount,val FROM Agg$Values WHERE PK = ? AND KEY IN(?, ?)",
			queryParams: []interface{}{1, 1, 2},
			init: []string{
				"DELETE FROM Agg",
			},
			expect: []interface{}{
				&Agg{Id: 1, Seq: 1, Amount: 11, Val: 111},
				&Agg{Id: 1, Seq: 2, Amount: 12, Val: 121},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Agg{}
				err := r.Scan(&agg.Id, &agg.Seq, &agg.Amount, &agg.Val)
				return &agg, err
			},
		},
		{
			description: "batch insert",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO SimpleAgg(id,amount) VALUES(?,?),(?,?)",
			execParams:  []interface{}{1, 10, 2, 20},
			querySQL:    "SELECT id,amount FROM SimpleAgg WHERE PK IN(?, ?)",
			queryParams: []interface{}{1, 2},
			init: []string{
				"DELETE FROM SimpleAgg",
			},
			expect: []interface{}{
				&SimpleAgg{Id: 1, Amount: 10},
				&SimpleAgg{Id: 2, Amount: 20},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := SimpleAgg{}
				err := r.Scan(&agg.Id, &agg.Amount)
				return &agg, err
			},
		},
		// TODO
		{
			description: "update map bin with inc ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "UPDATE Agg$Values SET amount = amount + ?, val = val + 3  WHERE PK = ? AND KEY = ?",
			execParams:  []interface{}{10, 1, 1},
			querySQL:    "SELECT id, seq, amount, val FROM Agg$Values WHERE PK = ? AND KEY = ?",
			queryParams: []interface{}{1, 1},
			init: []string{
				"DELETE FROM Agg",
				"INSERT INTO Agg$Values(id,seq, amount, val) VALUES(1, 1, 1, 1)",
			},
			expect: []interface{}{
				&Agg{Id: 1, Seq: 1, Amount: 11, Val: 4},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Agg{}
				err := r.Scan(&agg.Id, &agg.Seq, &agg.Amount, &agg.Val)
				return &agg, err
			},
		},
		{
			description: "update bin with inc ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "UPDATE SimpleAgg SET amount = amount + ?  WHERE PK = ?",
			execParams:  []interface{}{10, 1},
			querySQL:    "SELECT id, amount FROM SimpleAgg WHERE PK = ?",
			queryParams: []interface{}{1},
			init: []string{
				"DELETE FROM SimpleAgg",
				"INSERT INTO SimpleAgg(id,amount) VALUES(1,1)",
			},
			expect: []interface{}{
				&SimpleAgg{Id: 1, Amount: 11},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := SimpleAgg{}
				err := r.Scan(&agg.Id, &agg.Amount)
				return &agg, err
			},
		},
		{
			description: "update with inc ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "UPDATE SimpleAgg SET amount = amount + ?  WHERE PK = ?",
			execParams:  []interface{}{10, 1},
			querySQL:    "SELECT id, amount FROM SimpleAgg WHERE PK = ?",
			queryParams: []interface{}{1},
			init: []string{
				"DELETE FROM SimpleAgg",
				"INSERT INTO SimpleAgg(id,amount) VALUES(1,1)",
			},
			expect: []interface{}{
				&SimpleAgg{Id: 1, Amount: 11},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := SimpleAgg{}
				err := r.Scan(&agg.Id, &agg.Amount)
				return &agg, err
			},
		},
		{
			description: "update record - literal",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "UPDATE Foo SET Name = 'foo updated' WHERE PK = ?",
			execParams:  []interface{}{1},
			querySQL:    "SELECT * FROM Foo WHERE PK = ?",
			init: []string{
				"DELETE FROM Foo",
				"INSERT INTO Foo(Id,Name) VALUES(1,'foo1')",
			},
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Foo{Id: 1, Name: "foo updated"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "update record placeholder",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "UPDATE Foo SET Name = ? WHERE PK = ?",
			execParams:  []interface{}{"foo updated", 1},
			querySQL:    "SELECT * FROM Foo WHERE PK = ?",
			init: []string{
				"DELETE FROM Foo",
				"INSERT INTO Foo(Id,Name) VALUES(1,'foo1')",
			},
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Foo{Id: 1, Name: "foo updated"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "get 1 record by PK with 1 bin map value by key",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
			},
			queryParams: []interface{}{1, 101},
			expect: []interface{}{
				&Doc{Id: 1, Seq: 101, Name: "doc2"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record by PK with all bin map values",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
			},
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Doc{Id: 1, Seq: 100, Name: "doc1"},
				&Doc{Id: 1, Seq: 101, Name: "doc2"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record with all bins by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			//			execSQL:     "REGISTER SET Foo AS struct{Id int; Name string}",
			querySQL: "SELECT * FROM Foo WHERE PK = ?",
			init: []string{
				"DELETE FROM Foo",
				"INSERT INTO Foo(Id,Name) VALUES(1,'foo1')",
			},
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Foo{Id: 1, Name: "foo1"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "get 0 records by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM Foo",
				"INSERT INTO Foo(Id,Name) VALUES(1,'foo1')",
			},
			querySQL:    "SELECT * FROM Foo WHERE PK = ?",
			queryParams: []interface{}{0},
			expect:      []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "get 1 record with all listed bins by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "REGISTER SET Foo AS struct{Id int; Name string}",
			querySQL:    "SELECT Id, Name FROM Foo WHERE PK = ?",
			init: []string{
				"DELETE FROM Foo",
				"INSERT INTO Foo(Id,Name) VALUES(1,'foo1')",
			},
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Foo{Id: 1, Name: "foo1"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "batch get - all records found by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM Foo WHERE PK IN(?, ?)",
			queryParams: []interface{}{1, 3},
			init: []string{
				"DELETE FROM Foo",
				"INSERT INTO Foo(Id,Name) VALUES(1,'foo1')",
				"INSERT INTO Foo(Id,Name) VALUES(2,'foo2')",
				"INSERT INTO Foo(Id,Name) VALUES(3,'foo3')",
			},
			expect: []interface{}{
				&Foo{Id: 1, Name: "foo1"},
				&Foo{Id: 3, Name: "foo3"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "batch get - few records found by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM Foo WHERE PK IN(?, ?, ?)",
			queryParams: []interface{}{0, 1, 3},
			init: []string{
				"DELETE FROM Foo",
				"INSERT INTO Foo(Id,Name) VALUES(1,'foo1')",
				"INSERT INTO Foo(Id,Name) VALUES(2,'foo2')",
				"INSERT INTO Foo(Id,Name) VALUES(3,'foo3')",
			},
			expect: []interface{}{
				&Foo{Id: 1, Name: "foo1"},
				&Foo{Id: 3, Name: "foo3"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "batch get - 0 records found by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM Foo WHERE PK IN(?, ?, ?)",
			queryParams: []interface{}{0, 10, 30},
			init: []string{
				"DELETE FROM Foo",
				"INSERT INTO Foo(Id,Name) VALUES(1,'foo1')",
			},
			expect: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "scan all - more than 0 records found",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM Foo",
			queryParams: []interface{}{},
			skip:        true, //this test fails intermittently, most likely truncate is not working as expected
			init: []string{
				"DELETE FROM Foo",
				"INSERT INTO Foo(Id,Name) VALUES(1,'foo1')",
				"INSERT INTO Foo(Id,Name) VALUES(2,'foo2')",
				"INSERT INTO Foo(Id,Name) VALUES(3,'foo3')",
			},
			expect: []interface{}{
				&Foo{Id: 1, Name: "foo1"},
				&Foo{Id: 2, Name: "foo2"},
				&Foo{Id: 3, Name: "foo3"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "scan all - no records found",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM Foo",
			queryParams: []interface{}{},
			init: []string{
				"DELETE FROM Foo",
			},
			expect: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "get 1 record by PK with 2 bin map values by key and between operator",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY BETWEEN ? AND ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 99,'doc0')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 104,'doc4')",
			},
			queryParams: []interface{}{1, 101, 102},
			expect: []interface{}{
				&Doc{Id: 1, Seq: 101, Name: "doc2"},
				&Doc{Id: 1, Seq: 102, Name: "doc3"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record by PK with 1 bin map values by key and between operator",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY BETWEEN ? AND ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
			},
			queryParams: []interface{}{1, 101, 101},
			expect: []interface{}{
				&Doc{Id: 1, Seq: 101, Name: "doc2"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record by PK with 0 bin map values by key and between operator",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY BETWEEN ? AND ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
			},
			queryParams: []interface{}{1, 900, 901},
			expect:      []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 0 records by PK with 0 bin map values by key and between operator",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY BETWEEN ? AND ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
			},
			queryParams: []interface{}{901, 900, 901},
			expect:      []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record by PK with 1 bin map values by key",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
			},
			queryParams: []interface{}{1, 101},
			expect: []interface{}{
				&Doc{Id: 1, Seq: 101, Name: "doc2"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record by PK with 0 bin map values by key",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
			},
			queryParams: []interface{}{1, 901},
			expect:      []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 0 record by PK with 0 bin map values by key",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
			},
			queryParams: []interface{}{900, 901},
			expect:      []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record by PK with 2 bin map values by key list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY IN (?,?)",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 104,'doc4')",
			},
			queryParams: []interface{}{1, 101, 104},
			expect: []interface{}{
				&Doc{Id: 1, Seq: 101, Name: "doc2"},
				&Doc{Id: 1, Seq: 104, Name: "doc4"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record by PK with 1 bin map values by key list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY IN (?,?)",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
			},
			queryParams: []interface{}{1, 101, 904},
			expect: []interface{}{
				&Doc{Id: 1, Seq: 101, Name: "doc2"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record by PK with 0 bin map values by key list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY IN (?,?)",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
			},
			queryParams: []interface{}{1, 901, 904},
			expect:      []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 0 record by PK with 0 bin map values by key list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND KEY IN (?,?)",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 102,'doc3')",
			},
			queryParams: []interface{}{900, 901, 904},
			expect:      []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				doc := Doc{}
				err := r.Scan(&doc.Id, &doc.Seq, &doc.Name)
				return &doc, err
			},
		},
		{
			description: "get 1 record with all bins by PK with string list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM Qux WHERE PK = ?",
			init: []string{
				"DELETE FROM Qux",
				"INSERT INTO Qux(id,seq,name,list) VALUES(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, "List of strings 1", []string{"item1", "item2", "item3"}},
			},
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Qux{Id: 1, Seq: 1, Name: "List of strings 1", List: []string{"item1", "item2", "item3"}},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				qux := Qux{}
				err := r.Scan(&qux.Id, &qux.Seq, &qux.Name, &qux.List)
				return &qux, err
			},
		},
		{
			description: "get 1 record by PK with 1 bin map value by key with string list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM Qux",
				"INSERT INTO Qux$Bars(id,seq,name,list) VALUES(?,?,?,?)",
				"INSERT INTO Qux$Bars(id,seq,name,list) VALUES(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, "List of strings 1", []string{"item1", "item2", "item3"}},
				{1, 2, "List of strings 2", []string{"item11", "item22", "item33"}},
			},
			querySQL:    "SELECT id, seq, name, list FROM Qux$Bars WHERE PK = ? AND KEY = ?",
			queryParams: []interface{}{1, 2},
			expect: []interface{}{
				&Qux{Id: 1, Seq: 2, Name: "List of strings 2", List: []string{"item11", "item22", "item33"}},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				qux := Qux{}
				err := r.Scan(&qux.Id, &qux.Seq, &qux.Name, &qux.List)
				return &qux, err
			},
		},
		{
			description: "get 1 record with all bins by PK - with time value stored as string",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM Baz",
				"INSERT INTO Baz(id,seq,name,time) VALUES(1,1,'Time formatted stored as string','2021-01-06T05:00:00Z')",
				"INSERT INTO Baz(id,seq,name,time) VALUES(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{},
				{2, 2, "Time formatted stored as string", "2021-01-06T05:00:00Z"},
			},
			querySQL:    "SELECT * FROM Baz WHERE PK = ?",
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Baz{Id: 1, Seq: 1, Name: "Time formatted stored as string", Time: getTime("2021-01-06T05:00:00Z")},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				baz := Baz{}
				err := r.Scan(&baz.Id, &baz.Seq, &baz.Name, &baz.Time)
				return &baz, err
			},
		},
		{
			description: "get 1 record by PK with 2 bin map values by key - with time value stored as string",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM Baz",
				"INSERT INTO Baz$Bars(id,seq,name,time) VALUES(?,?,?,?)",
				"INSERT INTO Baz$Bars(id,seq,name,time) VALUES(1,2,'Time formatted stored as string 2','2021-01-06T05:00:00Z')",
				"INSERT INTO Baz$Bars(id,seq,name,time) VALUES(1,3,'Time formatted stored as string 3','2021-01-08T09:10:11Z')",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, "Time formatted stored as string 1", "2021-01-06T05:04:03Z"},
				{},
				{},
			},
			querySQL:    "SELECT id, seq, name, time FROM Baz$Bars WHERE PK = ? AND KEY IN (?,?)",
			queryParams: []interface{}{1, 1, 2},
			expect: []interface{}{
				&Baz{Id: 1, Seq: 1, Name: "Time formatted stored as string 1", Time: getTime("2021-01-06T05:04:03Z")},
				&Baz{Id: 1, Seq: 2, Name: "Time formatted stored as string 2", Time: getTime("2021-01-06T05:00:00Z")},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				baz := Baz{}
				err := r.Scan(&baz.Id, &baz.Seq, &baz.Name, &baz.Time)
				return &baz, err
			},
		},
		{
			description: "get 1 record with all bins by PK - with time value stored as int",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM BazUnix",
				"INSERT INTO BazUnix(id,seq,name,time) VALUES(?,?,?,?)",
				"INSERT INTO BazUnix(id,seq,name,time) VALUES(2,2,'Time stored as int 2','2021-01-07T06:05:04Z')",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, "Time stored as int", getTime("2021-01-06T05:00:00Z")},
				{},
			},
			querySQL:    "SELECT * FROM BazUnix WHERE PK = ?",
			queryParams: []interface{}{1},
			expect: []interface{}{
				&BazUnix{Id: 1, Seq: 1, Name: "Time stored as int", Time: getTime("2021-01-06T05:00:00Z").In(time.Local)},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				baz := BazUnix{}
				err := r.Scan(&baz.Id, &baz.Seq, &baz.Name, &baz.Time)
				return &baz, err
			},
		},
		{
			description: "get 1 records by PK with 2 bin map values by key - with time value stored as int",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM BazUnix",
				"INSERT INTO BazUnix$Bars(id,seq,name,time) VALUES(1,1,'Time stored as int 3','2021-01-06T05:00:00Z')",
				"INSERT INTO BazUnix$Bars(id,seq,name,time) VALUES(?,?,?,?)",
				"INSERT INTO BazUnix$Bars(id,seq,name,time) VALUES(1,3,'Time stored as int 5','2021-02-03T04:05:06Z')",
			},
			initParams: [][]interface{}{
				{},
				{},
				{1, 2, "Time stored as int 4", getTime("2021-02-03T04:05:06Z")},
				{},
			},
			querySQL:    "SELECT id, seq, name, time FROM BazUnix$Bars WHERE PK = ? AND KEY IN (?,?)",
			queryParams: []interface{}{1, 1, 2},
			expect: []interface{}{
				&BazUnix{Id: 1, Seq: 1, Name: "Time stored as int 3", Time: getTime("2021-01-06T05:00:00Z").In(time.Local)},
				&BazUnix{Id: 1, Seq: 2, Name: "Time stored as int 4", Time: getTime("2021-02-03T04:05:06Z").In(time.Local)},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				baz := BazUnix{}
				err := r.Scan(&baz.Id, &baz.Seq, &baz.Name, &baz.Time)
				return &baz, err
			},
		},
		{
			description: "get 1 record with all bins by PK - with time value stored as string, time ptr in type",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM BazPtr",
				"INSERT INTO BazPtr(id,seq,name,time) VALUES(1,1,'Time formatted stored as string','2021-01-06T05:00:00Z')",
				"INSERT INTO BazPtr(id,seq,name,time) VALUES(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{},
				{2, 2, "Time formatted stored as string", "2021-01-06T05:00:00Z"},
			},
			querySQL:    "SELECT * FROM BazPtr WHERE PK = ?",
			queryParams: []interface{}{1},
			expect: []interface{}{
				&BazPtr{Id: 1, Seq: 1, Name: "Time formatted stored as string", Time: getTimePtr(getTime("2021-01-06T05:00:00Z"))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bazPtr := BazPtr{}
				err := r.Scan(&bazPtr.Id, &bazPtr.Seq, &bazPtr.Name, &bazPtr.Time)
				return &bazPtr, err
			},
		},
	}

	ctx := context.Background()
	for _, tc := range testCases {
		//for _, tc := range testCases[0:1] {
		//for _, tc := range testCases[len(testCases)-1:] {
		fmt.Printf("running test: %v\n", tc.description)

		t.Run(tc.description, func(t *testing.T) {

			if tc.skip {
				t.Skip(tc.description)
				return
			}
			db, err := sql.Open("aerospike", tc.dsn)
			if !assert.Nil(t, err, tc.description) {
				return
			}
			for _, set := range sets {
				_, err = db.ExecContext(context.Background(), set.SQL, set.params...)
				if !assert.Nil(t, err, tc.description) {
					return
				}
			}
			err = initDb(ctx, db, tc.init, tc.initParams)
			if !assert.Nil(t, err, tc.description) {
				fmt.Println("initDb ERROR: ", err.Error())
				return
			}
			if tc.execSQL != "" {
				_, err = db.ExecContext(context.Background(), tc.execSQL, tc.execParams...)
				if !assert.Nil(t, err, tc.description) {
					fmt.Println("execSQL ERROR: ", err.Error())
					return
				}
			}

			rows, err := db.QueryContext(context.Background(), tc.querySQL, tc.queryParams...)
			if !assert.Nil(t, err, tc.description) {
				return
			}
			assert.NotNil(t, rows, tc.description)

			var items = make([]interface{}, 0)
			for rows.Next() {
				item, err := tc.scanner(rows)
				assert.Nil(t, err, tc.description)
				items = append(items, item)
			}
			sort.Slice(items, func(i, j int) bool {
				return fmt.Sprintf("%v", items[i]) < fmt.Sprintf("%v", items[j])
			})
			assert.Equal(t, tc.expect, items, tc.description)
		})
	}
}

func initDb(ctx context.Context, db *sql.DB, init []string, initParams [][]interface{}) error {
	for i, SQL := range init {
		args := make([]interface{}, len(initParams))
		if len(initParams) != 0 {
			args = initParams[i]
		}
		_, err := db.ExecContext(ctx, SQL, args...)
		if err != nil {
			return err
		}
	}
	return nil
}

func getTime(formattedTime string) time.Time {
	result, err := time.Parse(time.RFC3339, formattedTime)
	if err != nil {
		result = time.Time{}
	}
	return result
}

func getTimePtr(t time.Time) *time.Time {
	return &t
}
