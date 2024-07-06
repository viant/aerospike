package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
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

// ID or PK option for that
/*
	SELECT * FROM Foo$ WHERE PK = 1
	SELECT * FROM Foo$Bin WHERE PK = 'PK'
	SELECT Foo$Bin FROM Foo WHERE PK = 'PK'
	SELECT * FROM Foo$MapBin WHERE PK = 'PK'
	SELECT * FROM Foo$MapBin WHERE PK = 'value'
	SELECT * FROM Foo$MapBin WHERE PK = 'value' AND KEY = 'key1'

	SELECT * FROM Foo, UNNEST(Foo.MapBin) WHERE PK = 'value' AND KEY = 'key1'
	SELECT PK.Bins.* FROM Foo$MapBin Bins WHERE PK = 'value' AND KEY = 'key1
*/

func Test_QueryContext(t *testing.T) {
	namespace := "test"
	//
	//seqKey := aerospike.CtxMapKey(aerospike.NewIntegerValue(101))
	//ops := []*aerospike.Operation{
	//	aerospike.MapIncrementOp(aerospike.DefaultMapPolicy(), "Bars", aerospike.NewStringValue("seq"), 1, seqKey),
	//}
	//
	////		record, err = client.Operate(wpolicy, key, as.MapPutOp(as.DefaultMapPolicy(), cdtBinName, as.StringValue("key21"), as.IntegerValue(11), as.CtxMapKey(as.StringValue("key2"))), as.GetOpForBin(cdtBinName))
	//key, _ = aerospike.NewKey("test", "Doc", 1)
	//_, err = client.Operate(nil, key, ops...)
	//if err != nil {
	//	//log.Fatal(err)
	//}

	type Foo struct {
		Id   int //`aql:"id,key=true"
		Name string
	}

	type Doc struct {
		Id   int    `aerospike:"id,pk=true" `
		Seq  int    `aerospike:"seq,key=true" `
		Name string `aerospike:"name" `
	}

	var testCases = []struct {
		description string
		dsn         string
		execSQL     string
		execParams  []interface{}
		querySQL    string
		init        []string
		queryParams []interface{}
		expect      interface{}
		scanner     func(r *sql.Rows) (interface{}, error)
	}{
		{
			description: "get 1 record with all bins by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "REGISTER SET Doc AS struct{Id int; Seq int `aerospike:\"seq,key=true\"`;  Name string}",
			querySQL:    "SELECT id, seq, name FROM Doc$Bars WHERE PK = ? AND Key = 101",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc$Bars(id, seq, name) VALUES(1, 101,'doc2')",
			},
			queryParams: []interface{}{1},
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
			description: "get 1 record with all bins by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "REGISTER SET Doc AS struct{Id int; Seq int `aerospike:\"seq,key=true\"`;  Name string}",
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
			execSQL:     "REGISTER SET Foo AS struct{Id int; Name string}",
			querySQL:    "SELECT * FROM Foo WHERE PK = ?",
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
			execSQL:     "REGISTER SET Foo AS ?",
			execParams:  []interface{}{Foo{}},
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
			execSQL:     "REGISTER SET Foo AS ?",
			execParams:  []interface{}{Foo{}},
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
			execSQL:     "REGISTER SET Foo AS ?",
			execParams:  []interface{}{Foo{}},
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
			description: "get 1 record with all bins by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "REGISTER SET Foo AS struct{Id int; Name string}",
			querySQL:    "SELECT * FROM Foo WHERE PK = ?",
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
			description: "batch get - 0 records found by PK",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "REGISTER SET Foo AS ?",
			execParams:  []interface{}{Foo{}},
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
			execSQL:     "REGISTER SET Foo AS ?",
			execParams:  []interface{}{Foo{}},
			querySQL:    "SELECT * FROM Foo",
			queryParams: []interface{}{},
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
			execSQL:     "REGISTER SET Foo AS ?",
			execParams:  []interface{}{Foo{}},
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
	}

	ctx := context.Background()
	for _, tc := range testCases[:1] {
		//for _, tc := range testCases[len(testCases)-1:] {
		t.Run(tc.description, func(t *testing.T) {

			db, err := sql.Open("aerospike", tc.dsn)
			if !assert.Nil(t, err, tc.description) {
				return
			}

			if tc.execSQL != "" {
				_, err = db.ExecContext(context.Background(), tc.execSQL, tc.execParams...)
				assert.Nil(t, err, tc.description)
			}
			err = initDb(ctx, db, tc.init)
			if !assert.Nil(t, err, tc.description) {
				return
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

func initDb(ctx context.Context, db *sql.DB, init []string) error {
	for _, SQL := range init {
		_, err := db.ExecContext(ctx, SQL)
		if err != nil {
			return err
		}
	}
	return nil
}

type entry struct {
	pk     interface{}
	set    string
	binMap map[string]interface{}
}
