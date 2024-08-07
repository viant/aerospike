package aerospike

import (
	"context"
	"database/sql"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/stretchr/testify/assert"
	"log"
	"sort"
	"testing"
	"time"
)

type (
	testCase struct {
		description        string
		resetRegistry      bool
		truncateNamespaces bool
		dsn                string
		execSQL            string
		execParams         []interface{}
		querySQL           string
		init               []string
		initParams         [][]interface{}
		queryParams        []interface{}
		expect             interface{}
		sets               []*parameterizedQuery
		skip               bool
		scanner            func(r *sql.Rows) (interface{}, error)
		sleepSec           int
		justNActualRows    int
	}

	testCases          []*testCase
	parameterizedQuery struct {
		SQL    string
		params []interface{}
	}
)

func Test_Meta(t *testing.T) {
	namespace := "test"
	type catalog struct {
		CatalogName  string
		SchemaName   string
		SQLPath      string
		CharacterSet string
		Collation    string
	}

	type table struct {
		TableCatalog  string
		TableSchema   string
		TableName     string
		TableComment  string
		TableType     string
		AutoIncrement string
		CreateTime    string
		UpdateTime    string
		TableRows     int
		Version       string
		Engine        string
		DDL           string
	}

	type tableColumn struct {
		TableCatalog           string
		TableSchema            string
		TableName              string
		ColumnName             string
		OrdinalPosition        int
		ColumnComment          string
		DataType               string
		CharacterMaximumLength int
		NumericPrecision       int
		NumericScale           int
		IsNullable             string
		ColumnDefault          string
		ColumnKey              string
		IsAutoIncrement        int
	}

	type processlist struct {
		PID      string
		Username string
		Region   string
		Catalog  string
		Schema   string
		AppName  string
	}

	type version struct {
		Version string
	}

	var testCases = testCases{
		{
			description:   "metadata: all schemas - all namespaces in db",
			dsn:           "aerospike://127.0.0.1:3000/" + namespace,
			resetRegistry: true,
			querySQL: `select
		   '' catalog_name,
		   schema_name,
		   '' sql_path,
		   'utf8' default_character_set_name,
		   '' as default_collation_name
		   from information_schema.schemata`,
			queryParams: []interface{}{},
			expect: []interface{}{
				&catalog{CatalogName: "", SchemaName: "test", SQLPath: "", CharacterSet: "utf8", Collation: ""},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := catalog{}
				err := r.Scan(&rec.CatalogName, &rec.SchemaName, &rec.SQLPath, &rec.CharacterSet, &rec.Collation)
				return &rec, err
			},
		},
		{
			description:   "metadata: 1 schema - 1 namespaces from db",
			dsn:           "aerospike://127.0.0.1:3000/" + namespace,
			resetRegistry: true,
			querySQL: `select
'' catalog_name,
schema_name,
'' sql_path,
'utf8' default_character_set_name,
'' as default_collation_name
from information_schema.schemata
where pk = 'test'`,
			queryParams: []interface{}{},
			expect: []interface{}{
				&catalog{CatalogName: "", SchemaName: "test", SQLPath: "", CharacterSet: "utf8", Collation: ""},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := catalog{}
				err := r.Scan(&rec.CatalogName, &rec.SchemaName, &rec.SQLPath, &rec.CharacterSet, &rec.Collation)
				return &rec, err
			},
		},
		{
			description:        "metadata: all tables - all registered sets for current connection",
			dsn:                "aerospike://127.0.0.1:3000/" + namespace,
			resetRegistry:      true,
			truncateNamespaces: true,
			sets: []*parameterizedQuery{
				{SQL: "REGISTER SET A01 AS struct{Id int; Name string}"},
				{SQL: "REGISTER SET A02 AS struct{Id int; Name string}"},
				{SQL: "REGISTER SET A03 AS struct{Id int; Name string}"},
			},
			querySQL: `select
'' table_catalog,
table_schema,
table_name,
'' table_comment,
'' table_type,
'' as auto_increment,
'' create_time,
'' update_time,
0 table_rows,
'' version,
'' engine,
'' ddl
from information_schema.tables`,
			queryParams:     []interface{}{},
			justNActualRows: 2,
			expect: []interface{}{
				&table{TableCatalog: "", TableSchema: "test", TableName: "A01", TableComment: "", TableType: "", AutoIncrement: "", CreateTime: "", UpdateTime: "", TableRows: 0, Version: "", Engine: "", DDL: ""},
				&table{TableCatalog: "", TableSchema: "test", TableName: "A02", TableComment: "", TableType: "", AutoIncrement: "", CreateTime: "", UpdateTime: "", TableRows: 0, Version: "", Engine: "", DDL: ""},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := table{}
				err := r.Scan(&rec.TableCatalog, &rec.TableSchema, &rec.TableName, &rec.TableComment, &rec.TableType, &rec.AutoIncrement, &rec.CreateTime, &rec.UpdateTime, &rec.TableRows, &rec.Version, &rec.Engine, &rec.DDL)
				return &rec, err
			},
		},
		{
			description:        "metadata: 2 tables - 2 registered sets for current connection",
			dsn:                "aerospike://127.0.0.1:3000/" + namespace,
			resetRegistry:      true,
			truncateNamespaces: true,
			sets: []*parameterizedQuery{
				{SQL: "REGISTER SET A01 AS struct{Id int; Name string}"},
				{SQL: "REGISTER SET A02 AS struct{Id int; Name string}"},
				{SQL: "REGISTER SET A03 AS struct{Id int; Name string}"},
			},
			querySQL: `select
'' table_catalog,
table_schema,
table_name,
'' table_comment,
'' table_type,
'' as auto_increment,
'' create_time,
'' update_time,
0 table_rows,
'' version,
'' engine,
'' ddl
from information_schema.tables
where pk in ('A02','A03')`,
			queryParams: []interface{}{},
			expect: []interface{}{
				&table{TableCatalog: "", TableSchema: "test", TableName: "A02", TableComment: "", TableType: "", AutoIncrement: "", CreateTime: "", UpdateTime: "", TableRows: 0, Version: "", Engine: "", DDL: ""},
				&table{TableCatalog: "", TableSchema: "test", TableName: "A03", TableComment: "", TableType: "", AutoIncrement: "", CreateTime: "", UpdateTime: "", TableRows: 0, Version: "", Engine: "", DDL: ""},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := table{}
				err := r.Scan(&rec.TableCatalog, &rec.TableSchema, &rec.TableName, &rec.TableComment, &rec.TableType, &rec.AutoIncrement, &rec.CreateTime, &rec.UpdateTime, &rec.TableRows, &rec.Version, &rec.Engine, &rec.DDL)
				return &rec, err
			},
		},
		{
			description:        "metadata: all table columns - all registered set fields for current connection",
			dsn:                "aerospike://127.0.0.1:3000/" + namespace,
			resetRegistry:      true,
			truncateNamespaces: true,
			sets: []*parameterizedQuery{
				{SQL: "REGISTER SET A01 AS struct{Id int; Name string}"},
				{SQL: "REGISTER SET A02 AS struct{Id int; Name string}"},
			},
			querySQL: `select
		   '' table_catalog,
		   table_schema,
		   table_name,
		   column_name,
		   ordinal_position,
		   column_comment,
		   data_type,
		   character_maximum_length,
		   numeric_precision,
		   numeric_scale,
		   is_nullable,
		   column_default,
		   column_key,
		   is_autoincrement
		   from information_schema.columns`,
			queryParams:     []interface{}{},
			justNActualRows: 2, // !!!
			expect: []interface{}{
				&tableColumn{
					TableCatalog:           "",
					TableSchema:            "test",
					TableName:              "A01",
					ColumnName:             "Id",
					OrdinalPosition:        0,
					ColumnComment:          "",
					DataType:               "int",
					CharacterMaximumLength: 0,
					NumericPrecision:       0,
					NumericScale:           0,
					IsNullable:             "false",
					ColumnDefault:          "",
					ColumnKey:              "",
					IsAutoIncrement:        0,
				},
				&tableColumn{
					TableCatalog:           "",
					TableSchema:            "test",
					TableName:              "A01",
					ColumnName:             "Name",
					OrdinalPosition:        1,
					ColumnComment:          "",
					DataType:               "string",
					CharacterMaximumLength: 0,
					NumericPrecision:       0,
					NumericScale:           0,
					IsNullable:             "false",
					ColumnDefault:          "",
					ColumnKey:              "",
					IsAutoIncrement:        0,
				},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := tableColumn{}
				err := r.Scan(&rec.TableCatalog, &rec.TableSchema, &rec.TableName, &rec.ColumnName, &rec.OrdinalPosition, &rec.ColumnComment, &rec.DataType, &rec.CharacterMaximumLength, &rec.NumericPrecision, &rec.NumericScale, &rec.IsNullable, &rec.ColumnDefault, &rec.ColumnKey, &rec.IsAutoIncrement)
				return &rec, err
			},
		},
		{
			description:        "metadata: table columns for 2 tables - fields for 2 sets for current connection",
			dsn:                "aerospike://127.0.0.1:3000/" + namespace,
			resetRegistry:      true,
			truncateNamespaces: true,
			sets: []*parameterizedQuery{
				{SQL: "REGISTER SET A01 AS struct{Id int; Name string}"},
				{SQL: "REGISTER SET A02 AS struct{Id int; Name string}"},
			},
			querySQL: `select
		   '' table_catalog,
		   table_schema,
		   table_name,
		   column_name,
		   ordinal_position,
		   column_comment,
		   data_type,
		   character_maximum_length,
		   numeric_precision,
		   numeric_scale,
		   is_nullable,
		   column_default,
		   column_key,
		   is_autoincrement
		   from information_schema.columns
		   where pk = 'A02'`,
			queryParams:     []interface{}{},
			justNActualRows: 2, //TODO
			expect: []interface{}{
				&tableColumn{
					TableCatalog:           "",
					TableSchema:            "test",
					TableName:              "A02",
					ColumnName:             "Id",
					OrdinalPosition:        0,
					ColumnComment:          "",
					DataType:               "int",
					CharacterMaximumLength: 0,
					NumericPrecision:       0,
					NumericScale:           0,
					IsNullable:             "false",
					ColumnDefault:          "",
					ColumnKey:              "",
					IsAutoIncrement:        0,
				},
				&tableColumn{
					TableCatalog:           "",
					TableSchema:            "test",
					TableName:              "A02",
					ColumnName:             "Name",
					OrdinalPosition:        1,
					ColumnComment:          "",
					DataType:               "string",
					CharacterMaximumLength: 0,
					NumericPrecision:       0,
					NumericScale:           0,
					IsNullable:             "false",
					ColumnDefault:          "",
					ColumnKey:              "",
					IsAutoIncrement:        0,
				},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := tableColumn{}
				err := r.Scan(&rec.TableCatalog, &rec.TableSchema, &rec.TableName, &rec.ColumnName, &rec.OrdinalPosition, &rec.ColumnComment, &rec.DataType, &rec.CharacterMaximumLength, &rec.NumericPrecision, &rec.NumericScale, &rec.IsNullable, &rec.ColumnDefault, &rec.ColumnKey, &rec.IsAutoIncrement)
				return &rec, err
			},
		},
		{
			description:        "metadata: session",
			dsn:                "aerospike://127.0.0.1:3000/" + namespace,
			resetRegistry:      true,
			truncateNamespaces: true,
			sets: []*parameterizedQuery{
				{SQL: "REGISTER SET A01 AS struct{Id int; Name string}"},
				{SQL: "REGISTER SET A02 AS struct{Id int; Name string}"},
			},
			querySQL: `select
pid,
user_name,
region,
catalog_name,
schema_name,
app_name
from information_schema.processlist`,
			queryParams: []interface{}{},
			expect: []interface{}{
				&processlist{
					PID:      "0",
					Username: "test_name",
					Region:   "",
					Catalog:  "",
					Schema:   "test",
					AppName:  "",
				},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := processlist{}
				err := r.Scan(&rec.PID, &rec.Username, &rec.Region, &rec.Catalog, &rec.Schema, &rec.AppName)
				return &rec, err
			},
		},
		{
			description:   "metadata: version",
			dsn:           "aerospike://127.0.0.1:3000/" + namespace,
			resetRegistry: true,
			querySQL:      `select version from information_schema.serverinfo`,
			queryParams:   []interface{}{},
			//expect:        []interface{}{&version{Version: "Aerospike Community Edition build 6.2.0.2"}},
			expect: []interface{}{&version{Version: "Aerospike 6.0.0.0"}},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := version{}
				err := r.Scan(&rec.Version)
				return &rec, err
			},
		},
	}

	testCases.runTest(t)

}

func Test_ExecContext(t *testing.T) {
	namespace := "test"

	type Foo struct {
		Id   int
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
			sql:         "REGISTER SET Bar AS struct{id int; name string}", //TODO is this struct usable when all fields are private?
		},
		{
			description: "register named set",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			sql:         "REGISTER SET Foo AS ?",
			params:      []interface{}{Foo{}},
		},
		{
			description: "register inlined global set",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			sql:         "REGISTER GLOBAL SET Bar AS struct{id int; name string}",
		},
		{
			description: "register named global set",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			sql:         "REGISTER GLOBAL SET Foo AS ?",
			params:      []interface{}{Foo{}},
		},
		{
			description: "register named global set with ttl",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			sql:         "REGISTER GLOBAL SET WITH TTL 100 Foo AS ?",
			params:      []interface{}{Foo{}},
		},
		{
			description: "register inlined global set with ttl",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			sql:         "REGISTER GLOBAL SET WITH TTL 100 Bar AS struct{id int; name string}",
		},
	}

	for _, tc := range testCase {
		//for _, tc := range testCase[0:1] {
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

	type Message struct {
		Id   int    `aerospike:"id,pk=true" `
		Seq  int    `aerospike:"seq,listKey=true" `
		Body string `aerospike:"body"`
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

	type BazDoublePtr struct {
		Id   int         `aerospike:"id,pk=true"`
		Seq  int         `aerospike:"seq,key=true" `
		Name string      `aerospike:"name"`
		Time **time.Time `aerospike:"time"`
	}

	type BazUnix struct {
		Id   int       `aerospike:"id,pk=true"`
		Seq  int       `aerospike:"seq,key=true" `
		Name string    `aerospike:"name"`
		Time time.Time `aerospike:"time,unixsec"`
	}

	type BazUnixPtr struct {
		Id   int        `aerospike:"id,pk=true"`
		Seq  int        `aerospike:"seq,key=true" `
		Name string     `aerospike:"name"`
		Time *time.Time `aerospike:"time,unixsec"`
	}

	type BazUnixDoublePtr struct {
		Id   int         `aerospike:"id,pk=true"`
		Seq  int         `aerospike:"seq,key=true" `
		Name string      `aerospike:"name"`
		Time **time.Time `aerospike:"time,unixsec"`
	}

	type Qux struct {
		Id    int      `aerospike:"id,pk=true"`
		Seq   int      `aerospike:"seq,key=true"`
		Name  string   `aerospike:"name"`
		List  []string `aerospike:"list"`
		Slice []string `aerospike:"slice"`
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

	type Abc struct {
		Id   int
		Name string
	}

	type User struct {
		UID    string `aerospike:"uid,pk"`
		Email  string `aerospike:"email,index"`
		Active bool   `aerospike:"active"`
	}

	var sets = []*parameterizedQuery{
		{SQL: "REGISTER SET Doc AS struct{Id int; Seq int `aerospike:\"seq,key=true\"`;  Name string}"},
		{SQL: "REGISTER SET Foo AS ?", params: []interface{}{Foo{}}},
		{SQL: "REGISTER SET SimpleAgg AS ?", params: []interface{}{SimpleAgg{}}},
		{SQL: "REGISTER SET Agg AS ?", params: []interface{}{Agg{}}},
		{SQL: "REGISTER SET Baz AS ?", params: []interface{}{Baz{}}},
		{SQL: "REGISTER SET BazUnix AS ?", params: []interface{}{BazUnix{}}},
		{SQL: "REGISTER SET BazUnixPtr AS ?", params: []interface{}{BazUnixPtr{}}},
		{SQL: "REGISTER SET BazUnixDoublePtr AS ?", params: []interface{}{BazUnixDoublePtr{}}},
		{SQL: "REGISTER SET Qux AS ?", params: []interface{}{Qux{}}},
		{SQL: "REGISTER SET BazPtr AS ?", params: []interface{}{BazPtr{}}},
		{SQL: "REGISTER SET BazDoublePtr AS ?", params: []interface{}{BazDoublePtr{}}},
		{SQL: "REGISTER SET Msg AS ?", params: []interface{}{Message{}}},
		{SQL: "REGISTER SET WITH TTL 2 Abc AS struct{Id int; Name string}"},
		{SQL: "REGISTER SET users AS ?", params: []interface{}{User{}}},
	}

	type CountRec struct {
		Count int
	}

	var testCases = testCases{
		{
			description: "secondary index ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM users WHERE email = ?",
			queryParams: []interface{}{"xxx@test.io"},
			init: []string{
				"DROP INDEX IF EXISTS UserEmail ON " + namespace + ".users",
				"CREATE STRING INDEX UserEmail ON " + namespace + ".users(email)",
				"DELETE FROM users",
			},
			execSQL:    "INSERT INTO users(uid,email,active) VALUES(?,?,?),(?,?,?)",
			execParams: []interface{}{"1", "me@cpm.org", true, "2", "xxx@test.io", false},
			expect:     []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := SimpleAgg{}
				err := r.Scan(&agg.Id, &agg.Amount)
				return &agg, err
			},
		},
		{
			description: "get 1 record with all bins by PK with string list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM Qux WHERE PK IN(?,?)",
			init: []string{
				"DELETE FROM Qux",
				"INSERT INTO Qux(id,seq,name,list) VALUES(?,?,?,?),(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, "List of strings 1", []string{"item1", "item2", "item3"}, 2, 1, "List of strings 2", nil},
			},
			queryParams: []interface{}{1, 2},
			expect: []interface{}{
				&Qux{Id: 1, Seq: 1, Name: "List of strings 1", List: []string{"item1", "item2", "item3"}},
				&Qux{Id: 2, Seq: 1, Name: "List of strings 2", List: nil},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				qux := Qux{}
				err := r.Scan(&qux.Id, &qux.Seq, &qux.Name, &qux.List, &qux.Slice)
				return &qux, err
			},
		},
		{
			description: "nested sql ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id FROM (SELECT * FROM SimpleAgg WHERE 1 = 0)",
			init: []string{
				"DELETE FROM SimpleAgg",
			},
			expect: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := SimpleAgg{}
				err := r.Scan(&agg.Id, &agg.Amount)
				return &agg, err
			},
		},
		{
			description: "false predicate ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM SimpleAgg WHERE 1 = 0",
			init: []string{
				"DELETE FROM SimpleAgg",
			},
			expect: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := SimpleAgg{}
				err := r.Scan(&agg.Id, &agg.Amount)
				return &agg, err
			},
		},
		{
			description: "false predicate ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM SimpleAgg WHERE 1 = 0",
			init: []string{
				"DELETE FROM SimpleAgg",
			},
			expect: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := SimpleAgg{}
				err := r.Scan(&agg.Id, &agg.Amount)
				return &agg, err
			},
		},
		{
			description: "insert record - with pointer params",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Foo(Id,Name) VALUES(?,?)",
			execParams:  []interface{}{intPtr(1), stringPtr("foo inserted")},
			querySQL:    "SELECT * FROM Foo WHERE PK = ?",
			init: []string{
				"DELETE FROM Foo",
			},
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Foo{Id: 1, Name: "foo inserted"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		{
			description: "count",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Msg$Items(id,body) VALUES(?,?),(?,?),(?,?),(?,?)",
			execParams:  []interface{}{1, "test message", 1, "another message", 1, "last message", 1, "eee"},

			querySQL:    "SELECT COUNT(*) FROM Msg$Items WHERE PK = ?",
			queryParams: []interface{}{1},
			init: []string{
				"DELETE FROM Msg",
			},
			expect: []interface{}{
				&CountRec{Count: 4},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := CountRec{}
				err := r.Scan(&rec.Count)
				return &rec, err
			},
		},
		{
			description: "list insert with index criteria",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Msg$Items(id,body) VALUES(?,?),(?,?),(?,?)",
			execParams:  []interface{}{1, "test message", 1, "another message", 1, "last message"},

			querySQL:    "SELECT id,seq,body FROM Msg$Items WHERE PK = ? AND KEY IN(?,?)",
			queryParams: []interface{}{1, 0, 2},
			init: []string{
				"DELETE FROM Msg",
			},
			expect: []interface{}{
				&Message{Id: 1, Seq: 0, Body: "test message"},
				&Message{Id: 1, Seq: 2, Body: "last message"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				msg := Message{}
				err := r.Scan(&msg.Id, &msg.Seq, &msg.Body)
				return &msg, err
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
		{
			description: "list insert",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Msg$Items(id,body) VALUES(?,?),(?,?)",
			execParams:  []interface{}{1, "test message", 1, "another message"},

			querySQL:    "SELECT id,seq,body FROM Msg$Items WHERE PK = ?",
			queryParams: []interface{}{1},
			init: []string{
				"DELETE FROM Msg",
			},
			expect: []interface{}{
				&Message{Id: 1, Seq: 0, Body: "test message"},
				&Message{Id: 1, Seq: 1, Body: "another message"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				msg := Message{}
				err := r.Scan(&msg.Id, &msg.Seq, &msg.Body)
				return &msg, err
			},
		},
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
		{
			description: "insert record with ttl",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execParams:  []interface{}{1},
			querySQL:    "SELECT * FROM Abc WHERE PK = ?",
			init: []string{
				"DELETE FROM Abc",
				"INSERT INTO Abc(Id,Name) VALUES(1,'abc1')",
			},
			queryParams: []interface{}{1},
			expect:      []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				abc := Abc{}
				err := r.Scan(&abc.Id, &abc.Name)
				return &abc, err
			},
			sleepSec: 3,
		},
		{
			description: "get 1 record with all bins by PK - with time value stored as string, time double ptr in type",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM BazDoublePtr",
				"INSERT INTO BazDoublePtr(id,seq,name,time) VALUES(1,1,'Time formatted stored as string','2021-01-06T05:00:00Z')",
				"INSERT INTO BazDoublePtr(id,seq,name,time) VALUES(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{},
				{2, 2, "Time formatted stored as string", "2021-01-06T05:00:00Z"},
			},
			querySQL:    "SELECT * FROM BazDoublePtr WHERE PK = ?",
			queryParams: []interface{}{1},
			expect: []interface{}{
				&BazDoublePtr{Id: 1, Seq: 1, Name: "Time formatted stored as string", Time: getTimeDoublePtr(getTime("2021-01-06T05:00:00Z"))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bazPtr := BazDoublePtr{}
				err := r.Scan(&bazPtr.Id, &bazPtr.Seq, &bazPtr.Name, &bazPtr.Time)
				return &bazPtr, err
			},
		},

		{
			description: "get 1 record with all bins by PK - with time value stored as int, time ptr in type",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM BazUnixPtr",
				"INSERT INTO BazUnixPtr(id,seq,name,time) VALUES(?,?,?,?)",
				"INSERT INTO BazUnixPtr(id,seq,name,time) VALUES(2,2,'Time stored as int 2','2021-01-07T06:05:04Z')",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, "Time stored as int", getTimePtr(getTime("2021-01-06T05:00:00Z"))},
				{},
			},
			querySQL:    "SELECT * FROM BazUnixPtr WHERE PK = ?",
			queryParams: []interface{}{1},
			expect: []interface{}{
				&BazUnixPtr{Id: 1, Seq: 1, Name: "Time stored as int", Time: getTimePtr(getTime("2021-01-06T05:00:00Z").In(time.Local))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				baz := BazUnixPtr{}
				err := r.Scan(&baz.Id, &baz.Seq, &baz.Name, &baz.Time)
				return &baz, err
			},
		},
		{
			description: "get 1 record with all bins by PK - with time value stored as int, time double ptr in type",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM BazUnixDoublePtr",
				"INSERT INTO BazUnixDoublePtr(id,seq,name,time) VALUES(?,?,?,?)",
				"INSERT INTO BazUnixDoublePtr(id,seq,name,time) VALUES(2,2,'Time stored as int 2','2021-01-07T06:05:04Z')",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, "Time stored as int", getTimePtr(getTime("2021-01-06T05:00:00Z"))},
				{},
			},
			querySQL:    "SELECT * FROM BazUnixDoublePtr WHERE PK = ?",
			queryParams: []interface{}{1},
			expect: []interface{}{
				&BazUnixDoublePtr{Id: 1, Seq: 1, Name: "Time stored as int", Time: getTimeDoublePtr(getTime("2021-01-06T05:00:00Z").In(time.Local))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				baz := BazUnixDoublePtr{}
				err := r.Scan(&baz.Id, &baz.Seq, &baz.Name, &baz.Time)
				return &baz, err
			},
		},
	}
	for _, tc := range testCases {
		if len(tc.sets) == 0 {
			tc.sets = sets
		}
	}

	//	testCases = testCases[0:1] //TODO DELETE

	testCases.runTest(t)
}

func (s testCases) runTest(t *testing.T) {
	ctx := context.Background()
	for _, tc := range s {

		fmt.Printf("running test: %v\n", tc.description)
		t.Run(tc.description, func(t *testing.T) {

			if tc.truncateNamespaces {
				err := truncateNamespace(tc.dsn)
				if !assert.Nil(t, err, tc.description) {
					return
				}
			}

			if tc.skip {
				t.Skip(tc.description)
				return
			}
			if tc.resetRegistry {
				globalSets.clear()
			}
			db, err := sql.Open("aerospike", tc.dsn)
			if !assert.Nil(t, err, tc.description) {
				return
			}
			for _, set := range tc.sets {
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

			if tc.sleepSec > 0 {
				time.Sleep(time.Duration(tc.sleepSec) * time.Second)
			}

			rows, err := db.QueryContext(context.Background(), tc.querySQL, tc.queryParams...)
			if !assert.Nil(t, err, tc.description) {
				return
			}
			assert.NotNil(t, rows, tc.description)

			var actual = make([]interface{}, 0)
			for rows.Next() {
				item, err := tc.scanner(rows)
				if !assert.Nil(t, err, tc.description) {
					return
				}
				actual = append(actual, item)
			}
			sort.Slice(actual, func(i, j int) bool {
				return fmt.Sprintf("%v", actual[i]) < fmt.Sprintf("%v", actual[j])
			})

			if tc.justNActualRows != 0 {
				if len(actual) >= tc.justNActualRows {
					actual = actual[:tc.justNActualRows]
				}
			}

			assert.Equal(t, tc.expect, actual, tc.description)
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

func getTimeDoublePtr(t time.Time) **time.Time {
	ptr := &t
	return &ptr
}

func truncateNamespace(dsn string) error {
	cfg, err := ParseDSN(dsn)
	client, err := as.NewClientWithPolicy(nil, cfg.host, cfg.port)
	if err != nil {
		return fmt.Errorf("failed to connect to aerospike with dsn %s due to: %v", dsn, err)
	}

	defer client.Close()

	// Define the namespace
	namespace := cfg.namespace

	err = client.Truncate(nil, namespace, "", nil)
	if err != nil {
		log.Fatalf("Failed to truncate set %s: %v", "", err)
	}
	fmt.Printf("Truncated set %s in namespace %s\n", "", namespace)

	return err
}

// /
// Define a type for a slice of tables
type tableSlice []table

// Implement the sort.Interface for tableSlice

// Len is the number of elements in the collection.
func (t tableSlice) Len() int {
	return len(t)
}

// Swap swaps the elements with indexes i and j.
func (t tableSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (t tableSlice) Less(i, j int) bool {
	return t[i].TableName < t[j].TableName
}

func modifyActual(src []interface{}) (interface{}, error) {
	var result []*table
	for _, item := range src {
		t, ok := item.(*table)
		if !ok {
			return nil, fmt.Errorf("expected type %T, got %T", t, item)
		}
	}
	return result, nil
}

func intPtr(i int) *int {
	return &i
}

func stringPtr(s string) *string {
	return &s
}
