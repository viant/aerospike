package aerospike

import (
	"context"
	"database/sql"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"log"
	"sort"
	"testing"
	"time"
)

func boolPtr(b bool) *bool {
	return &b
}

var namespace = "ns_memory"

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
		allowExecError     bool
	}

	tstCases           []*testCase
	parameterizedQuery struct {
		SQL    string
		params []interface{}
	}
)

func Test_Meta(t *testing.T) {
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

	var testCases = tstCases{
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
				&catalog{CatalogName: "", SchemaName: namespace, SQLPath: "", CharacterSet: "utf8", Collation: ""},
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
where pk = '` + namespace + `'`,
			queryParams: []interface{}{},
			expect: []interface{}{
				&catalog{CatalogName: "", SchemaName: namespace, SQLPath: "", CharacterSet: "utf8", Collation: ""},
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
				&table{TableCatalog: "", TableSchema: namespace, TableName: "A01", TableComment: "", TableType: "", AutoIncrement: "", CreateTime: "", UpdateTime: "", TableRows: 0, Version: "", Engine: "", DDL: ""},
				&table{TableCatalog: "", TableSchema: namespace, TableName: "A02", TableComment: "", TableType: "", AutoIncrement: "", CreateTime: "", UpdateTime: "", TableRows: 0, Version: "", Engine: "", DDL: ""},
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
				&table{TableCatalog: "", TableSchema: namespace, TableName: "A02", TableComment: "", TableType: "", AutoIncrement: "", CreateTime: "", UpdateTime: "", TableRows: 0, Version: "", Engine: "", DDL: ""},
				&table{TableCatalog: "", TableSchema: namespace, TableName: "A03", TableComment: "", TableType: "", AutoIncrement: "", CreateTime: "", UpdateTime: "", TableRows: 0, Version: "", Engine: "", DDL: ""},
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
					TableSchema:            namespace,
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
					TableSchema:            namespace,
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
					TableSchema:            namespace,
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
					TableSchema:            namespace,
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
					Schema:   namespace,
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

	//testCases = testCases[0:1]

	testCases.runTest(t)

}

func Test_ExecContext(t *testing.T) {

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
	type Foo struct {
		Id   int
		Name string
	}

	type Foo2 struct {
		Id     int
		Name   string
		Actual bool
	}

	type Message struct {
		Id   int    `aerospike:"id,pk=true" `
		Seq  int    `aerospike:"seq,arrayindex" `
		Body string `aerospike:"body"`
	}
	type Baz struct {
		Id   int       `aerospike:"id,pk=true"`
		Seq  int       `aerospike:"seq,mapKey" `
		Name string    `aerospike:"name"`
		Time time.Time `aerospike:"time"`
	}

	type BazPtr struct {
		Id   int        `aerospike:"id,pk=true"`
		Seq  int        `aerospike:"seq,mapKey" `
		Name string     `aerospike:"name"`
		Time *time.Time `aerospike:"time"`
	}

	type BazDoublePtr struct {
		Id   int         `aerospike:"id,pk=true"`
		Seq  int         `aerospike:"seq,mapKey" `
		Name string      `aerospike:"name"`
		Time **time.Time `aerospike:"time"`
	}

	type BazUnix struct {
		Id   int       `aerospike:"id,pk=true"`
		Seq  int       `aerospike:"seq,mapKey" `
		Name string    `aerospike:"name"`
		Time time.Time `aerospike:"time,unixsec"`
	}

	type BazUnixPtr struct {
		Id   int        `aerospike:"id,pk=true"`
		Seq  int        `aerospike:"seq,mapKey" `
		Name string     `aerospike:"name"`
		Time *time.Time `aerospike:"time,unixsec"`
	}

	type BazUnixDoublePtr struct {
		Id   int         `aerospike:"id,pk=true"`
		Seq  int         `aerospike:"seq,mapKey" `
		Name string      `aerospike:"name"`
		Time **time.Time `aerospike:"time,unixsec"`
	}

	type Qux struct {
		Id    int      `aerospike:"id,pk=true"`
		Seq   int      `aerospike:"seq,mapKey"`
		Name  string   `aerospike:"name"`
		List  []string `aerospike:"list"`
		Slice []string `aerospike:"slice"`
	}

	type Doc struct {
		Id   int    `aerospike:"id,pk=true" `
		Seq  int    `aerospike:"seq,mapKey" `
		Name string `aerospike:"name" `
	}

	type SimpleAgg struct {
		Id     int `aerospike:"id,pk=true" `
		Amount int `aerospike:"amount" `
	}
	type Agg struct {
		Id     int `aerospike:"id,pk=true" `
		Seq    int `aerospike:"seq,mapKey" `
		Amount int `aerospike:"amount" `
		Val    int `aerospike:"val" `
	}

	type Abc struct {
		Id   int
		Name string
	}

	type Abc2 struct {
		Id   int
		Name string
		Desc string `aerospike:"-"`
	}

	type User struct {
		UID    string `aerospike:"uid,pk"`
		Email  string `aerospike:"email,secondaryIndex"`
		Active bool   `aerospike:"active"`
	}

	type User2 struct {
		UID    string `aerospike:"uid,pk"`
		Email  string `aerospike:"email,secondaryIndex"`
		Active *bool  `aerospike:"active"`
	}

	type Bar struct {
		Id     int       `aerospike:"id,pk=true"`
		Seq    int       `aerospike:"seq,mapKey"`
		Amount int       `aerospike:"amount"`
		Price  float64   `aerospike:"price"`
		Name   string    `aerospike:"name"`
		Time   time.Time `aerospike:"time"`
	}

	type BarPtr struct {
		Id     int        `aerospike:"id,pk=true"`
		Seq    int        `aerospike:"seq,mapKey"`
		Amount *int       `aerospike:"amount"`
		Price  *float64   `aerospike:"price"`
		Name   *string    `aerospike:"name"`
		Time   *time.Time `aerospike:"time"`
	}

	type BarDoublePtr struct {
		Id     int         `aerospike:"id,pk=true"`
		Seq    int         `aerospike:"seq,mapKey"`
		Amount **int       `aerospike:"amount"`
		Price  **float64   `aerospike:"price"`
		Name   **string    `aerospike:"name"`
		Time   **time.Time `aerospike:"time"`
	}

	type Signal struct {
		ID       string      `aerospike:"id,pk=true"` //--> day,value
		KeyValue interface{} `aerospike:"keyValue,mapKey"`
		Bucket   int         `aerospike:"bucket,arrayIndex,arraySize=5"`
		Count    int         `aerospike:"count,component"`
	}

	type Signal2 struct {
		ID       string      `aerospike:"id,pk=true"` //--> day,value
		KeyValue interface{} `aerospike:"keyValue,mapKey"`
		Bucket   int         `aerospike:"bucket,arrayIndex,arraySize=2"`
		Count    int         `aerospike:"count,component"`
	}

	type AuthCode struct {
		Code     string `aerospike:"code,pk" sqlx:"code,primarykey" json:"code,omitempty"` // the random auth code value
		ClientId string `aerospike:"client_id" json:"client_id,omitempty"`                 // OAuth client that requested it
		UserId   string `aerospike:"user_id" json:"user_id,omitempty"`                     // the authenticated user's ID (sub)
	}

	var sets = []*parameterizedQuery{
		{SQL: "REGISTER SET Signal2 AS ?", params: []interface{}{Signal2{}}},
		{SQL: "REGISTER SET Signal AS ?", params: []interface{}{Signal{}}},
		{SQL: "REGISTER SET Agg/Values AS ?", params: []interface{}{Agg{}}},
		{SQL: "REGISTER SET Doc AS ?", params: []interface{}{Doc{}}},
		{SQL: "REGISTER SET Foo AS ?", params: []interface{}{Foo{}}},
		{SQL: "REGISTER SET Foo2 AS ?", params: []interface{}{Foo2{}}},
		{SQL: "REGISTER SET Baz AS ?", params: []interface{}{Baz{}}},
		{SQL: "REGISTER SET SimpleAgg AS ?", params: []interface{}{SimpleAgg{}}},
		{SQL: "REGISTER SET BazUnix AS ?", params: []interface{}{BazUnix{}}},
		{SQL: "REGISTER SET BazUnixPtr AS ?", params: []interface{}{BazUnixPtr{}}},
		{SQL: "REGISTER SET BazUnixDoublePtr AS ?", params: []interface{}{BazUnixDoublePtr{}}},
		{SQL: "REGISTER SET Qux AS ?", params: []interface{}{Qux{}}},
		{SQL: "REGISTER SET BazPtr AS ?", params: []interface{}{BazPtr{}}},
		{SQL: "REGISTER SET BazDoublePtr AS ?", params: []interface{}{BazDoublePtr{}}},
		{SQL: "REGISTER SET Msg AS ?", params: []interface{}{Message{}}},
		{SQL: "REGISTER SET WITH TTL 2 Abc AS struct{Id int; Name string}"},
		{SQL: "REGISTER SET users AS ?", params: []interface{}{User{}}},
		{SQL: "REGISTER SET users2 AS ?", params: []interface{}{User2{}}},
		{SQL: "REGISTER SET Abc2 AS ?", params: []interface{}{Abc2{}}},
		{SQL: "REGISTER SET bar AS ?", params: []interface{}{Bar{}}},
		{SQL: "REGISTER SET barPtr AS ?", params: []interface{}{BarPtr{}}},
		{SQL: "REGISTER SET barDoublePtr AS struct { Id int `aerospike:\"id,pk=true\"`; Seq int `aerospike:\"seq,mapKey\"`; Amount **int `aerospike:\"amount\"`; Price **float64 `aerospike:\"price\"`; Name **string `aerospike:\"name\"`; Time **time.Time `aerospike:\"time\"` }", params: []interface{}{}},
		{SQL: "REGISTER SET authCode AS ?", params: []interface{}{AuthCode{}}},
	}

	type CountRecGroup struct {
		ID    int
		Count int
	}

	type CountRec struct {
		Count int
	}

	var testCases = tstCases{
		{
			description: "array batch merge insert stmt cache check",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id,keyValue,bucket,count FROM Signal2/Values WHERE pk = ?",
			queryParams: []interface{}{"1"},
			init: []string{
				"TRUNCATE TABLE Signal2 ",
				"INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?)",
				"INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?)",
				"INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?)",
				"INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?) AS new ON DUPLICATE KEY UPDATE count = count + new.count",
			},
			initParams: [][]interface{}{
				{},
				{
					"1", 1, 1, 1,
					"1", 2, 1, 2,
				},
				{
					"1", 1, 1, 1,
				},
				{
					"1", 3, 1, 1,
					"1", 4, 1, 2,
				},
				{
					"1", 3, 0, 100,
					"1", 3, 1, 100,
					"1", 4, 0, 200,
				},
			},
			execSQL: "INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?),(?,?,?,?) AS new ON DUPLICATE KEY UPDATE count = count + new.count",
			execParams: []interface{}{
				"1", 1, 0, 5,
				"1", 1, 1, 10,
				"1", 2, 1, 20,
				"1", 2, 1, 30},

			expect: []interface{}{
				&Signal2{ID: "1", KeyValue: 1, Bucket: 0, Count: 5},
				&Signal2{ID: "1", KeyValue: 1, Bucket: 1, Count: 11},

				&Signal2{ID: "1", KeyValue: 2, Bucket: 0, Count: 0},
				&Signal2{ID: "1", KeyValue: 2, Bucket: 1, Count: 52},

				&Signal2{ID: "1", KeyValue: 3, Bucket: 0, Count: 100},
				&Signal2{ID: "1", KeyValue: 3, Bucket: 1, Count: 101},

				&Signal2{ID: "1", KeyValue: 4, Bucket: 0, Count: 200},
				&Signal2{ID: "1", KeyValue: 4, Bucket: 1, Count: 2},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Signal2{}
				err := r.Scan(&agg.ID, &agg.KeyValue, &agg.Bucket, &agg.Count)
				return &agg, err
			},
		},
		{
			description: "insert record with ignored field",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execParams:  []interface{}{1},
			querySQL:    "SELECT * FROM Abc2 WHERE PK = ?",
			init: []string{
				"DELETE FROM Abc2",
				"INSERT INTO Abc2(Id,Name) VALUES(?, ?)",
			},
			initParams: [][]interface{}{
				{},
				{1, "abc2"},
			},
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Abc2{Id: 1, Name: "abc2", Desc: ""},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				abc := Abc2{}
				err := r.Scan(&abc.Id, &abc.Name)
				return &abc, err
			},
			sleepSec: 3,
		},
		{
			description: "query with pk as a empty string ptr",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM authCode",
				"INSERT INTO authCode(Code,ClientId,UserId) VALUES(?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{"code01", "client01", "user01"},
			},
			querySQL:    "SELECT * FROM authCode WHERE PK = ?",
			queryParams: []interface{}{nullStringPtr()},

			expect: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := AuthCode{}
				err := r.Scan(&foo.Code, &foo.ClientId, &foo.UserId)
				return &foo, err
			},
		},
		{
			description: "query with pk as a string ptr",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM authCode",
				"INSERT INTO authCode(Code,ClientId,UserId) VALUES(?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{"code01", "client01", "user01"},
			},
			querySQL:    "SELECT * FROM authCode WHERE PK = ?",
			queryParams: []interface{}{stringPtr("code01")},

			expect: []interface{}{
				&AuthCode{Code: "code01", ClientId: "client01", UserId: "user01"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := AuthCode{}
				err := r.Scan(&foo.Code, &foo.ClientId, &foo.UserId)
				return &foo, err
			},
		},
		{
			description: "query with pk as a string",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM authCode",
				"INSERT INTO authCode(Code,ClientId,UserId) VALUES(?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{"code01", "client01", "user01"},
			},
			querySQL:    "SELECT * FROM authCode WHERE PK = ?",
			queryParams: []interface{}{"code01"},
			expect: []interface{}{
				&AuthCode{Code: "code01", ClientId: "client01", UserId: "user01"},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := AuthCode{}
				err := r.Scan(&foo.Code, &foo.ClientId, &foo.UserId)
				return &foo, err
			},
		},
		{
			description: "insert bool value",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM Foo2",
				"INSERT INTO Foo2(Id,Name,Actual) VALUES(?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{1, "foo2", true},
			},
			querySQL:    "SELECT * FROM Foo2 WHERE PK = ?",
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Foo2{Id: 1, Name: "foo2", Actual: true},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo2{}
				err := r.Scan(&foo.Id, &foo.Name, &foo.Actual)
				return &foo, err
			},
		},
		{
			description: "update bool value",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM Foo2",
				"INSERT INTO Foo2(Id,Name,Actual) VALUES(?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{1, "foo22", false},
			},
			execSQL:     "UPDATE Foo2 SET Actual = ? WHERE PK = ?",
			execParams:  []interface{}{true, 1},
			querySQL:    "SELECT * FROM Foo2 WHERE PK = ?",
			queryParams: []interface{}{1},
			expect: []interface{}{
				&Foo2{Id: 1, Name: "foo22", Actual: true},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo2{}
				err := r.Scan(&foo.Id, &foo.Name, &foo.Actual)
				return &foo, err
			},
		},
		{
			description: "wrapepd count with group by",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Msg/Items(id,body) VALUES(?,?),(?,?),(?,?),(?,?),(?,?)",
			execParams:  []interface{}{1, "test message", 1, "another message", 1, "last message", 1, "eee", 2, "test message"},

			querySQL:    "SELECT id, cnt FROM (SELECT id, COUNT(*) cnt FROM Msg/Items WHERE id in(?,?) GROUP BY 1)",
			queryParams: []interface{}{1, 2},
			init: []string{
				"DELETE FROM Msg",
			},
			expect: []interface{}{
				&CountRecGroup{ID: 1, Count: 4},
				&CountRecGroup{ID: 2, Count: 1},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := CountRecGroup{}
				err := r.Scan(&rec.ID, &rec.Count)
				return &rec, err
			},
		},
		{
			description: "count with group by",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Msg/Items(id,body) VALUES(?,?),(?,?),(?,?),(?,?),(?,?)",
			execParams:  []interface{}{1, "test message", 1, "another message", 1, "last message", 1, "eee", 2, "test message"},

			querySQL:    "SELECT id, COUNT(*) FROM Msg/Items WHERE id in(?,?) GROUP BY 1",
			queryParams: []interface{}{1, 2},
			init: []string{
				"DELETE FROM Msg",
			},
			expect: []interface{}{
				&CountRecGroup{ID: 1, Count: 4},
				&CountRecGroup{ID: 2, Count: 1},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				rec := CountRecGroup{}
				err := r.Scan(&rec.ID, &rec.Count)
				return &rec, err
			},
		},

		/// // "1" | MAP('{1:{"count":[0, 0]}, 2:{"count":[0, 0]}}')
		{
			description: "array batch merge",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id,keyValue,bucket,count FROM Signal2/Values WHERE pk = ?",
			queryParams: []interface{}{"1"},
			init: []string{
				"TRUNCATE TABLE Signal2 ",
				"INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{
					"1", 1, 1, 1,
					"1", 2, 1, 2,
				},
			},
			execSQL: "INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?),(?,?,?,?) AS new ON DUPLICATE KEY UPDATE count = count + new.count",
			execParams: []interface{}{
				"1", 1, 0, 5,
				"1", 1, 1, 10,
				"1", 2, 1, 20,
				"1", 2, 1, 30},

			expect: []interface{}{
				&Signal2{ID: "1", KeyValue: 1, Bucket: 0, Count: 5},
				&Signal2{ID: "1", KeyValue: 1, Bucket: 1, Count: 11},

				&Signal2{ID: "1", KeyValue: 2, Bucket: 0, Count: 0},
				&Signal2{ID: "1", KeyValue: 2, Bucket: 1, Count: 52},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Signal2{}
				err := r.Scan(&agg.ID, &agg.KeyValue, &agg.Bucket, &agg.Count)
				return &agg, err
			},
		},
		///
		{
			description: "array batch merge",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id,keyValue,bucket,count FROM Signal2/Values WHERE pk = ?",
			queryParams: []interface{}{"1"},
			init: []string{
				"TRUNCATE TABLE Signal2 ",
				"INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{
					"1", "v1", 1, 1,
					"1", "v2", 1, 2,
				},
			},
			execSQL: "INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?),(?,?,?,?) AS new ON DUPLICATE KEY UPDATE count = count + new.count",
			execParams: []interface{}{
				"1", "v1", 0, 5,
				"1", "v1", 1, 10,
				"1", "v2", 1, 20,
				"1", "v2", 1, 30},

			expect: []interface{}{
				&Signal2{ID: "1", KeyValue: "v1", Bucket: 0, Count: 5},
				&Signal2{ID: "1", KeyValue: "v1", Bucket: 1, Count: 11},
				&Signal2{ID: "1", KeyValue: "v2", Bucket: 0, Count: 0},
				&Signal2{ID: "1", KeyValue: "v2", Bucket: 1, Count: 52},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Signal2{}
				err := r.Scan(&agg.ID, &agg.KeyValue, &agg.Bucket, &agg.Count)
				return &agg, err
			},
		},
		{
			description: "array update by insert",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id,keyValue,bucket,count FROM Signal2/Values WHERE pk = ?",
			queryParams: []interface{}{"1"},
			init: []string{
				"TRUNCATE TABLE Signal2 ",
				"INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{
					"1", "v1", 1, 1,
					"1", "v2", 1, 2,
				},
			},
			execSQL: "INSERT INTO Signal2/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?),(?,?,?,?)",
			execParams: []interface{}{
				"1", "v1", 0, 5,
				"1", "v1", 1, 10,
				"1", "v2", 1, 21,
				"1", "v2", 1, 22},

			expect: []interface{}{
				&Signal2{ID: "1", KeyValue: "v1", Bucket: 0, Count: 5},
				&Signal2{ID: "1", KeyValue: "v1", Bucket: 1, Count: 10},

				&Signal2{ID: "1", KeyValue: "v2", Bucket: 0, Count: 0},
				&Signal2{ID: "1", KeyValue: "v2", Bucket: 1, Count: 22},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Signal2{}
				err := r.Scan(&agg.ID, &agg.KeyValue, &agg.Bucket, &agg.Count)
				return &agg, err
			},
		},
		{
			description: "map array with key filter and index range",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			//querySQL:    "SELECT id,value,bucket,count FROM Signal WHERE pk = ?",
			querySQL:    "SELECT id,keyValue,bucket,count FROM Signal/Values WHERE id = ? AND keyValue = ?  AND bucket between ? and ?",
			queryParams: []interface{}{"1", "v1", 2, 3},
			init: []string{
				"TRUNCATE TABLE Signal ",
			},
			execSQL:    "INSERT INTO Signal/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?)",
			execParams: []interface{}{"1", "v1", 1, 10, "1", "v1", 2, 11, "1", "v2", 3, 12},
			expect: []interface{}{
				&Signal{ID: "1", KeyValue: "v1", Bucket: 2, Count: 11},
				&Signal{ID: "1", KeyValue: "v1", Bucket: 3, Count: 0},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Signal{}
				err := r.Scan(&agg.ID, &agg.KeyValue, &agg.Bucket, &agg.Count)
				return &agg, err
			},
		},

		{
			description: "map array with key filter  ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			//querySQL:    "SELECT id,value,bucket,count FROM Signal WHERE pk = ?",
			querySQL:    "SELECT id,keyValue,bucket,count FROM Signal/Values WHERE id = ? AND keyValue IN(?)",
			queryParams: []interface{}{"1", "v2"},
			init: []string{
				"TRUNCATE TABLE Signal ",
			},
			execSQL:    "INSERT INTO Signal/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?)",
			execParams: []interface{}{"1", "v1", 1, 10, "1", "v1", 2, 11, "1", "v2", 3, 12},
			expect: []interface{}{
				&Signal{ID: "1", KeyValue: "v2", Bucket: 0, Count: 0},
				&Signal{ID: "1", KeyValue: "v2", Bucket: 1, Count: 0},
				&Signal{ID: "1", KeyValue: "v2", Bucket: 2, Count: 0},
				&Signal{ID: "1", KeyValue: "v2", Bucket: 3, Count: 12},
				&Signal{ID: "1", KeyValue: "v2", Bucket: 4, Count: 0},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Signal{}
				err := r.Scan(&agg.ID, &agg.KeyValue, &agg.Bucket, &agg.Count)
				return &agg, err
			},
		},
		{
			description: "array ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			//querySQL:    "SELECT id,value,bucket,count FROM Signal WHERE pk = ?",
			querySQL:    "SELECT id,keyValue,bucket,count FROM Signal/Values WHERE pk = ?",
			queryParams: []interface{}{"1"},
			init: []string{
				"TRUNCATE TABLE Signal ",
			},
			execSQL:    "INSERT INTO Signal/Values(id,keyValue,bucket,count) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?)",
			execParams: []interface{}{"1", "v1", 1, 10, "1", "v1", 2, 11, "1", "v2", 3, 12},
			expect: []interface{}{
				&Signal{ID: "1", KeyValue: "v1", Bucket: 0, Count: 0},
				&Signal{ID: "1", KeyValue: "v1", Bucket: 1, Count: 10},
				&Signal{ID: "1", KeyValue: "v1", Bucket: 2, Count: 11},
				&Signal{ID: "1", KeyValue: "v1", Bucket: 3, Count: 0},
				&Signal{ID: "1", KeyValue: "v1", Bucket: 4, Count: 0},

				&Signal{ID: "1", KeyValue: "v2", Bucket: 0, Count: 0},
				&Signal{ID: "1", KeyValue: "v2", Bucket: 1, Count: 0},
				&Signal{ID: "1", KeyValue: "v2", Bucket: 2, Count: 0},
				&Signal{ID: "1", KeyValue: "v2", Bucket: 3, Count: 12},
				&Signal{ID: "1", KeyValue: "v2", Bucket: 4, Count: 0},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := Signal{}
				err := r.Scan(&agg.ID, &agg.KeyValue, &agg.Bucket, &agg.Count)
				return &agg, err
			},
		},

		/*

			id,pk=true"` //--> day,value
				KeyValue  interface{} `aerospike:"value,arrayIndex"`
				Bucket int         `aerospike:"bucket,sliceKey=true,array=288"`
				Count  int         `aerospike:"count
		*/
		{
			description: "secondary secondaryIndex ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT * FROM users WHERE email = ?",
			queryParams: []interface{}{"xxx@test.io"},
			init: []string{
				"DROP INDEX IF EXISTS UserEmail ON " + namespace + ".users",
				"CREATE STRING INDEX UserEmail ON " + namespace + ".users(email)",
				"TRUNCATE TABLE users",
			},
			execSQL:    "INSERT INTO users(uid,email,active) VALUES(?,?,?),(?,?,?)",
			execParams: []interface{}{"1", "me@cpm.org", true, "2", "xxx@test.io", false},
			expect: []interface{}{
				&User{UID: "2", Email: "xxx@test.io", Active: false},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				agg := User{}
				err := r.Scan(&agg.UID, &agg.Email, &agg.Active)
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
			execSQL:     "INSERT INTO Msg/Items(id,body) VALUES(?,?),(?,?),(?,?),(?,?)",
			execParams:  []interface{}{1, "test message", 1, "another message", 1, "last message", 1, "eee"},

			querySQL:    "SELECT COUNT(*) FROM Msg/Items WHERE PK = ?",
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
			description: "list insert with secondaryIndex criteria",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "INSERT INTO Msg/Items(id,body) VALUES(?,?),(?,?),(?,?)",
			execParams:  []interface{}{1, "test message", 1, "another message", 1, "last message"},

			querySQL:    "SELECT id,seq,body FROM Msg/Items WHERE PK = ? AND index IN(?,?)",
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
			execSQL:     "INSERT INTO Msg/Items(id,body) VALUES(?,?),(?,?)",
			execParams:  []interface{}{1, "test message", 1, "another message"},

			querySQL:    "SELECT id,seq,body FROM Msg/Items WHERE PK = ?",
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
			execSQL:     "INSERT INTO Agg/Values(id,seq,amount,val) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?) AS new ON DUPLICATE KEY UPDATE val = val + new.val, amount = amount + new.amount",
			execParams:  []interface{}{1, 1, 11, 111, 1, 2, 12, 121, 2, 1, 11, 111},
			querySQL:    "SELECT id,seq,amount,val FROM Agg/Values WHERE PK = ? AND KEY IN(?, ?)",
			queryParams: []interface{}{1, 1, 2},
			init: []string{
				"DELETE FROM Agg/Values",
				"INSERT INTO Agg/Values(id,seq,amount,val) VALUES(1,1,1,1)",
				"INSERT INTO Agg/Values(id,seq,amount,val) VALUES(1,2,1,1)",
				"INSERT INTO Agg/Values(id,seq,amount,val) VALUES(2,1,1,1)",
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
			execSQL:     "INSERT INTO Agg/Values(id,seq,amount,val) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?) AS new ON DUPLICATE KEY UPDATE val = val + new.val, amount = amount + new.amount",
			execParams: []interface{}{
				1, 1, 11, 111,
				1, 2, 12, 121,
				2, 1, 11, 111},
			querySQL:    "SELECT id,seq,amount,val FROM Agg/Values WHERE PK = ? AND KEY IN(?, ?)",
			queryParams: []interface{}{1, 1, 2},
			init: []string{
				"DELETE FROM Agg/Values",
				"INSERT INTO Agg/Values(id,seq,amount,val) VALUES(1,1,1,1)",
				"INSERT INTO Agg/Values(id,seq,amount,val) VALUES(1,2,1,1)",
				"INSERT INTO Agg/Values(id,seq,amount,val) VALUES(2,1,1,1)",
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
			execSQL:     "INSERT INTO Agg/Values(id,seq,amount,val) VALUES(?,?,?,?),(?,?,?,?),(?,?,?,?)",
			execParams:  []interface{}{1, 1, 11, 111, 1, 2, 12, 121, 2, 1, 11, 111},
			querySQL:    "SELECT id,seq,amount,val FROM Agg/Values WHERE PK = ? AND KEY IN(?, ?)",
			queryParams: []interface{}{1, 1, 2},
			init: []string{
				"DELETE FROM Agg/Values",
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
			execSQL:     "UPDATE Agg/Values SET amount = amount + ?, val = val + 3  WHERE PK = ? AND KEY = ?",
			execParams:  []interface{}{10, 1, 1},
			querySQL:    "SELECT id, seq, amount, val FROM Agg/Values WHERE PK = ? AND KEY = ?",
			queryParams: []interface{}{1, 1},
			init: []string{
				"DELETE FROM Agg/Values",
				"INSERT INTO Agg/Values(id,seq, amount, val) VALUES(1, 1, 1, 1)",
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
			description: "update bin with dec ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "UPDATE SimpleAgg SET amount = amount - ?  WHERE PK = ?",
			execParams:  []interface{}{2, 1},
			querySQL:    "SELECT id, amount FROM SimpleAgg WHERE PK = ?",
			queryParams: []interface{}{1},
			init: []string{
				"DELETE FROM SimpleAgg",
				"INSERT INTO SimpleAgg(id,amount) VALUES(1,10)",
			},
			expect: []interface{}{
				&SimpleAgg{Id: 1, Amount: 8},
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
			description: "update with dec ",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			execSQL:     "UPDATE SimpleAgg SET amount = amount - ?  WHERE PK = ?",
			execParams:  []interface{}{2, 1},
			querySQL:    "SELECT id, amount FROM SimpleAgg WHERE PK = ?",
			queryParams: []interface{}{1},
			init: []string{
				"DELETE FROM SimpleAgg",
				"INSERT INTO SimpleAgg(id,amount) VALUES(1,10)",
			},
			expect: []interface{}{
				&SimpleAgg{Id: 1, Amount: 8},
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
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
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
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
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
			description: "get 1 record by PK with 2 bin map values by mapKey and between operator",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY BETWEEN ? AND ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 99,'doc0')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 104,'doc4')",
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
			description: "get 1 record by PK with 1 bin map values by mapKey and between operator",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY BETWEEN ? AND ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
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
			description: "get 1 record by PK with 0 bin map values by mapKey and between operator",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY BETWEEN ? AND ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
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
			description: "get 0 records by PK with 0 bin map values by mapKey and between operator",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY BETWEEN ? AND ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
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
			description: "get 1 record by PK with 1 bin map values by mapKey",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
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
			description: "get 1 record by PK with 0 bin map values by mapKey",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
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
			description: "get 0 record by PK with 0 bin map values by mapKey",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY = ?",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
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
			description: "get 1 record by PK with 2 bin map values by mapKey list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY IN (?,?)",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 104,'doc4')",
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
			description: "get 1 record by PK with 1 bin map values by mapKey list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY IN (?,?)",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
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
			description: "get 1 record by PK with 0 bin map values by mapKey list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY IN (?,?)",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
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
			description: "get 0 record by PK with 0 bin map values by mapKey list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			querySQL:    "SELECT id, seq, name FROM Doc/Bars WHERE PK = ? AND KEY IN (?,?)",
			init: []string{
				"DELETE FROM Doc",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 100,'doc1')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 101,'doc2')",
				"INSERT INTO Doc/Bars(id, seq, name) VALUES(1, 102,'doc3')",
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
			description: "get 1 record by PK with 1 bin map value by mapKey with string list",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM Qux",
				"INSERT INTO Qux/Bars(id,seq,name,list) VALUES(?,?,?,?)",
				"INSERT INTO Qux/Bars(id,seq,name,list) VALUES(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, "List of strings 1", []string{"item1", "item2", "item3"}},
				{1, 2, "List of strings 2", []string{"item11", "item22", "item33"}},
			},
			querySQL:    "SELECT id, seq, name, list FROM Qux/Bars WHERE PK = ? AND KEY = ?",
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
			description: "get 1 record by PK with 2 bin map values by mapKey - with time value stored as string",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM Baz",
				"INSERT INTO Baz/Bars(id,seq,name,time) VALUES(?,?,?,?)",
				"INSERT INTO Baz/Bars(id,seq,name,time) VALUES(1,2,'Time formatted stored as string 2','2021-01-06T05:00:00Z')",
				"INSERT INTO Baz/Bars(id,seq,name,time) VALUES(1,3,'Time formatted stored as string 3','2021-01-08T09:10:11Z')",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, "Time formatted stored as string 1", "2021-01-06T05:04:03Z"},
				{},
				{},
			},
			querySQL:    "SELECT id, seq, name, time FROM Baz/Bars WHERE PK = ? AND KEY IN (?,?)",
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
			description: "get 1 records by PK with 2 bin map values by mapKey - with time value stored as int",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM BazUnix",
				"INSERT INTO BazUnix/Bars(id,seq,name,time) VALUES(1,1,'Time stored as int 3','2021-01-06T05:00:00Z')",
				"INSERT INTO BazUnix/Bars(id,seq,name,time) VALUES(?,?,?,?)",
				"INSERT INTO BazUnix/Bars(id,seq,name,time) VALUES(1,3,'Time stored as int 5','2021-02-03T04:05:06Z')",
			},
			initParams: [][]interface{}{
				{},
				{},
				{1, 2, "Time stored as int 4", getTime("2021-02-03T04:05:06Z")},
				{},
			},
			querySQL:    "SELECT id, seq, name, time FROM BazUnix/Bars WHERE PK = ? AND KEY IN (?,?)",
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
			description: "get 2 records with all bins by PK - with time value stored as string, time ptr in type",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM BazPtr",
				"INSERT INTO BazPtr(id,seq,name,time) VALUES(1,1,'Time formatted stored as string','2021-01-06T05:00:00Z')",
				"INSERT INTO BazPtr(id,seq,name,time) VALUES(?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{},
				{2, 2, "Time formatted stored as string", "2021-01-08T05:00:00Z"},
			},
			querySQL:    "SELECT * FROM BazPtr WHERE PK IN (?,?)",
			queryParams: []interface{}{1, 2},
			expect: []interface{}{
				&BazPtr{Id: 1, Seq: 1, Name: "Time formatted stored as string", Time: getTimePtr(getTime("2021-01-06T05:00:00Z"))},
				&BazPtr{Id: 2, Seq: 2, Name: "Time formatted stored as string", Time: getTimePtr(getTime("2021-01-08T05:00:00Z"))},
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
		{
			description: "get 2 records with all bins by PK - struct with no ptrs",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM bar",
				"INSERT INTO bar(id,seq,amount,price,name,time) VALUES(1,1,11,1.25,'Time formatted stored as string','2021-01-06T05:00:00Z')",
				"INSERT INTO bar(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{},
				{2, 2, 22, 2.25, "Time formatted stored as string", "2021-01-08T05:00:00Z"},
			},
			querySQL:    "SELECT * FROM bar WHERE PK IN (?,?)",
			queryParams: []interface{}{1, 2},
			expect: []interface{}{
				&Bar{Id: 1, Seq: 1, Amount: 11, Price: 1.25, Name: "Time formatted stored as string", Time: getTime("2021-01-06T05:00:00Z")},
				&Bar{Id: 2, Seq: 2, Amount: 22, Price: 2.25, Name: "Time formatted stored as string", Time: getTime("2021-01-08T05:00:00Z")},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bar := Bar{}
				err := r.Scan(&bar.Id, &bar.Seq, &bar.Amount, &bar.Price, &bar.Name, &bar.Time)
				return &bar, err
			},
		},
		{
			description: "get 2 records with all bins by PK - struct with ptrs",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM barPtr",
				"INSERT INTO barPtr(id,seq,amount,price,name,time) VALUES(1,1,11,1.25,'Time formatted stored as string','2021-01-06T05:00:00Z')",
				"INSERT INTO barPtr(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{},
				{2, 2, 22, 2.25, "Time formatted stored as string", "2021-01-08T05:00:00Z"},
			},
			querySQL:    "SELECT * FROM barPtr WHERE PK IN (?,?)",
			queryParams: []interface{}{1, 2},
			expect: []interface{}{
				&BarPtr{Id: 1, Seq: 1, Amount: getIntPtr(11), Price: getFloatPtr(1.25), Name: getStringPtr("Time formatted stored as string"), Time: getTimePtr(getTime("2021-01-06T05:00:00Z"))},
				&BarPtr{Id: 2, Seq: 2, Amount: getIntPtr(22), Price: getFloatPtr(2.25), Name: getStringPtr("Time formatted stored as string"), Time: getTimePtr(getTime("2021-01-08T05:00:00Z"))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bar := BarPtr{}
				err := r.Scan(&bar.Id, &bar.Seq, &bar.Amount, &bar.Price, &bar.Name, &bar.Time)
				return &bar, err
			},
		},
		{
			description: "get 2 records with all bins by PK - struct with double ptrs",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM barDoublePtr",
				"INSERT INTO barDoublePtr(id,seq,amount,price,name,time) VALUES(1,1,11,1.25,'Time formatted stored as string','2021-01-06T05:00:00Z')",
				"INSERT INTO barDoublePtr(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{},
				{2, 2, 22, 2.25, "Time formatted stored as string", "2021-01-08T05:00:00Z"},
			},
			querySQL:    "SELECT * FROM barDoublePtr WHERE PK IN (?,?)",
			queryParams: []interface{}{1, 2},
			expect: []interface{}{
				&BarDoublePtr{Id: 1, Seq: 1, Amount: getIntDoublePtr(11), Price: getFloatDoublePtr(1.25), Name: getStringDoublePtr("Time formatted stored as string"), Time: getTimeDoublePtr(getTime("2021-01-06T05:00:00Z"))},
				&BarDoublePtr{Id: 2, Seq: 2, Amount: getIntDoublePtr(22), Price: getFloatDoublePtr(2.25), Name: getStringDoublePtr("Time formatted stored as string"), Time: getTimeDoublePtr(getTime("2021-01-08T05:00:00Z"))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bar := BarDoublePtr{}
				err := r.Scan(&bar.Id, &bar.Seq, &bar.Amount, &bar.Price, &bar.Name, &bar.Time)
				return &bar, err
			},
		},
		{
			description: "get 2 records with all bins by PK with map - struct with ptrs",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM barPtr",
				"INSERT INTO barPtr/Values(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?),(?,?,?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{1, 1, 11, 1.25, "Time formatted stored as string", "2021-01-06T05:00:00Z", 1, 2, 22, 2.25, "Time formatted stored as string", "2021-01-08T05:00:00Z"},
			},
			querySQL:    "SELECT * FROM barPtr/Values WHERE PK IN (?) AND KEY IN (?,?)",
			queryParams: []interface{}{1, 1, 2},
			expect: []interface{}{
				&BarPtr{Id: 1, Seq: 1, Amount: getIntPtr(11), Price: getFloatPtr(1.25), Name: getStringPtr("Time formatted stored as string"), Time: getTimePtr(getTime("2021-01-06T05:00:00Z"))},
				&BarPtr{Id: 1, Seq: 2, Amount: getIntPtr(22), Price: getFloatPtr(2.25), Name: getStringPtr("Time formatted stored as string"), Time: getTimePtr(getTime("2021-01-08T05:00:00Z"))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bar := BarPtr{}
				err := r.Scan(&bar.Id, &bar.Seq, &bar.Amount, &bar.Price, &bar.Name, &bar.Time)
				return &bar, err
			},
		},
		{
			description: "get 2 records with all bins by PK with map - struct with double ptrs",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM barDoublePtr",
				"INSERT INTO barDoublePtr/KeyValue(id,seq,amount,price,name,time) VALUES(1,1,11,1.25,'Time formatted stored as string','2021-01-06T05:00:00Z')",
				"INSERT INTO barDoublePtr/KeyValue(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{},
				{1, 2, 22, 2.25, "Time formatted stored as string", "2021-01-08T05:00:00Z"},
			},
			querySQL:    "SELECT * FROM barDoublePtr/KeyValue WHERE PK IN (?) AND KEY IN (?,?)",
			queryParams: []interface{}{1, 1, 2},
			expect: []interface{}{
				&BarDoublePtr{Id: 1, Seq: 1, Amount: getIntDoublePtr(11), Price: getFloatDoublePtr(1.25), Name: getStringDoublePtr("Time formatted stored as string"), Time: getTimeDoublePtr(getTime("2021-01-06T05:00:00Z"))},
				&BarDoublePtr{Id: 1, Seq: 2, Amount: getIntDoublePtr(22), Price: getFloatDoublePtr(2.25), Name: getStringDoublePtr("Time formatted stored as string"), Time: getTimeDoublePtr(getTime("2021-01-08T05:00:00Z"))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bar := BarDoublePtr{}
				err := r.Scan(&bar.Id, &bar.Seq, &bar.Amount, &bar.Price, &bar.Name, &bar.Time)
				return &bar, err
			},
		},
		{
			description: "get 2 records with all bins by PK aggregation with map - struct with ptrs",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM barPtr",
				"INSERT INTO barPtr/Values(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?),(?,?,?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{
					1, 1, 11, 1.25, "Time 1", "2021-01-06T05:00:00Z",
					1, 2, 22, 2.25, "Time 2", "2021-01-08T05:00:00Z",
				},
			},
			execSQL: "INSERT INTO barPtr/Values(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?),(?,?,?,?,?,?) AS new ON DUPLICATE KEY UPDATE amount = amount + new.amount, price = price + new.price, name = new.name, time = new.time",
			execParams: []interface{}{
				1, 1, 11, 1.25, "Time 11", "2021-01-26T05:00:00Z",
				1, 2, 22, 2.25, "Time 22", "2021-01-28T05:00:00Z",
			},

			querySQL:    "SELECT * FROM barPtr/Values WHERE PK IN (?) AND KEY IN (?,?)",
			queryParams: []interface{}{1, 1, 2},
			expect: []interface{}{
				&BarPtr{Id: 1, Seq: 1, Amount: getIntPtr(22), Price: getFloatPtr(2.5), Name: getStringPtr("Time 11"), Time: getTimePtr(getTime("2021-01-26T05:00:00Z"))},
				&BarPtr{Id: 1, Seq: 2, Amount: getIntPtr(44), Price: getFloatPtr(4.5), Name: getStringPtr("Time 22"), Time: getTimePtr(getTime("2021-01-28T05:00:00Z"))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bar := BarPtr{}
				err := r.Scan(&bar.Id, &bar.Seq, &bar.Amount, &bar.Price, &bar.Name, &bar.Time)
				return &bar, err
			},
		},
		{
			description: "get 2 records with all bins by PK aggregation with map - struct with double ptrs",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM barDoublePtr",
				"INSERT INTO barDoublePtr/Values(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?),(?,?,?,?,?,?)",
			},
			initParams: [][]interface{}{
				{},
				{
					1, 1, 11, 1.25, "Time 1", "2021-01-06T05:00:00Z",
					1, 2, 22, 2.25, "Time 2", "2021-01-08T05:00:00Z",
				},
			},
			execSQL: "INSERT INTO barDoublePtr/Values(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?),(?,?,?,?,?,?) AS new ON DUPLICATE KEY UPDATE amount = amount + new.amount, price = price + new.price, name = new.name, time = new.time",
			execParams: []interface{}{
				1, 1, 11, 1.25, "Time 11", "2021-01-26T05:00:00Z",
				1, 2, 22, 2.25, "Time 22", "2021-01-28T05:00:00Z",
			},

			querySQL:    "SELECT * FROM barDoublePtr/Values WHERE PK IN (?) AND KEY IN (?,?)",
			queryParams: []interface{}{1, 1, 2},
			expect: []interface{}{
				&BarDoublePtr{Id: 1, Seq: 1, Amount: getIntDoublePtr(22), Price: getFloatDoublePtr(2.5), Name: getStringDoublePtr("Time 11"), Time: getTimeDoublePtr(getTime("2021-01-26T05:00:00Z"))},
				&BarDoublePtr{Id: 1, Seq: 2, Amount: getIntDoublePtr(44), Price: getFloatDoublePtr(4.5), Name: getStringDoublePtr("Time 22"), Time: getTimeDoublePtr(getTime("2021-01-28T05:00:00Z"))},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bar := BarDoublePtr{}
				err := r.Scan(&bar.Id, &bar.Seq, &bar.Amount, &bar.Price, &bar.Name, &bar.Time)
				return &bar, err
			},
		},
		{
			description: "aggregate with duplicated pk and mapKey in one batch",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"TRUNCATE TABLE bar",
			},
			execSQL: "INSERT INTO bar/Values(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?) AS new ON DUPLICATE KEY UPDATE amount = amount + new.amount, price = price + new.price, name = new.name, time = new.time",
			execParams: []interface{}{
				1, 1, 11, 1.25, "Time A", "2001-01-01T01:01:01Z",
				1, 2, 22, 2.50, "Time A", "2001-01-01T01:01:01Z",
				1, 1, 11, 1.25, "Time B", "2002-02-02T02:02:02Z",
				1, 2, 22, 2.50, "Time B", "2002-02-02T02:02:02Z",
				1, 3, 33, 3.50, "Time C", "2003-03-03T03:03:03Z",

				2, 1, 11, 1.25, "Time A", "2001-01-01T01:01:01Z",
				2, 2, 22, 2.50, "Time A", "2001-01-01T01:01:01Z",
				2, 1, 11, 1.25, "Time B", "2002-02-02T02:02:02Z",
				2, 2, 22, 2.50, "Time B", "2002-02-02T02:02:02Z",
				2, 3, 33, 3.50, "Time C", "2003-03-03T03:03:03Z",
			},
			querySQL:    "SELECT * FROM bar/Values WHERE PK IN (?,?) AND KEY IN (?,?,?)",
			queryParams: []interface{}{1, 2, 1, 2, 3},
			expect: []interface{}{
				&Bar{Id: 1, Seq: 1, Amount: 22, Price: 2.5, Name: "Time B", Time: getTime("2002-02-02T02:02:02Z")},
				&Bar{Id: 1, Seq: 2, Amount: 44, Price: 5.0, Name: "Time B", Time: getTime("2002-02-02T02:02:02Z")},
				&Bar{Id: 1, Seq: 3, Amount: 33, Price: 3.5, Name: "Time C", Time: getTime("2003-03-03T03:03:03Z")},
				&Bar{Id: 2, Seq: 1, Amount: 22, Price: 2.5, Name: "Time B", Time: getTime("2002-02-02T02:02:02Z")},
				&Bar{Id: 2, Seq: 2, Amount: 44, Price: 5.0, Name: "Time B", Time: getTime("2002-02-02T02:02:02Z")},
				&Bar{Id: 2, Seq: 3, Amount: 33, Price: 3.5, Name: "Time C", Time: getTime("2003-03-03T03:03:03Z")},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bar := Bar{}
				err := r.Scan(&bar.Id, &bar.Seq, &bar.Amount, &bar.Price, &bar.Name, &bar.Time)
				return &bar, err
			},
		},
		{
			description: "avoid to insert float value as int",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM bar",
			},
			execSQL: "INSERT INTO bar/Values(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?)",
			execParams: []interface{}{
				1, 1, 123.45, 1.2, "Time 11", "2021-01-06T05:00:00Z",
			},
			allowExecError: true,
		},
		{
			description: "aggregate with with int to float param conversion",
			dsn:         "aerospike://127.0.0.1:3000/" + namespace,
			init: []string{
				"DELETE FROM bar",
			},
			execSQL: "INSERT INTO bar/Values(id,seq,amount,price,name,time) VALUES(?,?,?,?,?,?),(?,?,?,?,?,?) AS new ON DUPLICATE KEY UPDATE amount = amount + new.amount, price = price + new.price, name = new.name, time = new.time",
			execParams: []interface{}{
				1, 1, 11, 1, "Time 11", "2021-01-06T05:00:00Z",
				1, 1, 11, 1.25, "Time 11", "2021-01-26T05:00:00Z",
			},

			querySQL:    "SELECT * FROM bar/Values WHERE PK IN (?) AND KEY IN (?)",
			queryParams: []interface{}{1, 1},
			expect: []interface{}{
				&Bar{Id: 1, Seq: 1, Amount: 22, Price: 2.25, Name: "Time 11", Time: getTime("2021-01-26T05:00:00Z")},
			},
			scanner: func(r *sql.Rows) (interface{}, error) {
				bar := Bar{}
				err := r.Scan(&bar.Id, &bar.Seq, &bar.Amount, &bar.Price, &bar.Name, &bar.Time)
				return &bar, err
			},
		},
	}

	//testCases = testCases[0:1]

	for _, tc := range testCases {
		if len(tc.sets) == 0 {
			tc.sets = sets
		}
	}

	testCases.runTest(t)
}

func (s tstCases) runTest(t *testing.T) {
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
				if tc.allowExecError && err != nil {
					return
				}
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

			if !assert.Equal(t, tc.expect, actual, tc.description) {
				fmt.Println("************* EXPECTED")
				toolbox.DumpIndent(tc.expect, false)

				fmt.Println("************* ACTUAL")
				toolbox.DumpIndent(actual, false)

			}
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

func getStringPtr(s string) *string {
	return &s
}

func getStringDoublePtr(s string) **string {
	ptr := &s
	return &ptr
}

func getIntPtr(i int) *int {
	return &i
}

func getIntDoublePtr(i int) **int {
	ptr := &i
	return &ptr
}

func getFloatPtr(f float64) *float64 {
	return &f
}

func getFloatDoublePtr(f float64) **float64 {
	ptr := &f
	return &ptr
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
type tableSlice []tableInfo

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
// secondaryIndex i should sort before the element with secondaryIndex j.
func (t tableSlice) Less(i, j int) bool {
	return t[i].TableName < t[j].TableName
}

func modifyActual(src []interface{}) (interface{}, error) {
	var result []*tableInfo
	for _, item := range src {
		t, ok := item.(*tableInfo)
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

func nullStringPtr() *string { return nil }
