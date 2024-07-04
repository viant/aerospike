package sql

import (
	"context"
	"database/sql"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ExecContext(t *testing.T) {

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
			dsn:         "aerospike://127.0.0.1:3000/test",
			sql:         "REGISTER SET Bar AS struct{id int; name string}",
		},
		{
			description: "register named set",
			dsn:         "aerospike://127.0.0.1:3000/test",
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
	SELECT * FROM Foo$MapBin WHERE PK = 'PK'
	SELECT * FROM Foo$MapBin WHERE PK = 'value'
	SELECT * FROM Foo$MapBin WHERE PK = 'value' AND KEY = 'key1'

	SELECT * FROM Foo, UNNEST(Foo.MapBin) WHERE PK = 'value' AND KEY = 'key1'
	SELECT PK.Bins.* FROM Foo$MapBin Bins WHERE PK = 'value' AND KEY = 'key1
*/

type entry struct {
	pk     string
	set    string
	binMap map[string]interface{}
}

func Test_QueryContext(t *testing.T) {

	type Foo struct {
		Id   int //`aql:"id,key=true"
		Name string
	}

	var testCases = []struct {
		description string
		dsn         string
		execSQL     string
		execParams  []interface{}
		querySQL    string
		queryParams []interface{}
		expect      interface{}
		scanner     func(r *sql.Rows) (interface{}, error)
		testData    []*entry
	}{
		{
			description: "get 1 records by PK",
			dsn:         "aerospike://127.0.0.1:3000/test",
			// * 'bigquery://projectID/[location/]datasetID?queryString' //TODO params
			execSQL:     "REGISTER SET Foo AS ?",
			execParams:  []interface{}{Foo{}},
			querySQL:    "SELECT * FROM Foo WHERE PK = ?",
			queryParams: []interface{}{"1"},
			testData:    []*entry{{pk: "1", set: "Foo", binMap: as.BinMap{"Id": 1, "Name": "foo1"}}},
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
			dsn:         "aerospike://127.0.0.1:3000/test",
			execSQL:     "REGISTER SET Foo AS ?",
			execParams:  []interface{}{Foo{}},
			querySQL:    "SELECT * FROM Foo WHERE PK = ?",
			queryParams: []interface{}{"0"},
			testData:    []*entry{{pk: "1", set: "Foo", binMap: as.BinMap{"Id": 1, "Name": "foo1"}}},
			expect:      []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
		},
		//{
		//	description: "register named type",
		//	dsn:         "aerospike:///testdata/",
		//	execSQL:     "REGISTER TYPE Foo AS ?",
		//	execParams:  []interface{}{Foo{}},
		//	querySQL:    "SELECT * FROM Foo WHERE id=2",
		//	queryParams: []interface{}{},
		//	scanner: func(r *sql.Rows) (interface{}, error) {
		//		foo := Foo{}
		//		err := r.Scan(&foo.Id, &foo.Name)
		//		return &foo, err
		//	},
		//	expect: []interface{}{
		//		&Foo{Id: 2, Name: "name2"},
		//	},
		//},
		//{
		//	description: "register named type",
		//	dsn:         "aerospike:///testdata/",
		//	execSQL:     "REGISTER TYPE Foo AS ?",
		//	execParams:  []interface{}{Foo{}},
		//	querySQL:    "SELECT * FROM Foo WHERE id IN(?, ?, ?)",
		//	queryParams: []interface{}{1, 2, 3},
		//	scanner: func(r *sql.Rows) (interface{}, error) {
		//		foo := Foo{}
		//		err := r.Scan(&foo.Id, &foo.Name)
		//		return &foo, err
		//	},
		//	expect: []interface{}{
		//		&Foo{Id: 1, Name: "name1"},
		//		&Foo{Id: 2, Name: "name2"},
		//	},
		//},
	}

	for _, tc := range testCases {
		//for _, tc := range testCases[1:2] {
		t.Run(tc.description, func(t *testing.T) {
			err := prepareTestData(tc.dsn, tc.testData)
			if !assert.Nil(t, err, tc.description) {
				return
			}

			db, err := sql.Open("aerospike", tc.dsn)
			if !assert.Nil(t, err, tc.description) {
				return
			}
			if !assert.NotNil(t, db, tc.description) {
				return
			}

			if tc.execSQL != "" {
				_, err = db.ExecContext(context.Background(), tc.execSQL, tc.execParams...)
				assert.Nil(t, err, tc.description)
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
			assert.Equal(t, tc.expect, items, tc.description)
		})
	}
}

func prepareTestData(dsn string, testData []*entry) error {
	cfg, err := ParseDSN(dsn)
	client, err := as.NewClientWithPolicy(nil, cfg.Host, cfg.Port)
	if err != nil {
		return fmt.Errorf("failed to connect to aerospike with dsn %s due to: %v", dsn, err)
	}
	defer client.Close()

	sets := map[string]bool{}
	for _, v := range testData {
		sets[v.set] = true
	}

	for set := range sets {
		err = client.Truncate(nil, cfg.Namespace, set, nil)
		if err != nil {
			return fmt.Errorf("failed to truncate set: %v", err)
		}
	}

	for _, v := range testData {
		key, err := as.NewKey(cfg.Namespace, v.set, v.pk)
		if err != nil {
			return fmt.Errorf("failed to create key: %v", err)
		}
		err = client.Put(nil, key, v.binMap)
		if err != nil {
			return fmt.Errorf("failed to delete test record: %v", err)
		}
	}

	return err
}
