package sql

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func BenchmarkStatement_ExecContext_BatchInsert(b *testing.B) {
	data := perfTestData()
	namespace := "udb"
	values := strings.Repeat("(?, ?, ?, ?, ?),", len(data))
	SQL := "INSERT INTO PerfTest$Values (id, seq, active, quantity, value) VALUES" + values[:len(values)-1]

	args := make([]interface{}, 0, len(data)*5)
	for _, item := range data {
		args = append(args, item.Bind()...)
	}
	b.ResetTimer()

	db, err := sql.Open("aerospike", "aerospike://127.0.0.1:3000/"+namespace)
	if !assert.Nil(b, err) {
		return
	}

	_, err = db.ExecContext(context.Background(), "REGISTER SET PerfTest AS ?", PerfTest{})
	if !assert.Nil(b, err) {
		return
	}
	if !assert.NotNil(b, db) {
		return
	}
	_, err = db.ExecContext(context.Background(), "DELETE FROM PerfTest")
	for i := 0; i < b.N; i++ {
		_, err = db.ExecContext(context.Background(), SQL, args...)
		if !assert.Nil(b, err) {
			return
		}
	}
}

/*
-- Create a secondary index on the keys of the map_bin bin in the test.example_set_2 set
CREATE INDEX map_key_index ON test.example_set_2 (map_bin) MAPKEYS STRING;

-- Create a secondary index on the values of the map_bin bin in the test.example_set_2 set
CREATE INDEX map_value_index ON test.example_set_2 (map_bin) MAPVALUES NUMERIC;


CREATE INDEX map_key_index ON udb.PerfTest (Values) MAPKEYS STRING;
CREATE INDEX map_value_index ON udb.PerfTest (Values) MAPVALUES NUMERIC;
*/

func BenchmarkStatement_ExecContext_BatchMerge(b *testing.B) {
	/*  feature_value->1.. 100 000
	 *  pk:date, mod(%100) 500
		secofday, count
		set :feature_type -> viant-taxonomy

		signals -> date, secofday (5min interval), feature_type, feature_value, count
	*/
	data := perfTestData()
	namespace := "udb"
	values := strings.Repeat("(?, ?, ?, ?, ?),", len(data))
	SQL := "INSERT INTO PerfTest$Values (id, seq, active, quantity, value) VALUES" + values[:len(values)-1] + " AS new ON DUPLICATE KEY UPDATE active = active + new.active, quantity = quantity + new.quantity, value = value + new.value"

	args := make([]interface{}, 0, len(data)*5)
	for _, item := range data {
		args = append(args, item.Bind()...)
	}
	b.ResetTimer()

	db, err := sql.Open("aerospike", "aerospike://127.0.0.1:3000/"+namespace)
	if !assert.Nil(b, err) {
		return
	}

	_, err = db.ExecContext(context.Background(), "REGISTER SET PerfTest AS ?", PerfTest{})
	if !assert.Nil(b, err) {
		return
	}
	if !assert.NotNil(b, db) {
		return
	}
	_, err = db.ExecContext(context.Background(), "DELETE FROM PerfTest")
	for i := 0; i < b.N; i++ {
		_, err := db.ExecContext(context.Background(), SQL, args...)
		if !assert.Nil(b, err) {
			return
		}
	}
}

type PerfTest struct {
	ID       int     `aerospike:"id,pk"`
	Seq      int     `aerospike:"seq,key"`
	Active   int     `aerospike:"active"`
	Quantity int     `aerospike:"quantity"`
	Value    float64 `aerospike:"value"`
}

func (p *PerfTest) Bind() []interface{} {
	return []interface{}{p.ID, p.Seq, p.Active, p.Quantity, p.Value}
}

func perfTestData() []*PerfTest {
	var items = make([]*PerfTest, 100000)
	for i := range items {
		items[i] = &PerfTest{
			ID:       i % 1000,
			Seq:      i,
			Active:   i % 2,
			Quantity: 3 + i,
			Value:    float64(12 + i),
		}
	}
	return items
}
