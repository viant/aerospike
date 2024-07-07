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
	namespace := "test"
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

func BenchmarkStatement_ExecContext_BatchMerge(b *testing.B) {
	data := perfTestData()
	namespace := "test"
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
	var items = make([]*PerfTest, 10000)
	for i := range items {
		items[i] = &PerfTest{
			ID:       i % 100,
			Seq:      i,
			Active:   i % 2,
			Quantity: 3 + i,
			Value:    float64(12 + i),
		}
	}
	return items
}
