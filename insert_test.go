package aerospike

import (
	"context"
	"database/sql"
	"github.com/hashicorp/golang-lru"
	"github.com/stretchr/testify/assert"
	ainsert "github.com/viant/aerospike/insert"
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
-- Create a secondary secondaryIndex on the keys of the map_bin bin in the test.example_set_2 set
CREATE INDEX map_key_index ON test.example_set_2 (map_bin) MAPKEYS STRING;

-- Create a secondary secondaryIndex on the values of the map_bin bin in the test.example_set_2 set
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
	Seq      int     `aerospike:"seq,mapKey"`
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

// prepareInsert
func Test_prepareInsert_handlesInvalidSQLInPrepareInsert(t *testing.T) {
	statement := &Statement{}
	connection := &connection{}

	err := statement.prepareInsert("INVALID SQL", connection)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "prepareInsert parse")
}

func Test_prepareInsert_createsNewValuesOnlyStatementWhenNotCached(t *testing.T) {
	statement := &Statement{}
	cache, _ := lru.New(2)
	connection := &connection{
		insertCache: cache,
	}
	sql := "INSERT INTO table_name/Values (col1, col2) VALUES (?, ?)"

	err := statement.prepareInsert(sql, connection)
	assert.Nil(t, err)

	expectedKey := "INSERT INTO table_name/Values (col1, col2) VALUES#PLACEHOLDERS#"
	iActualStmt, ok := cache.Get(expectedKey)
	assert.True(t, ok)
	assert.NotNil(t, iActualStmt)
	actualStmt, ok := iActualStmt.(*ainsert.Statement)
	assert.True(t, ok)

	assert.Equal(t, ainsert.ParameterizedValuesOnly, actualStmt.Kind)
	assert.Equal(t, 2, actualStmt.ValuesOnlyPlaceholdersCnt)
	assert.Nil(t, actualStmt.Values)
	assert.NotNil(t, actualStmt.PlaceholderValue)
	assert.Equal(t, cache.Len(), 1)
}

func Test_prepareInsert_createsValueOnlyStatementWhenCached(t *testing.T) {
	statement := &Statement{}
	cache, _ := lru.New(2)
	connection := &connection{
		insertCache: cache,
	}
	sql := "INSERT INTO table_name/Values (col1, col2) VALUES (?, ?)"

	err := statement.prepareInsert(sql, connection)
	assert.Nil(t, err)

	sql2 := "INSERT INTO table_name/Values (col1, col2) VALUES (?, ?), (?, ?)"
	err = statement.prepareInsert(sql2, connection)
	assert.Nil(t, err)

	actualStmt := statement.insert
	assert.NotNil(t, actualStmt)

	assert.Equal(t, ainsert.ParameterizedValuesOnly, actualStmt.Kind)
	assert.Equal(t, 4, actualStmt.ValuesOnlyPlaceholdersCnt)
	assert.Nil(t, actualStmt.Values)
	assert.NotNil(t, actualStmt.PlaceholderValue)
	assert.Equal(t, 1, cache.Len())
}

func Test_prepareInsert_createsNewNoValuesOnlyStatementWhenNotCached(t *testing.T) {
	statement := &Statement{}
	cache, _ := lru.New(2)
	connection := &connection{
		insertCache: cache,
	}
	sql := "INSERT INTO table_name/Values (col1, col2) VALUES (1, 2)"

	err := statement.prepareInsert(sql, connection)
	assert.Nil(t, err)

	expectedKey := "INSERT INTO table_name/Values (col1, col2) VALUES (1, 2)"
	iActualStmt, ok := cache.Get(expectedKey)
	assert.True(t, ok)
	assert.NotNil(t, iActualStmt)
	actualStmt, ok := iActualStmt.(*ainsert.Statement)
	assert.True(t, ok)

	assert.Equal(t, ainsert.Kind(""), actualStmt.Kind)
	assert.Equal(t, 0, actualStmt.ValuesOnlyPlaceholdersCnt)
	assert.NotNil(t, actualStmt.Values)
	assert.Equal(t, 2, len(actualStmt.Values))
	assert.Nil(t, actualStmt.PlaceholderValue)
	assert.Equal(t, 1, cache.Len())
}

func Test_prepareInsert_createsNoValueOnlyStatementWhenCached_02(t *testing.T) {
	statement := &Statement{}
	cache, _ := lru.New(2)
	connection := &connection{
		insertCache: cache,
	}
	sql := "INSERT INTO table_name/Values (col1, col2) VALUES (1, 2)"

	err := statement.prepareInsert(sql, connection)
	assert.Nil(t, err)

	sql2 := "INSERT INTO table_name/Values (col1, col2) VALUES (1, 2), (3,4)"
	err = statement.prepareInsert(sql2, connection)
	assert.Nil(t, err)

	actualStmt := statement.insert
	assert.NotNil(t, actualStmt)

	assert.Equal(t, ainsert.Kind(""), actualStmt.Kind)
	assert.Equal(t, 0, actualStmt.ValuesOnlyPlaceholdersCnt)
	assert.NotNil(t, actualStmt.Values)
	assert.Equal(t, 4, len(actualStmt.Values))
	assert.Nil(t, actualStmt.PlaceholderValue)
	assert.Equal(t, 2, cache.Len())
}

func Test_prepareInsert_createsNoValueOnlyStatementWhenCached(t *testing.T) {
	statement := &Statement{}
	cache, _ := lru.New(2)
	connection := &connection{
		insertCache: cache,
	}
	sql := "INSERT INTO table_name/Values (col1, col2) VALUES (1, 2)"

	err := statement.prepareInsert(sql, connection)
	assert.Nil(t, err)

	sql2 := "INSERT INTO table_name/Values (col1, col2) VALUES (1, 2)"
	err = statement.prepareInsert(sql2, connection)
	assert.Nil(t, err)

	actualStmt := statement.insert
	assert.NotNil(t, actualStmt)

	assert.Equal(t, ainsert.Kind(""), actualStmt.Kind)
	assert.Equal(t, 0, actualStmt.ValuesOnlyPlaceholdersCnt)
	assert.NotNil(t, actualStmt.Values)
	assert.Equal(t, 2, len(actualStmt.Values))
	assert.Nil(t, actualStmt.PlaceholderValue)
	assert.Equal(t, 1, cache.Len())
}

func Test_prepareInsert_handlesNilInsertCache(t *testing.T) {
	statement := &Statement{}
	connection := &connection{
		insertCache: nil,
	}
	sql := "INSERT INTO table_name/Values (col1, col2) VALUES (?, ?)"

	err := statement.prepareInsert(sql, connection)
	assert.Nil(t, err)
	actualStmt := statement.insert
	assert.NotNil(t, actualStmt)
	assert.Equal(t, ainsert.Kind(""), actualStmt.Kind)
	assert.Equal(t, 0, actualStmt.ValuesOnlyPlaceholdersCnt)
	assert.NotNil(t, actualStmt.Values)
	assert.Equal(t, 2, len(actualStmt.Values))
	assert.Nil(t, actualStmt.PlaceholderValue)
}

// extractKindAndKey
func Test_extractKindAndKey_00(t *testing.T) {
	kind, key, placeholderCount := extractKindAndKey("INSERT INTO table_name/Values (col1, col2) VALUES")
	assert.Equal(t, ainsert.Kind(""), kind)
	assert.Equal(t, "INSERT INTO table_name/Values (col1, col2) VALUES", key)
	assert.Equal(t, 0, placeholderCount)
}

func Test_extractKindAndKey_01(t *testing.T) {
	kind, key, placeholderCount := extractKindAndKey("INSERT INTO table_name/Values (col1, col2) VALUES (?, ?)")
	assert.Equal(t, ainsert.ParameterizedValuesOnly, kind)
	assert.Equal(t, "INSERT INTO table_name/Values (col1, col2) VALUES#PLACEHOLDERS#", key)
	assert.Equal(t, 2, placeholderCount)
}

func Test_extractKindAndKey_01B(t *testing.T) {
	kind, key, placeholderCount := extractKindAndKey("INSERT INTO table_name/Values (col1, col2) VALUES (1, 2)")
	assert.Equal(t, ainsert.Kind(""), kind)
	assert.Equal(t, "INSERT INTO table_name/Values (col1, col2) VALUES (1, 2)", key)
	assert.Equal(t, 0, placeholderCount)
}

func Test_extractKindAndKey_02(t *testing.T) {
	kind, key, placeholderCount := extractKindAndKey("INSERT INTO table_name/Values (col1, col2) VALUES (?, ?),(?, ?) AS new ON DUPLICATE KEY UPDATE count = count + new.count")
	assert.Equal(t, ainsert.ParameterizedValuesOnly, kind)
	assert.Equal(t, "INSERT INTO table_name/Values (col1, col2) VALUES#PLACEHOLDERS# AS new ON DUPLICATE KEY UPDATE count = count + new.count", key)
	assert.Equal(t, 4, placeholderCount)
}

func Test_extractKindAndKey_02B(t *testing.T) {
	kind, key, placeholderCount := extractKindAndKey("INSERT INTO table_name/Values (col1, col2) VALUES (?, ?),(?, ?) AS new ON DUPLICATE KEY UPDATE count = count + ?")
	assert.Equal(t, ainsert.Kind(""), kind)
	assert.Equal(t, "INSERT INTO table_name/Values (col1, col2) VALUES (?, ?),(?, ?) AS new ON DUPLICATE KEY UPDATE count = count + ?", key)
	assert.Equal(t, 0, placeholderCount)
}

// isPlaceholderList
func Test_isPlaceholderList_01(t *testing.T) {
	isValid := isPlaceholderList("INVALID PLACEHOLDER LIST")
	assert.False(t, isValid)
}

func Test_isPlaceholderList_02(t *testing.T) {
	isValid := isPlaceholderList("(?, ?, ?), (?, ?, ?)")
	assert.True(t, isValid)
}
