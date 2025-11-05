package aerospike

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Focused tests for []byte and *[]byte handling without modifying the large driver_test.go suite.
func Test_QueryContext_Bytes(t *testing.T) {
	type ByteData struct {
		Id   int    `aerospike:"id,pk=true"`
		Data []byte `aerospike:"data"`
	}

	type ByteDataPtr struct {
		Id   int     `aerospike:"id,pk=true"`
		Data *[]byte `aerospike:"data"`
	}

	// Reuse DSN vars defined in driver_test.go
	db, err := sql.Open("aerospike", dsnGlobal+dsnParamsSet[0])
	if !assert.Nil(t, err) {
		return
	}
	defer db.Close()

	// Register sets used in this test
	if _, err = db.Exec("REGISTER SET byteData AS ?", ByteData{}); !assert.Nil(t, err) {
		return
	}
	if _, err = db.Exec("REGISTER SET byteDataPtr AS ?", ByteDataPtr{}); !assert.Nil(t, err) {
		return
	}

	// Clean state
	_, _ = db.Exec("DELETE FROM byteData")
	_, _ = db.Exec("DELETE FROM byteDataPtr")

	// Case 1: []byte round-trip
	in := []byte{1, 2, 3, 4, 5}
	if _, err = db.Exec("INSERT INTO byteData(id,data) VALUES(?,?)", 1, in); !assert.Nil(t, err) {
		return
	}
	// allow a short delay for write visibility
	time.Sleep(50 * time.Millisecond)
	rows, err := db.Query("SELECT id,data FROM byteData WHERE PK = ?", 1)
	if !assert.Nil(t, err) {
		return
	}
	defer rows.Close()
	if rows.Next() {
		var id int
		var out []byte
		if !assert.Nil(t, rows.Scan(&id, &out)) {
			return
		}
		assert.Equal(t, 1, id)
		assert.Equal(t, in, out)
	} else {
		t.Fatalf("no row returned for byteData")
	}

	// Case 2: *[]byte round-trip
	inPtr := []byte{10, 20, 30}
	// Insert uses []byte (Aerospike client does not accept *[]byte)
	if _, err = db.Exec("INSERT INTO byteDataPtr(id,data) VALUES(?,?)", 2, inPtr); !assert.Nil(t, err) {
		return
	}
	// allow a short delay for write visibility
	time.Sleep(50 * time.Millisecond)
	rows2, err := db.Query("SELECT id,data FROM byteDataPtr WHERE PK = ?", 2)
	if !assert.Nil(t, err) {
		return
	}
	defer rows2.Close()
	if rows2.Next() {
		rec := ByteDataPtr{}
		if !assert.Nil(t, rows2.Scan(&rec.Id, &rec.Data)) {
			return
		}
		if assert.NotNil(t, rec.Data, "Data pointer should not be nil") {
			assert.Equal(t, inPtr, *rec.Data)
		}
		assert.Equal(t, 2, rec.Id)
	} else {
		t.Fatalf("no row returned for byteDataPtr")
	}
}
