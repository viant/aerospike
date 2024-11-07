package aerospike

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"net/url"
	"testing"
)

func Test_ParseDSN(t *testing.T) {

	var testCase = []struct {
		description string
		dsn         string
		expect      interface{}
	}{
		{
			description: "dsn just with namespace",
			dsn:         "aerospike://127.0.0.1:3000/namespace_abc",
			expect: &Config{
				host:                "127.0.0.1",
				port:                3000,
				namespace:           "namespace_abc",
				ClientPolicy:        as.NewClientPolicy(),
				batchSize:           1000,
				concurrency:         10,
				maxConcurrentWrite:  0,
				Values:              url.Values{},
				connectionQueueSize: 0,
			},
		},
		{
			description: "dsn just with all params",
			dsn:         "aerospike://127.0.0.1:3000/namespace_abc?concurrency=10&maxConcurrentWrite=5&batchSize=100&connectionQueueSize=200",
			expect: &Config{
				host:               "127.0.0.1",
				port:               3000,
				namespace:          "namespace_abc",
				ClientPolicy:       as.NewClientPolicy(),
				batchSize:          100,
				concurrency:        10,
				maxConcurrentWrite: 5,
				Values: url.Values{
					"batchSize":           []string{"100"},
					"concurrency":         []string{"10"},
					"connectionQueueSize": []string{"200"},
					"maxConcurrentWrite":  []string{"5"},
				},
				connectionQueueSize: 200,
			},
		},
	}

	for _, tc := range testCase {
		actual, err := ParseDSN(tc.dsn)
		if !assert.Nil(t, err, tc.description) {
			continue
		}

		if !assert.Equal(t, tc.expect, actual, tc.description) {
			fmt.Println("************* EXPECTED")
			toolbox.DumpIndent(tc.expect, false)

			fmt.Println("************* ACTUAL")
			toolbox.DumpIndent(actual, false)

		}
	}
}
