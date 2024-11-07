package aerospike

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"net/url"
	"testing"
	"time"
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
			dsn:         "aerospike://127.0.0.1:3000/namespace_abcd?concurrency=10&maxConcurrentWrite=5&batchSize=100&connectionQueueSize=200&timeout=66&idleTimeout=11&loginTimeout=33&openingConnectionThreshold=550&tendInterval=15&maxErrorRate=56&errorRateWindow=7",
			expect: &Config{
				host:      "127.0.0.1",
				port:      3000,
				namespace: "namespace_abcd",
				ClientPolicy: &as.ClientPolicy{
					AuthMode:                    as.AuthModeInternal,
					Timeout:                     66 * time.Second,
					IdleTimeout:                 11 * time.Second,
					LoginTimeout:                33 * time.Second,
					ConnectionQueueSize:         200,
					OpeningConnectionThreshold:  550,
					FailIfNotConnected:          true,
					TendInterval:                15 * time.Second,
					LimitConnectionsToQueueSize: true,
					IgnoreOtherSubnetAliases:    false,
					MaxErrorRate:                56,
					ErrorRateWindow:             7,
					SeedOnlyCluster:             false,
				},
				batchSize:                  100,
				concurrency:                10,
				maxConcurrentWrite:         5,
				timeout:                    66 * time.Second,
				idleTimeout:                11 * time.Second,
				loginTimeout:               33 * time.Second,
				openingConnectionThreshold: 550,
				tendInterval:               15 * time.Second,
				maxErrorRate:               56,
				errorRateWindow:            7,
				Values: url.Values{
					"batchSize":                  []string{"100"},
					"concurrency":                []string{"10"},
					"connectionQueueSize":        []string{"200"},
					"maxConcurrentWrite":         []string{"5"},
					"timeout":                    []string{"66"},
					"idleTimeout":                []string{"11"},
					"loginTimeout":               []string{"33"},
					"openingConnectionThreshold": []string{"550"},
					"tendInterval":               []string{"15"},
					"maxErrorRate":               []string{"56"},
					"errorRateWindow":            []string{"7"},
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
