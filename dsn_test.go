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
				host:                  "127.0.0.1",
				port:                  3000,
				namespace:             "namespace_abc",
				batchSize:             defaultBatchSize,
				concurrency:           defaultConcurrency,
				maxConcurrentWrite:    0,
				Values:                url.Values{},
				insertCacheMaxEntries: defaultInsertCacheMaxEntries,
				disablePool:           false,
				disableCache:          false,
			},
		},
		{
			description: "dsn just with all params",
			dsn: "aerospike://127.0.0.1:3000/namespace_abcd?" +
				"concurrency=10" +
				"&batchSize=100" +
				"&maxConcurrentWrite=5" +
				"&insertCacheMaxEntries=17" +
				"&clientPolConnectionQueueSize=200" +
				"&clientPolTimeout=66" +
				"&clientPolIdleTimeout=11" +
				"&clientPolLoginTimeout=33" +
				"&clientPolOpeningConnectionThreshold=550" +
				"&clientPolTendInterval=15" +
				"&clientPolMaxErrorRate=56" +
				"&clientPolErrorRateWindow=7" +
				"&defPolTotalTimeout=301" +
				"&defPolSocketTimeout=302" +
				"&defPolMaxRetries=303" +
				"&defPolSleepBetweenRetries=304" +
				"&defWritePolTotalTimeout=401" +
				"&defWritePolSocketTimeout=402" +
				"&defWritePolMaxRetries=403" +
				"&defWritePolSleepBetweenRetries=404" +
				"&disablePool=true" +
				"&disableCache=true",
			expect: &Config{
				host:                  "127.0.0.1",
				port:                  3000,
				namespace:             "namespace_abcd",
				batchSize:             100,
				concurrency:           10,
				maxConcurrentWrite:    5,
				insertCacheMaxEntries: 17,
				disablePool:           true,
				disableCache:          true,
				Values: url.Values{
					"concurrency":                         []string{"10"},
					"batchSize":                           []string{"100"},
					"maxConcurrentWrite":                  []string{"5"},
					"insertCacheMaxEntries":               []string{"17"},
					"clientPolConnectionQueueSize":        []string{"200"},
					"clientPolTimeout":                    []string{"66"},
					"clientPolIdleTimeout":                []string{"11"},
					"clientPolLoginTimeout":               []string{"33"},
					"clientPolOpeningConnectionThreshold": []string{"550"},
					"clientPolTendInterval":               []string{"15"},
					"clientPolMaxErrorRate":               []string{"56"},
					"clientPolErrorRateWindow":            []string{"7"},
					"defPolTotalTimeout":                  []string{"301"},
					"defPolSocketTimeout":                 []string{"302"},
					"defPolMaxRetries":                    []string{"303"},
					"defPolSleepBetweenRetries":           []string{"304"},
					"defWritePolTotalTimeout":             []string{"401"},
					"defWritePolSocketTimeout":            []string{"402"},
					"defWritePolMaxRetries":               []string{"403"},
					"defWritePolSleepBetweenRetries":      []string{"404"},
					"disablePool":                         []string{"true"},
					"disableCache":                        []string{"true"},
				},
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

func Test_AdjustClientPolicy(t *testing.T) {

	var testCase = []struct {
		description string
		dsn         string
		expect      interface{}
	}{
		{
			description: "adjust client policy",
			dsn: "aerospike://127.0.0.1:3000/namespace_abcd?" +
				"concurrency=10" +
				"&batchSize=100" +
				"&maxConcurrentWrite=5" +
				"&clientPolConnectionQueueSize=200" +
				"&clientPolTimeout=66" +
				"&clientPolIdleTimeout=11" +
				"&clientPolLoginTimeout=33" +
				"&clientPolOpeningConnectionThreshold=550" +
				"&clientPolTendInterval=15" +
				"&clientPolMaxErrorRate=56" +
				"&clientPolErrorRateWindow=7" +
				"&defPolTotalTimeout=301" +
				"&defPolSocketTimeout=302" +
				"&defPolMaxRetries=303" +
				"&defPolSleepBetweenRetries=304" +
				"&defWritePolTotalTimeout=401" +
				"&defWritePolSocketTimeout=402" +
				"&defWritePolMaxRetries=403" +
				"&defWritePolSleepBetweenRetries=404",
			expect: &as.ClientPolicy{
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
		},
	}

	for _, tc := range testCase {
		cfg, err := ParseDSN(tc.dsn)
		if !assert.Nil(t, err, tc.description) {
			continue
		}

		actual := as.NewClientPolicy()

		err = cfg.adjustClientPolicy(actual)
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

func Test_AdjustDefaultPolicy(t *testing.T) {

	var testCase = []struct {
		description string
		dsn         string
		expect      interface{}
	}{
		{
			description: "adjust client policy",
			dsn: "aerospike://127.0.0.1:3000/namespace_abcd?" +
				"concurrency=10" +
				"&batchSize=100" +
				"&maxConcurrentWrite=5" +
				"&clientPolConnectionQueueSize=200" +
				"&clientPolTimeout=66" +
				"&clientPolIdleTimeout=11" +
				"&clientPolLoginTimeout=33" +
				"&clientPolOpeningConnectionThreshold=550" +
				"&clientPolTendInterval=15" +
				"&clientPolMaxErrorRate=56" +
				"&clientPolErrorRateWindow=7" +
				"&defPolTotalTimeout=301" +
				"&defPolSocketTimeout=302" +
				"&defPolMaxRetries=303" +
				"&defPolSleepBetweenRetries=304" +
				"&defWritePolTotalTimeout=401" +
				"&defWritePolSocketTimeout=402" +
				"&defWritePolMaxRetries=403" +
				"&defWritePolSleepBetweenRetries=404",
			expect: &as.BasePolicy{
				ReadModeAP:          as.ReadModeAPOne,
				ReadModeSC:          as.ReadModeSCSession,
				TotalTimeout:        301 * time.Millisecond,
				SocketTimeout:       302 * time.Second,
				MaxRetries:          303,
				SleepBetweenRetries: 304 * time.Millisecond,
				SleepMultiplier:     1.0,
				ReplicaPolicy:       as.SEQUENCE,
				SendKey:             false,
				UseCompression:      false,
			},
		},
	}

	for _, tc := range testCase {
		cfg, err := ParseDSN(tc.dsn)
		if !assert.Nil(t, err, tc.description) {
			continue
		}

		clientPolicy := as.NewClientPolicy()
		client, err := as.NewClientWithPolicy(clientPolicy, cfg.host, cfg.port)
		if !assert.Nil(t, err, tc.description) {
			continue
		}

		actual := client.DefaultPolicy
		err = cfg.adjustDefaultPolicy(actual)
		if !assert.Nil(t, err, tc.description) {
			continue
		}
		//err = cfg.adjustDefaultWritePolicy(client.DefaultWritePolicy)
		//if !assert.Nil(t, err, tc.description) {
		//	continue
		//}

		err = cfg.adjustDefaultPolicy(actual)
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

func Test_AdjustDefaultWritePolicy(t *testing.T) {

	var testCase = []struct {
		description string
		dsn         string
		expect      interface{}
	}{
		{
			description: "adjust client policy",
			dsn: "aerospike://127.0.0.1:3000/namespace_abcd?" +
				"concurrency=10" +
				"&batchSize=100" +
				"&maxConcurrentWrite=5" +
				"&clientPolConnectionQueueSize=200" +
				"&clientPolTimeout=66" +
				"&clientPolIdleTimeout=11" +
				"&clientPolLoginTimeout=33" +
				"&clientPolOpeningConnectionThreshold=550" +
				"&clientPolTendInterval=15" +
				"&clientPolMaxErrorRate=56" +
				"&clientPolErrorRateWindow=7" +
				"&defPolTotalTimeout=301" +
				"&defPolSocketTimeout=302" +
				"&defPolMaxRetries=303" +
				"&defPolSleepBetweenRetries=304" +
				"&defWritePolTotalTimeout=401" +
				"&defWritePolSocketTimeout=402" +
				"&defWritePolMaxRetries=403" +
				"&defWritePolSleepBetweenRetries=404",
			expect: &as.BasePolicy{
				ReadModeAP:          as.ReadModeAPOne,
				ReadModeSC:          as.ReadModeSCSession,
				TotalTimeout:        401 * time.Millisecond,
				SocketTimeout:       402 * time.Second,
				MaxRetries:          403,
				SleepBetweenRetries: 404 * time.Millisecond,
				SleepMultiplier:     1.0,
				ReplicaPolicy:       as.SEQUENCE,
				SendKey:             false,
				UseCompression:      false,
			},
		},
	}

	for _, tc := range testCase {
		cfg, err := ParseDSN(tc.dsn)
		if !assert.Nil(t, err, tc.description) {
			continue
		}

		clientPolicy := as.NewClientPolicy()
		client, err := as.NewClientWithPolicy(clientPolicy, cfg.host, cfg.port)
		if !assert.Nil(t, err, tc.description) {
			continue
		}

		err = cfg.adjustDefaultWritePolicy(client.DefaultWritePolicy)
		if !assert.Nil(t, err, tc.description) {
			continue
		}

		actual := client.DefaultWritePolicy.GetBasePolicy()

		if !assert.Equal(t, tc.expect, actual, tc.description) {
			fmt.Println("************* EXPECTED")
			toolbox.DumpIndent(tc.expect, false)

			fmt.Println("************* ACTUAL")
			toolbox.DumpIndent(actual, false)

		}
	}
}
