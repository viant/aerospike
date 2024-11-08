package aerospike

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"net/url"
	"strconv"
	"time"
)

// Config represent Connection config
type Config struct {
	host               string
	port               int
	namespace          string
	batchSize          int
	concurrency        int
	maxConcurrentWrite int
	Values             url.Values

	// expiry options
	/*
		Key     string
		Secret  string
		Region  string
		CredURL string
		CredKey string
		Token   string
		RoleArn string
	*/
}

func (c *Config) adjustClientPolicy(policy *as.ClientPolicy) error {
	name := "clientPolConnectionQueueSize"
	if v, ok := c.Values[name]; ok {
		value, err := strconv.Atoi(v[0])
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.ConnectionQueueSize = value
	}

	name = "clientPolTimeout"
	if v, ok := c.Values[name]; ok {
		value, err := time.ParseDuration(v[0] + "s")
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.Timeout = value
	}

	name = "clientPolIdleTimeout"
	if v, ok := c.Values[name]; ok {
		value, err := time.ParseDuration(v[0] + "s")
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.IdleTimeout = value
	}

	name = "clientPolLoginTimeout"
	if v, ok := c.Values[name]; ok {
		value, err := time.ParseDuration(v[0] + "s")
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.LoginTimeout = value
	}

	name = "clientPolOpeningConnectionThreshold"
	if v, ok := c.Values[name]; ok {
		value, err := strconv.Atoi(v[0])
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.OpeningConnectionThreshold = value
	}

	name = "clientPolTendInterval"
	if v, ok := c.Values[name]; ok {
		value, err := time.ParseDuration(v[0] + "s")
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.TendInterval = value
	}

	name = "clientPolMaxErrorRate"
	if v, ok := c.Values[name]; ok {
		value, err := strconv.Atoi(v[0])
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.MaxErrorRate = value
	}

	name = "clientPolErrorRateWindow"
	if v, ok := c.Values[name]; ok {
		value, err := strconv.Atoi(v[0])
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.ErrorRateWindow = value
	}

	return nil
}

func (c *Config) adjustDefaultPolicy(policy *as.BasePolicy) error {
	return c.adjustBasePolicy(policy, "defPol")
}

func (c *Config) adjustDefaultWritePolicy(policy *as.WritePolicy) error {
	return c.adjustBasePolicy(policy.GetBasePolicy(), "defWritePol")
}

func (c *Config) adjustBasePolicy(policy *as.BasePolicy, prefix string) error {

	name := prefix + "TotalTimeout"
	if v, ok := c.Values[name]; ok {
		value, err := time.ParseDuration(v[0] + "s")
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.TotalTimeout = value
	}

	name = prefix + "SocketTimeout"
	if v, ok := c.Values[name]; ok {
		value, err := time.ParseDuration(v[0] + "s")
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.SocketTimeout = value
	}

	name = prefix + "MaxRetries"
	if v, ok := c.Values[name]; ok {
		value, err := strconv.Atoi(v[0])
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.MaxRetries = value
	}

	name = prefix + "SleepBetweenRetries"
	if v, ok := c.Values[name]; ok {
		value, err := time.ParseDuration(v[0] + "ms")
		if err != nil {
			return fmt.Errorf("invalid dsn param %v: %v", name, err)
		}
		policy.SleepBetweenRetries = value
	}

	return nil
}
