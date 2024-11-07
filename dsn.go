package aerospike

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config represent Connection config
type Config struct {
	host                       string
	port                       int
	namespace                  string
	ClientPolicy               *as.ClientPolicy
	batchSize                  int
	concurrency                int
	maxConcurrentWrite         int
	connectionQueueSize        int
	Values                     url.Values
	timeout                    time.Duration
	idleTimeout                time.Duration
	loginTimeout               time.Duration
	openingConnectionThreshold int
	tendInterval               time.Duration
	maxErrorRate               int
	errorRateWindow            int

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

func ParseDSN(dsn string) (*Config, error) {
	URL, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn: %v", err)
	}
	if URL.Scheme != scheme {
		return nil, fmt.Errorf("invalid dsn scheme, expected %v, but had: %v", scheme, URL.Scheme)
	}

	host := URL.Hostname()
	if len(host) == 0 {
		return nil, fmt.Errorf("invalid dsn - missing host")
	}

	port := URL.Port()
	if len(port) == 0 {
		return nil, fmt.Errorf("invalid dsn - missing port")
	}

	iPort, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("unable to parse dsn port due to: %w", err)
	}

	namespace := strings.Trim(URL.Path, "/")
	if len(namespace) == 0 {
		return nil, fmt.Errorf("invalid dsn - missing namespace")
	}

	cfg := &Config{
		host:         host,
		port:         iPort,
		namespace:    namespace,
		batchSize:    1000,
		concurrency:  10,
		ClientPolicy: as.NewClientPolicy(),
		Values:       URL.Query(),
	}

	if len(cfg.Values) > 0 {
		if v, ok := cfg.Values["concurrency"]; ok {
			if cfg.concurrency, err = strconv.Atoi(v[0]); err != nil {
				return nil, fmt.Errorf("invalid dsn concurrency: %v", err)
			}
		}
		if v, ok := cfg.Values["maxConcurrentWrite"]; ok {
			if cfg.maxConcurrentWrite, err = strconv.Atoi(v[0]); err != nil {
				return nil, fmt.Errorf("invalid dsn concurrency: %v", err)
			}
		}
		if v, ok := cfg.Values["batchSize"]; ok {
			if cfg.batchSize, err = strconv.Atoi(v[0]); err != nil {
				return nil, fmt.Errorf("invalid dsn batchSize: %v", err)
			}
		}

		if v, ok := cfg.Values["connectionQueueSize"]; ok {
			if cfg.connectionQueueSize, err = strconv.Atoi(v[0]); err != nil {
				return nil, fmt.Errorf("invalid dsn batchSize: %v", err)
			}
		}

		if v, ok := cfg.Values["timeout"]; ok {
			if cfg.timeout, err = time.ParseDuration(v[0] + "s"); err != nil {
				return nil, fmt.Errorf("invalid dsn timeout: %v", err)
			}
		}

		if v, ok := cfg.Values["idleTimeout"]; ok {
			if cfg.idleTimeout, err = time.ParseDuration(v[0] + "s"); err != nil {
				return nil, fmt.Errorf("invalid dsn idleTimeout: %v", err)
			}
		}

		if v, ok := cfg.Values["loginTimeout"]; ok {
			if cfg.loginTimeout, err = time.ParseDuration(v[0] + "s"); err != nil {
				return nil, fmt.Errorf("invalid dsn loginTimeout: %v", err)
			}
		}

		if v, ok := cfg.Values["openingConnectionThreshold"]; ok {
			if cfg.openingConnectionThreshold, err = strconv.Atoi(v[0]); err != nil {
				return nil, fmt.Errorf("invalid dsn openingConnectionThreshold: %v", err)
			}
		}

		//if v, ok := cfg.Values["failIfNotConnected"]; ok {
		//	if cfg.failIfNotConnected, err = strconv.ParseBool(v[0]); err != nil {
		//		return nil, fmt.Errorf("invalid dsn failIfNotConnected: %v", err)
		//	}
		//}

		if v, ok := cfg.Values["tendInterval"]; ok {
			if cfg.tendInterval, err = time.ParseDuration(v[0] + "s"); err != nil {
				return nil, fmt.Errorf("invalid dsn tendInterval: %v", err)
			}
		}

		//if v, ok := cfg.Values["limitConnectionsToQueueSize"]; ok {
		//	if cfg.limitConnectionsToQueueSize, err = strconv.ParseBool(v[0]); err != nil {
		//		return nil, fmt.Errorf("invalid dsn limitConnectionsToQueueSize: %v", err)
		//	}
		//}
		//
		//if v, ok := cfg.Values["ignoreOtherSubnetAliases"]; ok {
		//	if cfg.ignoreOtherSubnetAliases, err = strconv.ParseBool(v[0]); err != nil {
		//		return nil, fmt.Errorf("invalid dsn ignoreOtherSubnetAliases: %v", err)
		//	}
		//}

		if v, ok := cfg.Values["maxErrorRate"]; ok {
			if cfg.maxErrorRate, err = strconv.Atoi(v[0]); err != nil {
				return nil, fmt.Errorf("invalid dsn maxErrorRate: %v", err)
			}
		}

		if v, ok := cfg.Values["errorRateWindow"]; ok {
			if cfg.errorRateWindow, err = strconv.Atoi(v[0]); err != nil {
				return nil, fmt.Errorf("invalid dsn errorRateWindow: %v", err)
			}
		}

		//if v, ok := cfg.Values["seedOnlyCluster"]; ok {
		//	if cfg.seedOnlyCluster, err = strconv.ParseBool(v[0]); err != nil {
		//		return nil, fmt.Errorf("invalid dsn seedOnlyCluster: %v", err)
		//	}
		//}
	}

	if cfg.connectionQueueSize != 0 {
		cfg.ClientPolicy.ConnectionQueueSize = cfg.connectionQueueSize
	}

	if cfg.timeout != 0 {
		cfg.ClientPolicy.Timeout = cfg.timeout
	}

	if cfg.idleTimeout != 0 {
		cfg.ClientPolicy.IdleTimeout = cfg.idleTimeout
	}

	if cfg.loginTimeout != 0 {
		cfg.ClientPolicy.LoginTimeout = cfg.loginTimeout
	}

	if cfg.openingConnectionThreshold != 0 {
		cfg.ClientPolicy.OpeningConnectionThreshold = cfg.openingConnectionThreshold
	}

	if cfg.tendInterval != 0 {
		cfg.ClientPolicy.TendInterval = cfg.tendInterval
	}

	if cfg.maxErrorRate != 0 {
		cfg.ClientPolicy.MaxErrorRate = cfg.maxErrorRate
	}

	if cfg.errorRateWindow != 0 {
		cfg.ClientPolicy.ErrorRateWindow = cfg.errorRateWindow
	}

	if isDebugOn() {
		fmt.Printf("Aerospike config: %+v\n", cfg)
	}

	return cfg, nil
}
