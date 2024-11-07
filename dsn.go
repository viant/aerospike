package aerospike

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v6"
	"net/url"
	"strconv"
	"strings"
)

// Config represent Connection config
type Config struct {
	host                string
	port                int
	namespace           string
	ClientPolicy        *as.ClientPolicy
	batchSize           int
	concurrency         int
	maxConcurrentWrite  int
	connectionQueueSize int
	Values              url.Values
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
	}

	if cfg.connectionQueueSize != 0 {
		cfg.ClientPolicy.ConnectionQueueSize = cfg.connectionQueueSize
	}

	if isDebugOn() {
		fmt.Printf("Aerospike config: %+v\n", cfg)
	}

	return cfg, nil
}
