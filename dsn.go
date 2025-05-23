package aerospike

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	defaultInsertCacheMaxEntries = 1000
	defaultBatchSize             = 1000
	defaultConcurrency           = 10
)

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
		host:                  host,
		port:                  iPort,
		namespace:             namespace,
		batchSize:             defaultBatchSize,
		concurrency:           defaultConcurrency,
		Values:                URL.Query(),
		insertCacheMaxEntries: defaultInsertCacheMaxEntries,
	}

	if len(cfg.Values) > 0 {
		if v, ok := cfg.Values["concurrency"]; ok {
			if cfg.concurrency, err = strconv.Atoi(v[0]); err != nil {
				return nil, fmt.Errorf("invalid dsn concurrency: %v", err)
			}
		}
		if v, ok := cfg.Values["disablePool"]; ok {
			if len(v) > 0 {
				cfg.disablePool = v[0] == "true"
			} else {
				cfg.disablePool = true
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
		if v, ok := cfg.Values["insertCacheMaxEntries"]; ok {
			if cfg.insertCacheMaxEntries, err = strconv.Atoi(v[0]); err != nil {
				return nil, fmt.Errorf("invalid dsn insertCacheMaxEntries: %v", err)
			}
		}
		if v, ok := cfg.Values["disableCache"]; ok {
			if len(v) > 0 {
				cfg.disableCache = v[0] == "true"
			} else {
				cfg.disableCache = true
			}
		}
	}

	if isDebugOn() {
		fmt.Printf("Aerospike config: %+v\n", cfg)
	}

	return cfg, nil
}
