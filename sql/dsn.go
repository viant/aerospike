package sql

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v4"
	"net/url"
	"strconv"
	"strings"
)

// Config represent Connection config
type Config struct {
	Host         string
	Port         int
	Namespace    string
	ClientPolicy *as.ClientPolicy
	url.Values
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
		Host:         host,
		Port:         iPort,
		Namespace:    namespace,
		ClientPolicy: as.NewClientPolicy(), //TODO
		Values:       URL.Query(),          //TODO
	}

	//if len(cfg.Values) > 0 { //TODO
	//	if _, ok := cfg.Values[endpoint]; ok {
	//		cfg.Endpoint = cfg.Values.Get(endpoint)
	//	}
	//}

	return cfg, nil
}
