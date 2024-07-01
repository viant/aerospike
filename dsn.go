package aerospike

import (
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"net/url"
	"strconv"
	"strings"
)

// Config represent Connection config
type Config struct {
	Host         string
	Port         int
	ClientPolicy *as.ClientPolicy

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

	cfg := &Config{}

	splitted := strings.Split(URL.Host, ":")

	if len(splitted) != 2 {
		return nil, fmt.Errorf("invalid dsn - expected format: host:port")
	}

	host := splitted[0]
	if host != "" {
		//if !strings.Contains(host, "://") {
		//	host = "http://" + host
		//}
		cfg.Host = host
	}

	port := URL.Port()
	aPort, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("unable to parse dsn port due to: %w", err)
	}
	cfg.Port = aPort

	cfg.ClientPolicy = as.NewClientPolicy() //TODO: populate client policy

	return cfg, nil
}
