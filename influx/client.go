package influx

import "github.com/bonitoo-io/influxdb3-go/influx/configs"

// Client provides an interface for interacting with an InfluxDB server, simplifying common operations such as writing, querying.
type Client struct {
	host     string
	database string
	token    string
}

func NewClient(cfg configs.ClientConfigs) (*Client, error) {
	client := &Client{host: *cfg.Host, database: *cfg.Database, token: *cfg.Token}

	return client, nil
}
