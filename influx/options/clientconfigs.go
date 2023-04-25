package options

// ClientConfigs contains configuration for a client instance. The configuration can be changed via setters.
type ClientConfigs struct {
	// The hostname or IP address of the InfluxDB server.
	Host *string
	// The database to be used for InfluxDB operations.
	Database *string
	// The authentication token for accessing the InfluxDB server.
	Token *string
}

// SetHost specifies hostname or IP address of the InfluxDB server.
func (c *ClientConfigs) SetHost(host string) *ClientConfigs {
	c.Host = &host
	return c
}

// SetDatabase specifies database to be used for InfluxDB operations.
func (c *ClientConfigs) SetDatabase(database string) *ClientConfigs {
	c.Database = &database
	return c
}

// SetToken specifies authentication token for accessing the InfluxDB server.
func (c *ClientConfigs) SetToken(token string) *ClientConfigs {
	c.Token = &token
	return c
}
