package influx

import (
	"net/http"
)

// ClientConfig holds the parameters for creating a new client.
// The only mandatory field is ServerURL. AuthToken is also important
// if authentication was not done outside this client.
type ClientConfig struct {
	// Host holds the URL of the InfluxDB server to connect to.
	// This must be non-empty. E.g. http://localhost:8086
	Host string

	// Token holds the authorization token for the API.
	// This can be obtained through the GUI web browser interface.
	Token string

	// Organization is name or ID of organization where data (databases, users, tasks, etc.) belongs to
	// Optional for InfluxDB Cloud
	Organization string

	// HTTPClient is used to make API requests.
	//
	// This can be used to specify a custom TLS configuration
	// (TLSClientConfig), a custom request timeout (Timeout),
	// or other customization as required.
	//
	// It HTTPClient is nil, http.DefaultClient will be used.
	HTTPClient *http.Client

	// Write options
	WriteOptions WriteOptions

	// Default HTTP headers to be included in requests
	Headers http.Header
}

