/*
 The MIT License

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
*/

package influxdb3

import (
	"errors"
	"fmt"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

const (
	envInfluxHost     = "INFLUX_HOST"
	envInfluxToken    = "INFLUX_TOKEN"
	envInfluxOrg      = "INFLUX_ORG"
	envInfluxDatabase = "INFLUX_DATABASE"
)

// ClientConfig holds the parameters for creating a new client.
// The only mandatory field is Host. Token is also important
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

	// Database used by the client.
	Database string

	// HTTPClient is used to make API requests.
	//
	// This can be used to specify a custom TLS configuration
	// (TLSClientConfig), a custom request timeout (Timeout),
	// or other customization as required.
	//
	// It HTTPClient is nil, http.DefaultClient will be used.
	HTTPClient *http.Client

	// Write options
	WriteOptions *WriteOptions

	// Default HTTP headers to be included in requests
	Headers http.Header
}

// validate validates the config.
func (c *ClientConfig) validate() error {
	if c.Host == "" {
		return errors.New("empty host")
	}
	if c.Token == "" {
		return errors.New("no token specified")
	}

	return nil
}

// parse initializes the client config from provided connection string.
func (c *ClientConfig) parse(connectionString string) error {
	u, err := url.Parse(connectionString)
	if err != nil {
		return err
	}

	if !(u.Scheme == "http" || u.Scheme == "https") {
		return errors.New("only http or https is supported")
	}

	values := u.Query()

	u.RawQuery = ""
	c.Host = u.String()

	token, ok := values["token"]
	if ok {
		c.Token = token[0]
	}
	org, ok := values["org"]
	if ok {
		c.Organization = org[0]
	}
	database, ok := values["database"]
	if ok {
		c.Database = database[0]
	}
	var writeOptions *WriteOptions
	precision, ok := values["precision"]
	if ok {
		if writeOptions == nil {
			writeOptions = &WriteOptions{}
		}
		switch precision[0] {
		case "ns":
			writeOptions.Precision = lineprotocol.Nanosecond
		case "us":
			writeOptions.Precision = lineprotocol.Microsecond
		case "ms":
			writeOptions.Precision = lineprotocol.Millisecond
		case "s":
			writeOptions.Precision = lineprotocol.Second
		default:
			return fmt.Errorf("unsupported precision %s", precision[0])
		}
	}
	gzipThreshold, ok := values["gzipThreshold"]
	if ok {
		if writeOptions == nil {
			writeOptions = &WriteOptions{}
		}
		writeOptions.GzipThreshold, err = strconv.Atoi(gzipThreshold[0])
		if err != nil {
			return err
		}
	}

	if writeOptions != nil {
		c.WriteOptions = writeOptions
	}

	return nil
}

// env initializes the client config from environment variables.
func (c *ClientConfig) env() error {
	c.Host = os.Getenv(envInfluxHost)
	c.Token = os.Getenv(envInfluxToken)
	c.Organization = os.Getenv(envInfluxOrg)
	c.Database = os.Getenv(envInfluxDatabase)

	return nil
}
