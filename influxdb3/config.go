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
	"net/http"
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
