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
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	_, err := New(ClientConfig{})
	require.Error(t, err)
	assert.Equal(t, "empty host", err.Error())

	c, err := New(ClientConfig{Host: "http://localhost:8086"})
	require.Nil(t, c)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "no token specified")

	c, err = New(ClientConfig{Host: "localhost\n", Token: "my-token"})
	require.Nil(t, c)
	if assert.Error(t, err) {
		expectedMessage := "parsing host URL:"
		assert.True(t, strings.HasPrefix(err.Error(), expectedMessage), "\nexpected prefix : %s\nactual message  : %s", expectedMessage, err.Error())
	}

	c, err = New(ClientConfig{Host: "http@localhost:8086", Token: "my-token"})
	require.Nil(t, c)
	assert.Error(t, err)
	assert.Equal(t, "parsing host URL: parse \"http@localhost:8086/\": first path segment in URL cannot contain colon", err.Error())

	c, err = New(ClientConfig{Host: "http://localhost:8086", Token: "my-token"})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, "http://localhost:8086", c.config.Host)
	assert.Equal(t, "http://localhost:8086/api/", c.apiURL.String())
	assert.Equal(t, "Token my-token", c.authorization)

	c, err = New(ClientConfig{Host: "http://localhost:8086", Token: "my-token", Organization: "my-org", Database: "my-database"})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, "Token my-token", c.authorization)
	assert.Equal(t, "my-database", c.config.Database)
	assert.Equal(t, "my-org", c.config.Organization)
	assert.EqualValues(t, DefaultWriteOptions, *c.config.WriteOptions)

	c, err = New(ClientConfig{
		Host:         "http://localhost:8086",
		Token:        "my-token",
		AuthScheme:   "my-auth-scheme",
		Organization: "my-org",
		Database:     "my-database",
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, "my-auth-scheme my-token", c.authorization)
	assert.Equal(t, "my-database", c.config.Database)
	assert.Equal(t, "my-org", c.config.Organization)
	assert.EqualValues(t, DefaultWriteOptions, *c.config.WriteOptions)
}

func TestNewWithCertificates(t *testing.T) {
	// Valid certificates.
	certFilePath := filepath.Join("testdata", "valid_certs.pem")
	c, err := New(ClientConfig{
		Host:             "https://localhost:8086",
		Token:            "my-token",
		SSLRootsFilePath: certFilePath,
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, certFilePath, c.config.SSLRootsFilePath)

	// Invalid certificates.
	certFilePath = filepath.Join("testdata", "invalid_certs.pem")
	c, err = New(ClientConfig{
		Host:             "https://localhost:8086",
		Token:            "my-token",
		SSLRootsFilePath: certFilePath,
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, certFilePath, c.config.SSLRootsFilePath)

	// Missing certificates file.
	certFilePath = filepath.Join("testdata", "non-existing-file")
	c, err = New(ClientConfig{
		Host:             "https://localhost:8086",
		Token:            "my-token",
		SSLRootsFilePath: certFilePath,
	})
	assert.Nil(t, c)
	require.Error(t, err)
	assert.ErrorContains(t, err, "error reading testdata/non-existing-file")
}

func TestNewWithProxy(t *testing.T) {
	defer func() {
		// Cleanup: unset proxy.
		os.Unsetenv("HTTPS_PROXY")
	}()

	// Invalid proxy url.
	c, err := New(ClientConfig{
		Host:  "http://localhost:8086",
		Token: "my-token",
		Proxy: "http://proxy:invalid-port",
	})
	assert.Nil(t, c)
	require.Error(t, err)
	assert.ErrorContains(t, err, "parsing proxy URL")

	// Valid proxy url.
	c, err = New(ClientConfig{
		Host:  "http://localhost:8086",
		Token: "my-token",
		Proxy: "http://proxy:8888",
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, "http://proxy:8888", c.config.Proxy)

	// Valid proxy url with HTTPS_PROXY env already set.
	//nolint:usetesting
	setEnvErr := os.Setenv("HTTPS_PROXY", "http://another-proxy:8888")
	if setEnvErr != nil {
		t.Fatal(setEnvErr)
	}
	c, err = New(ClientConfig{
		Host:  "http://localhost:8086",
		Token: "my-token",
		Proxy: "http://proxy:8888",
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, "http://proxy:8888", c.config.Proxy)
}

func TestNewWithDefaults(t *testing.T) {
	// Test default timeout.
	c, err := New(ClientConfig{
		Host:  "http://localhost:8086",
		Token: "my-token",
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, defaultTimeout, c.config.HTTPClient.Timeout)
	transport, ok := c.config.HTTPClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Equal(t, defaultIdleConnectionTimeout, transport.IdleConnTimeout)
	assert.Equal(t, defaultMaxIdleConnections, transport.MaxIdleConns)
	assert.Equal(t, defaultMaxIdleConnections, transport.MaxIdleConnsPerHost)
}

func TestNewWithTimeout(t *testing.T) {
	// Test no timeout.
	c, err := New(ClientConfig{
		Host:    "http://localhost:8086",
		Token:   "my-token",
		Timeout: -1, // no timeout
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, time.Duration(0), c.config.HTTPClient.Timeout)

	// Test Timeout set.
	timeout := 123 * time.Second
	c, err = New(ClientConfig{
		Host:    "http://localhost:8086",
		Token:   "my-token",
		Timeout: timeout,
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, timeout, c.config.HTTPClient.Timeout)

	// Test Timeout set with custom client.
	customClient := http.Client{Timeout: 456 * time.Second}
	c, err = New(ClientConfig{
		Host:       "http://localhost:8086",
		Token:      "my-token",
		HTTPClient: &customClient,
		Timeout:    timeout,
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, timeout, c.config.HTTPClient.Timeout)
}

func TestNewWithIdleConnectionTimeout(t *testing.T) {
	// Test no IdleConnectionTimeout.
	c, err := New(ClientConfig{
		Host:                  "http://localhost:8086",
		Token:                 "my-token",
		IdleConnectionTimeout: -1, // no timeout
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	transport, ok := c.config.HTTPClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Equal(t, time.Duration(0), transport.IdleConnTimeout)

	// Test IdleConnectionTimeout set.
	timeout := 123 * time.Second
	c, err = New(ClientConfig{
		Host:                  "http://localhost:8086",
		Token:                 "my-token",
		IdleConnectionTimeout: timeout,
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	transport, ok = c.config.HTTPClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Equal(t, timeout, transport.IdleConnTimeout)

	// Test IdleConnectionTimeout set with custom client.
	customClient := http.Client{Timeout: 456 * time.Second}
	c, err = New(ClientConfig{
		Host:                  "http://localhost:8086",
		Token:                 "my-token",
		HTTPClient:            &customClient,
		IdleConnectionTimeout: timeout,
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	transport, ok = c.config.HTTPClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Equal(t, timeout, transport.IdleConnTimeout)
}

func TestNewWithMaxIdleConnections(t *testing.T) {
	// Test no MaxIdleConnections.
	c, err := New(ClientConfig{
		Host:               "http://localhost:8086",
		Token:              "my-token",
		MaxIdleConnections: -1, // no limit
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	transport, ok := c.config.HTTPClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Equal(t, 0, transport.MaxIdleConns)
	assert.Equal(t, 0, transport.MaxIdleConnsPerHost)

	// Test MaxIdleConnections set.
	value := 123
	c, err = New(ClientConfig{
		Host:               "http://localhost:8086",
		Token:              "my-token",
		MaxIdleConnections: value,
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	transport, ok = c.config.HTTPClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Equal(t, value, transport.MaxIdleConns)
	assert.Equal(t, value, transport.MaxIdleConnsPerHost)

	// Test  set with custom client.
	customClient := http.Client{Transport: &http.Transport{
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
	}}
	c, err = New(ClientConfig{
		Host:               "http://localhost:8086",
		Token:              "my-token",
		HTTPClient:         &customClient,
		MaxIdleConnections: value,
	})
	require.NoError(t, err)
	assert.NotNil(t, c)
	transport, ok = c.config.HTTPClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.Equal(t, value, transport.MaxIdleConns)
	assert.Equal(t, value, transport.MaxIdleConnsPerHost)
}

func TestURLs(t *testing.T) {
	urls := []struct {
		HostURL      string
		serverAPIURL string
	}{
		{"http://host:8086", "http://host:8086/api/"},
		{"http://host:8086/", "http://host:8086/api/"},
		{"http://host:8086/path", "http://host:8086/path/api/"},
		{"http://host:8086/path/", "http://host:8086/path/api/"},
		{"http://host:8086/path1/path2/path3", "http://host:8086/path1/path2/path3/api/"},
		{"http://host:8086/path1/path2/path3/", "http://host:8086/path1/path2/path3/api/"},
	}
	for _, turl := range urls {
		t.Run(turl.HostURL, func(t *testing.T) {
			c, err := New(ClientConfig{Host: turl.HostURL, Token: "my-token"})
			require.NoError(t, err)
			assert.Equal(t, turl.HostURL, c.config.Host)
			assert.Equal(t, turl.serverAPIURL, c.apiURL.String())
		})
	}
}

func TestNewFromConnectionString(t *testing.T) {
	testCases := []struct {
		name string
		cs   string
		cfg  *ClientConfig
		err  string
	}{
		{
			name: "invalid URL",
			cs:   "|http::8086?token=abc?",
			err:  "cannot contain colon",
		},
		{
			name: "unsupported scheme",
			cs:   "host:8086",
			err:  "only http or https is supported",
		},
		{
			name: "no token",
			cs:   "https://host:8086",
			err:  "no token specified",
		},
		{
			name: "only token",
			cs:   "https://host:8086?token=abc",
			cfg: &ClientConfig{
				Host:         "https://host:8086",
				Token:        "abc",
				WriteOptions: &DefaultWriteOptions,
			},
		},
		{
			name: "basic",
			cs:   "https://host:8086?token=abc&org=my-org&database=my-db",
			cfg: &ClientConfig{
				Host:         "https://host:8086",
				Token:        "abc",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &DefaultWriteOptions,
			},
		},
		{
			name: "with auth scheme",
			cs:   "https://host:8086?token=abc&authScheme=Custom&org=my-org&database=my-db",
			cfg: &ClientConfig{
				Host:         "https://host:8086",
				Token:        "abc",
				AuthScheme:   "Custom",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &DefaultWriteOptions,
			},
		},
		{
			name: "with write options",
			cs:   "https://host:8086?token=abc&org=my-org&database=my-db&precision=ms&gzipThreshold=64&writeNoSync=true",
			cfg: &ClientConfig{
				Host:         "https://host:8086",
				Token:        "abc",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &WriteOptions{
					Precision:     lineprotocol.Millisecond,
					GzipThreshold: 64,
					NoSync:        true,
				},
			},
		},
		{
			name: "with precision long value - second",
			cs:   "https://host:8086?token=abc&org=my-org&database=my-db&precision=second",
			cfg: &ClientConfig{
				Host:         "https://host:8086",
				Token:        "abc",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &WriteOptions{
					GzipThreshold: DefaultWriteOptions.GzipThreshold,
					Precision:     lineprotocol.Second,
				},
			},
		},
		{
			name: "with precision long value - microsecond",
			cs:   "https://host:8086?token=abc&org=my-org&database=my-db&precision=microsecond",
			cfg: &ClientConfig{
				Host:         "https://host:8086",
				Token:        "abc",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &WriteOptions{
					GzipThreshold: DefaultWriteOptions.GzipThreshold,
					Precision:     lineprotocol.Microsecond,
				},
			},
		},
		{
			name: "invalid gzip threshold",
			cs:   "https://host:8086?token=abc&gzipThreshold=a0",
			err:  "invalid syntax",
		},
		{
			name: "invalid writeNoSync",
			cs:   "https://host:8086?token=abc&writeNoSync=truuu",
			err:  "invalid syntax",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewFromConnectionString(tc.cs)
			if tc.err != "" {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, c)
				assert.Equal(t, tc.cfg.Host, c.config.Host)
				assert.Equal(t, tc.cfg.Token, c.config.Token)
				assert.Equal(t, tc.cfg.AuthScheme, c.config.AuthScheme)
				assert.Equal(t, tc.cfg.Organization, c.config.Organization)
				assert.Equal(t, tc.cfg.Database, c.config.Database)
				assert.Equal(t, tc.cfg.WriteOptions, c.config.WriteOptions)
			}
		})
	}
}

func TestNewFromEnv(t *testing.T) {
	testCases := []struct {
		name string
		vars map[string]string
		cfg  *ClientConfig
		err  string
	}{
		{
			name: "no host",
			vars: map[string]string{},
			err:  "empty host",
		},
		{
			name: "no token",
			vars: map[string]string{
				"INFLUX_HOST": "http://host:8086",
			},
			err: "no token specified",
		},
		{
			name: "minimal",
			vars: map[string]string{
				"INFLUX_HOST":  "http://host:8086",
				"INFLUX_TOKEN": "abc",
			},
			cfg: &ClientConfig{
				Host:         "http://host:8086",
				Token:        "abc",
				WriteOptions: &DefaultWriteOptions,
			},
		},
		{
			name: "basic",
			vars: map[string]string{
				"INFLUX_HOST":     "http://host:8086",
				"INFLUX_TOKEN":    "abc",
				"INFLUX_ORG":      "my-org",
				"INFLUX_DATABASE": "my-db",
			},
			cfg: &ClientConfig{
				Host:         "http://host:8086",
				Token:        "abc",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &DefaultWriteOptions,
			},
		},
		{
			name: "with auth scheme",
			vars: map[string]string{
				"INFLUX_HOST":        "http://host:8086",
				"INFLUX_TOKEN":       "abc",
				"INFLUX_AUTH_SCHEME": "Custom",
				"INFLUX_ORG":         "my-org",
				"INFLUX_DATABASE":    "my-db",
			},
			cfg: &ClientConfig{
				Host:         "http://host:8086",
				Token:        "abc",
				AuthScheme:   "Custom",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &DefaultWriteOptions,
			},
		},
		{
			name: "with write options",
			vars: map[string]string{
				"INFLUX_HOST":           "http://host:8086",
				"INFLUX_TOKEN":          "abc",
				"INFLUX_ORG":            "my-org",
				"INFLUX_DATABASE":       "my-db",
				"INFLUX_PRECISION":      "ms",
				"INFLUX_GZIP_THRESHOLD": "64",
				"INFLUX_WRITE_NO_SYNC":  "true",
			},
			cfg: &ClientConfig{
				Host:         "http://host:8086",
				Token:        "abc",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &WriteOptions{
					Precision:     lineprotocol.Millisecond,
					GzipThreshold: 64,
					NoSync:        true,
				},
			},
		},
		{
			name: "with precision long value",
			vars: map[string]string{
				"INFLUX_HOST":      "http://host:8086",
				"INFLUX_TOKEN":     "abc",
				"INFLUX_ORG":       "my-org",
				"INFLUX_DATABASE":  "my-db",
				"INFLUX_PRECISION": "nanosecond",
			},
			cfg: &ClientConfig{
				Host:         "http://host:8086",
				Token:        "abc",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &WriteOptions{
					Precision:     lineprotocol.Nanosecond,
					GzipThreshold: DefaultWriteOptions.GzipThreshold,
					NoSync:        DefaultWriteOptions.NoSync,
				},
			},
		},
		{
			name: "invalid precision",
			vars: map[string]string{
				"INFLUX_HOST":      "http://host:8086",
				"INFLUX_TOKEN":     "abc",
				"INFLUX_PRECISION": "xs",
			},
			err: "unsupported precision",
		},
		{
			name: "invalid gzip threshold",
			vars: map[string]string{
				"INFLUX_HOST":           "http://host:8086",
				"INFLUX_TOKEN":          "abc",
				"INFLUX_GZIP_THRESHOLD": "a0",
			},
			err: "invalid syntax",
		},
		{
			name: "invalid writeNoSync env",
			vars: map[string]string{
				"INFLUX_HOST":          "http://host:8086",
				"INFLUX_TOKEN":         "abc",
				"INFLUX_WRITE_NO_SYNC": "truuu",
			},
			err: "invalid syntax",
		},
	}
	clearEnv := func() {
		os.Unsetenv(envInfluxHost)
		os.Unsetenv(envInfluxToken)
		os.Unsetenv(envInfluxAuthScheme)
		os.Unsetenv(envInfluxOrg)
		os.Unsetenv(envInfluxDatabase)
		os.Unsetenv(envInfluxPrecision)
		os.Unsetenv(envInfluxGzipThreshold)
		os.Unsetenv(envInfluxWriteNoSync)
	}
	setEnv := func(vars map[string]string) {
		for k, v := range vars {
			t.Setenv(k, v)
		}
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv()
			setEnv(tc.vars)
			c, err := NewFromEnv()
			if tc.err != "" {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, c)
				assert.Equal(t, tc.cfg.Host, c.config.Host)
				assert.Equal(t, tc.cfg.Token, c.config.Token)
				assert.Equal(t, tc.cfg.AuthScheme, c.config.AuthScheme)
				assert.Equal(t, tc.cfg.Organization, c.config.Organization)
				assert.Equal(t, tc.cfg.Database, c.config.Database)
				assert.Equal(t, tc.cfg.WriteOptions, c.config.WriteOptions)
			}
		})
	}
}

func TestMakeAPICall(t *testing.T) {
	html := `<html><body><h1>Response</h1></body></html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(html))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	turl, err := url.Parse(ts.URL)
	require.NoError(t, err)
	res, err := client.makeAPICall(context.Background(), httpParams{
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     nil,
		body:        nil,
	})
	assert.NotNil(t, res)
	assert.NoError(t, err)
	assert.Equal(t, "Token my-token", res.Request.Header.Get("Authorization"))
	assert.NoError(t, err)
	_ = res.Body.Close()

	res, err = client.makeAPICall(context.Background(), httpParams{
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     http.Header{"Authorization": {"Bearer management-api-token"}},
		body:        nil,
	})
	assert.Equal(t, "Bearer management-api-token", res.Request.Header.Get("Authorization"))
	assert.NoError(t, err)
	_ = res.Body.Close()

	client, err = New(ClientConfig{Host: ts.URL, Token: "my-token", AuthScheme: "Bearer"})
	require.NoError(t, err)
	res, err = client.makeAPICall(context.Background(), httpParams{
		endpointURL: turl,
		httpMethod:  "GET",
	})
	assert.Equal(t, "Bearer my-token", res.Request.Header.Get("Authorization"))
	assert.NoError(t, err)
	_ = res.Body.Close()
}

func TestResolveErrorMessage(t *testing.T) {
	errMsg := "compilation failed: error at @1:170-1:171: invalid expression @1:167-1:168: |"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":"invalid","message":"` + errMsg + `"}`))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	turl, err := url.Parse(ts.URL)
	require.NoError(t, err)
	res, err := client.makeAPICall(context.Background(), httpParams{ //nolint:bodyclose
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     nil,
		body:        nil,
	})
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Equal(t, "invalid: "+errMsg, err.Error())
}

func TestResolveErrorHTML(t *testing.T) {
	html := `<html><body><h1>Not found</h1></body></html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(html))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	turl, err := url.Parse(ts.URL)
	require.NoError(t, err)
	res, err := client.makeAPICall(context.Background(), httpParams{ //nolint:bodyclose
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     nil,
		body:        nil,
	})
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Equal(t, html, err.Error())
}

func TestResolveErrorRetryAfter(t *testing.T) {
	html := `<html><body><h1>Too many requests</h1></body></html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/html")
		w.Header().Add("Retry-After", "256")
		w.WriteHeader(492)
		_, _ = w.Write([]byte(html))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	turl, err := url.Parse(ts.URL)
	require.NoError(t, err)
	res, err := client.makeAPICall(context.Background(), httpParams{ //nolint:bodyclose
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     nil,
		body:        nil,
	})
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Equal(t, html, err.Error())
}

func TestResolveErrorWrongJsonResponse(t *testing.T) {
	errMsg := "compilation failed: error at @1:170-1:171: invalid expression @1:167-1:168: |"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		// Missing closing }
		_, _ = w.Write([]byte(`{"error": "` + errMsg + `"`))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	turl, err := url.Parse(ts.URL)
	require.NoError(t, err)
	res, err := client.makeAPICall(context.Background(), httpParams{ //nolint:bodyclose
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     nil,
		body:        nil,
	})
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Equal(t, "cannot decode error response: unexpected end of JSON input", err.Error())
}

func TestResolveErrorEdge(t *testing.T) {
	errMsg := "compilation failed: error at @1:170-1:171: invalid expression @1:167-1:168: |"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "` + errMsg + `"}`))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	turl, err := url.Parse(ts.URL)
	require.NoError(t, err)
	res, err := client.makeAPICall(context.Background(), httpParams{ //nolint:bodyclose
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     nil,
		body:        nil,
	})
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Equal(t, errMsg, err.Error())
}

func TestResolveErrorEdgeWithData(t *testing.T) {
	errMsg := "compilation failed"
	dataErrMsg := "compilation failed: error at @1:170-1:171: invalid expression @1:167-1:168: |"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "` + errMsg + `", "data": {"error_message": "` + dataErrMsg + `"}}`))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	turl, err := url.Parse(ts.URL)
	require.NoError(t, err)
	res, err := client.makeAPICall(context.Background(), httpParams{ //nolint:bodyclose
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     nil,
		body:        nil,
	})
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Equal(t, dataErrMsg, err.Error())
}

func TestResolveErrorNoError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	turl, err := url.Parse(ts.URL)
	require.NoError(t, err)
	res, err := client.makeAPICall(context.Background(), httpParams{ //nolint:bodyclose
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     nil,
		body:        nil,
	})
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Equal(t, `500 Internal Server Error`, err.Error())
}

func TestNewServerError(t *testing.T) {
	message := "message"
	err := NewServerError(message)
	assert.Equal(t, err.Message, message)
}

func TestFixUrl(t *testing.T) {
	boolRef := func(val bool) *bool {
		b := new(bool)
		*b = val
		return b
	}

	testCases := []*struct {
		input        string
		expected     string
		expectedSafe *bool
	}{
		{
			input:        "https://192.168.0.1:85",
			expected:     "192.168.0.1:85",
			expectedSafe: boolRef(true),
		},
		{
			input:        "http://192.168.0.1:85",
			expected:     "192.168.0.1:85",
			expectedSafe: boolRef(false),
		},
		{
			input:        "192.168.0.1:443",
			expected:     "192.168.0.1:443",
			expectedSafe: nil,
		},
		{
			input:        "192.168.0.1:80",
			expected:     "192.168.0.1:80",
			expectedSafe: nil,
		},
		{
			input:        "https://192.168.0.1",
			expected:     "192.168.0.1:443",
			expectedSafe: boolRef(true),
		},
		{
			input:        "http://192.168.0.1",
			expected:     "192.168.0.1:80",
			expectedSafe: boolRef(false),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input,
			func(t *testing.T) {
				u, safe := ReplaceURLProtocolWithPort(tc.input)
				assert.Equal(t, tc.expected, u)
				if safe == nil || tc.expectedSafe == nil {
					assert.Equal(t, tc.expectedSafe, safe)
				} else {
					assert.Equal(t, *tc.expectedSafe, *safe)
				}
			})
	}
}

func TestGetVersionInHeaderSuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Influxdb-Version", "3.0.0")
		w.Write([]byte(
			`{"version": "2.0"}`,
		))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	version, err := client.GetServerVersion()
	require.NoError(t, err)
	assert.Equal(t, "3.0.0", version)
}

func TestGetVersionInBodySuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(
			`{"version": "2.0"}`,
		))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	version, err := client.GetServerVersion()
	require.NoError(t, err)
	assert.Equal(t, "2.0", version)
}

func TestGetVersionInvalid(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Something", "3.0.0")
		w.Write([]byte(
			`{"something": "2.0"}`,
		))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	version, err := client.GetServerVersion()
	require.NoError(t, err)
	assert.Empty(t, version)
}

func TestGetVersionFail(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Influxdb-Version", "3.0.0")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	version, err := client.GetServerVersion()
	assert.Equal(t, "500 Internal Server Error", err.Error())
	assert.Empty(t, version)
}

func TestGetVersionMalformJson(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(
			`{Invalid`,
		))
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL, Token: "my-token"})
	require.NoError(t, err)
	version, err := client.GetServerVersion()
	assert.Contains(t, err.Error(), "invalid character")
	assert.Empty(t, version)
}
