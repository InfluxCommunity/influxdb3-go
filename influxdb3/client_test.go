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
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

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
		assert.True(t, strings.HasPrefix(err.Error(), expectedMessage), fmt.Sprintf("\nexpected prefix : %s\nactual message  : %s", expectedMessage, err.Error()))
	}

	c, err = New(ClientConfig{Host: "http@localhost:8086", Token: "my-token"})
	require.Nil(t, c)
	assert.Error(t, err)
	assert.Equal(t, "parsing host URL: parse \"http@localhost:8086/\": first path segment in URL cannot contain colon", err.Error())

	c, err = New(ClientConfig{Host: "http://localhost:8086", Token: "my-token"})
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, "http://localhost:8086", c.config.Host)
	assert.Equal(t, "http://localhost:8086/api/v2/", c.apiURL.String())
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

func TestURLs(t *testing.T) {
	urls := []struct {
		HostURL      string
		serverAPIURL string
	}{
		{"http://host:8086", "http://host:8086/api/v2/"},
		{"http://host:8086/", "http://host:8086/api/v2/"},
		{"http://host:8086/path", "http://host:8086/path/api/v2/"},
		{"http://host:8086/path/", "http://host:8086/path/api/v2/"},
		{"http://host:8086/path1/path2/path3", "http://host:8086/path1/path2/path3/api/v2/"},
		{"http://host:8086/path1/path2/path3/", "http://host:8086/path1/path2/path3/api/v2/"},
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
			cs:   "https://host:8086?token=abc&org=my-org&database=my-db&precision=ms",
			cfg: &ClientConfig{
				Host:         "https://host:8086",
				Token:        "abc",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &WriteOptions{
					Precision:     lineprotocol.Millisecond,
					GzipThreshold: 1000, // default
				},
			},
		},
		{
			name: "invalid gzip threshold",
			cs:   "https://host:8086?token=abc&gzipThreshold=a0",
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
			},
			cfg: &ClientConfig{
				Host:         "http://host:8086",
				Token:        "abc",
				Organization: "my-org",
				Database:     "my-db",
				WriteOptions: &WriteOptions{
					Precision:     lineprotocol.Millisecond,
					GzipThreshold: 64,
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
	}
	clearEnv := func() {
		os.Unsetenv(envInfluxHost)
		os.Unsetenv(envInfluxToken)
		os.Unsetenv(envInfluxAuthScheme)
		os.Unsetenv(envInfluxOrg)
		os.Unsetenv(envInfluxDatabase)
		os.Unsetenv(envInfluxPrecision)
		os.Unsetenv(envInfluxGzipThreshold)
	}
	setEnv := func(vars map[string]string) {
		for k, v := range vars {
			os.Setenv(k, v)
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
	assert.Nil(t, err)
	assert.Equal(t, "Token my-token", res.Request.Header.Get("Authorization"))
	assert.Nil(t, err)

	res, err = client.makeAPICall(context.Background(), httpParams{
		endpointURL: turl,
		queryParams: nil,
		httpMethod:  "GET",
		headers:     http.Header{"Authorization": {"Bearer management-api-token"}},
		body:        nil,
	})
	assert.Equal(t, "Bearer management-api-token", res.Request.Header.Get("Authorization"))
	assert.Nil(t, err)

	client, err = New(ClientConfig{Host: ts.URL, Token: "my-token", AuthScheme: "Bearer"})
	require.NoError(t, err)
	res, err = client.makeAPICall(context.Background(), httpParams{
		endpointURL: turl,
		httpMethod:  "GET",
	})
	assert.Equal(t, "Bearer my-token", res.Request.Header.Get("Authorization"))
	assert.Nil(t, err)
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
	res, err := client.makeAPICall(context.Background(), httpParams{
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
	res, err := client.makeAPICall(context.Background(), httpParams{
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
	res, err := client.makeAPICall(context.Background(), httpParams{
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
	res, err := client.makeAPICall(context.Background(), httpParams{
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
	res, err := client.makeAPICall(context.Background(), httpParams{
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
	res, err := client.makeAPICall(context.Background(), httpParams{
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
	res, err := client.makeAPICall(context.Background(), httpParams{
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
		t.Run(fmt.Sprintf("fix url: %s", tc.input),
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
