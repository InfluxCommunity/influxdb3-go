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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	_, err := New(ClientConfig{})
	require.Error(t, err)
	assert.Equal(t, "empty server URL", err.Error())

	_, err = New(ClientConfig{Host: "http@localhost:8086"})
	require.Error(t, err)
	assert.Equal(t, "parsing host URL: parse \"http@localhost:8086/\": first path segment in URL cannot contain colon", err.Error())

	c, err := New(ClientConfig{Host: "http://localhost:8086"})
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8086", c.config.Host)
	assert.Equal(t, "http://localhost:8086/api/v2/", c.apiURL.String())
	assert.Equal(t, "", c.authorization)

	_, err = New(ClientConfig{Host: "localhost\n"})
	if assert.Error(t, err) {
		expectedMessage := "parsing host URL:"
		assert.True(t, strings.HasPrefix(err.Error(), expectedMessage), fmt.Sprintf("\nexpected prefix : %s\nactual message  : %s", expectedMessage, err.Error()))
	}

	c, err = New(ClientConfig{Host: "http://localhost:8086", Token: "my-token", Database: "my-database"})
	require.NoError(t, err)
	assert.Equal(t, "Token my-token", c.authorization)
	assert.EqualValues(t, DefaultWriteOptions, *c.config.WriteOptions)
	assert.Equal(t, "my-database", c.config.Database)
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
			c, err := New(ClientConfig{Host: turl.HostURL})
			require.NoError(t, err)
			assert.Equal(t, turl.HostURL, c.config.Host)
			assert.Equal(t, turl.serverAPIURL, c.apiURL.String())
		})
	}
}

func TestMakeAPICall(t *testing.T) {
	html := `<html><body><h1>Response</h1></body></html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/html")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(html))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL})
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
}

func TestResolveErrorMessage(t *testing.T) {
	errMsg := "compilation failed: error at @1:170-1:171: invalid expression @1:167-1:168: |"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"code":"invalid","message":"` + errMsg + `"}`))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL})
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
		w.WriteHeader(404)
		_, _ = w.Write([]byte(html))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL})
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
	client, err := New(ClientConfig{Host: ts.URL})
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
		w.WriteHeader(400)
		// Missing closing }
		_, _ = w.Write([]byte(`{"error": "` + errMsg + `"`))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL})
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

func TestResolveErrorV1(t *testing.T) {
	errMsg := "compilation failed: error at @1:170-1:171: invalid expression @1:167-1:168: |"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error": "` + errMsg + `"}`))
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL})
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

func TestResolveErrorNoError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()
	client, err := New(ClientConfig{Host: ts.URL})
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
				url, safe := ReplaceURLProtocolWithPort(tc.input)
				assert.Equal(t, tc.expected, url)
				if safe == nil || tc.expectedSafe == nil {
					assert.Equal(t, tc.expectedSafe, safe)
				} else {
					assert.Equal(t, *tc.expectedSafe, *safe)
				}
			})
	}
}
