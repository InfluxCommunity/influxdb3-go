// Copyright 2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

// Package influx provides client for InfluxDB server.
package influx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
)

// Configs holds the parameters for creating a new client.
// The only mandatory field is ServerURL. AuthToken is also important
// if authentication was not done outside this client.
type Configs struct {
	// HostURL holds the URL of the InfluxDB server to connect to.
	// This must be non-empty. E.g. http://localhost:8086
	HostURL string

	// AuthToken holds the authorization token for the API.
	// This can be obtained through the GUI web browser interface.
	AuthToken string

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
	// Write Params
	WriteParams WriteParams
}

// Client implements an InfluxDB client.
type Client struct {
	// Configuration configs.
	configs Configs
	// Pre-created Authorization HTTP header value.
	authorization string
	// Cached base server API URL.
	apiURL *url.URL
	// Flight client for executing queries
	queryClient *flightsql.Client
}

// httpParams holds parameters for creating an HTTP request
type httpParams struct {
	// URL of server endpoint
	endpointURL *url.URL
	// Params to be added to URL
	queryParams url.Values
	// HTTP request method, eg. POST
	httpMethod string
	// HTTP request headers
	headers http.Header
	// HTTP POST/PUT body
	body io.Reader
}


// New creates new Client with given Params, where ServerURL and AuthToken are mandatory.
func New(params Configs) (*Client, error) {
	c := &Client{configs: params}
	if params.HostURL == "" {
		return nil, errors.New("empty server URL")
	}
	if c.configs.AuthToken != "" {
		c.authorization = "Token " + c.configs.AuthToken
	}
	if c.configs.HTTPClient == nil {
		c.configs.HTTPClient = http.DefaultClient
	}

	hostAddress := params.HostURL
	if !strings.HasSuffix(hostAddress, "/") {
		// For subsequent path parts concatenation, url has to end with '/'
		hostAddress = params.HostURL + "/"
	}
	
	var err error
	// Prepare host API URL
	c.apiURL, err = url.Parse(hostAddress)
	if err != nil {
		return nil, fmt.Errorf("parsing host URL: %w", err)
	}

	c.apiURL.Path = path.Join(c.apiURL.Path,"api/v2") + "/"

	if params.WriteParams.MaxBatchBytes == 0 {
		c.configs.WriteParams = DefaultWriteParams
	}

	err = c.initializeQueryClient()
	if err != nil {
		return nil, fmt.Errorf("flight client: %w", err)
	}

	return c, nil
}

// makeAPICall issues an HTTP request to InfluxDB host API url according to parameters.
// Additionally, sets Authorization header and User-Agent.
// It returns http.Response or error. Error can be a *hostError if host responded with error.
func (c *Client) makeAPICall(ctx context.Context, params httpParams) (*http.Response, error) {
	// copy URL
	urlObj := *params.endpointURL
	urlObj.RawQuery = params.queryParams.Encode()

	fullURL := urlObj.String()

	req, err := http.NewRequestWithContext(ctx, params.httpMethod, fullURL, params.body)
	if err != nil {
		return nil, fmt.Errorf("error calling %s: %v", fullURL, err)
	}
	for k, v := range params.headers {
		for _, i := range v {
			req.Header.Add(k, i)
		}
	}
	req.Header.Set("User-Agent", userAgent)
	if c.authorization != "" {
		req.Header.Add("Authorization", c.authorization)
	}

	resp, err := c.configs.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error calling %s: %v", fullURL, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, c.resolveHTTPError(resp)
	}

	return resp, nil
}

// resolveHTTPError parses host error response and returns error with human-readable message
func (c *Client) resolveHTTPError(r *http.Response) error {
	// successful status code range
	if r.StatusCode >= 200 && r.StatusCode < 300 {
		return nil
	}

	var httpError struct {
		ServerError
		// Error message of InfluxDB 1 error
		Error string `json:"error"`
	}

	httpError.StatusCode = r.StatusCode
	if v := r.Header.Get("Retry-After"); v != "" {
		r, err := strconv.ParseInt(v, 10, 32)
		if err == nil {
			httpError.RetryAfter = int(r)
		}
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpError.Message = fmt.Sprintf("cannot read error response:: %v", err)
	}
	ctype, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if ctype == "application/json" {
		err := json.Unmarshal(body, &httpError)
		if err != nil {
			httpError.Message = fmt.Sprintf("cannot decode error response: %v", err)
		}
		if httpError.Message == "" && httpError.Code == "" {
			httpError.Message = httpError.Error
		}
	}
	if httpError.Message == "" {
		if len(body) > 0 {
			httpError.Message = string(body)
		} else {
			httpError.Message = r.Status
		}
	}

	return &httpError.ServerError
}

// Close closes all idle connections.
func (c *Client) Close() error {
	c.configs.HTTPClient.CloseIdleConnections()
	// Support closer interface
	c.queryClient.Close()
	return nil
}
