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

	"github.com/apache/arrow/go/v12/arrow/flight"
)

// Client implements an InfluxDB client.
type Client struct {
	// Configuration options.
	config ClientConfig
	// Pre-created Authorization HTTP header value.
	authorization string
	// Cached base server API URL.
	apiURL *url.URL
	// Flight client for executing queries
	queryClient *flight.Client
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
func New(config ClientConfig) (*Client, error) {
	c := &Client{config: config}
	if config.Host == "" {
		return nil, errors.New("empty server URL")
	}
	if c.config.Token != "" {
		c.authorization = "Token " + c.config.Token
	}
	if c.config.HTTPClient == nil {
		c.config.HTTPClient = http.DefaultClient
	}

	hostAddress := config.Host
	if !strings.HasSuffix(hostAddress, "/") {
		// For subsequent path parts concatenation, url has to end with '/'
		hostAddress = config.Host + "/"
	}

	var err error
	// Prepare host API URL
	c.apiURL, err = url.Parse(hostAddress)
	if err != nil {
		return nil, fmt.Errorf("parsing host URL: %w", err)
	}

	c.apiURL.Path = path.Join(c.apiURL.Path, "api/v2") + "/"

	// Default params if nothing set
	if config.WriteOptions.GzipThreshold == 0 &&
		config.WriteOptions.Precision == 0 {
		c.config.WriteOptions = DefaultWriteOptions
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
	for k, v := range c.config.Headers {
		for _, i := range v {
			req.Header.Add(k, i)
		}
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

	resp, err := c.config.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error calling %s: %v", fullURL, err)
	}
	err = c.resolveHTTPError(resp)
	if err != nil {
		return nil, err
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
	c.config.HTTPClient.CloseIdleConnections()
	err := (*c.queryClient).Close()
	return err
}
