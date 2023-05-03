// Package model provides primitives to interact with the openapi HTTP API.
//
// Modified generated code.
package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
)

// Doer performs HTTP requests.
//
// The standard http.Client implements this interface.
type HTTPRequestDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client which conforms to the OpenAPI3 specification for this service.
type Client struct {
	// The endpoint of the server conforming to this interface, with scheme,
	// https://api.deepmap.com for example. This can contain a path relative
	// to the server, such as https://api.deepmap.com/dev-test, and all the
	// paths in the swagger spec will be appended to the server.
	Server string

	// Server + /api/v2/
	APIEndpoint string

	// Doer for performing requests, typically a *http.Client with any
	// customized settings, such as certificate chains.
	Client HTTPRequestDoer
}

// Creates a new Client, with reasonable defaults
func NewClient(server string, doer HTTPRequestDoer) (*Client, error) {
	// create a client with sane default values
	client := Client{
		Server: server,
		Client: doer,
	}
	// ensure the server URL always has a trailing slash
	if !strings.HasSuffix(client.Server, "/") {
		client.Server += "/"
	}
	// API endpoint
	client.APIEndpoint = client.Server + "api/v2/"

	// create httpClient, if not already present
	if client.Client == nil {
		client.Client = &http.Client{}
	}
	return &client, nil
}

func (e *Error) Error() error {
	return fmt.Errorf("%s: %s", string(e.Code), *e.Message)
}

func unmarshalJSONResponse(bodyBytes []byte, obj interface{}) error {
	if err := json.Unmarshal(bodyBytes, obj); err != nil {
		return err
	}
	return nil
}


func isJSON(rsp *http.Response) bool {
	ctype, _, _ := mime.ParseMediaType(rsp.Header.Get("Content-Type"))
	return ctype == "application/json"
}

func decodeError(body []byte, rsp *http.Response) error {
	if isJSON(rsp) {
		var serverError struct {
			Error
			V1Error *string `json:"error,omitempty"`
		}
		err := json.Unmarshal(body, &serverError)
		if err != nil {
			message := fmt.Sprintf("cannot decode error response: %v", err)
			serverError.Message = &message
		}
		if serverError.V1Error != nil {
			serverError.Message = serverError.V1Error
			serverError.Code = ErrorCodeInvalid
		}
		if serverError.Message == nil && serverError.Code == "" {
			serverError.Message = &rsp.Status
		}
		return serverError.Error.Error()
	} else {
		message := rsp.Status
		if len(body) > 0 {
			message = message + ": " + string(body)
		}
		return errors.New(message)
	}
}

// PostDelete calls the POST on /delete
// Delete data
func (c *Client) PostDelete(ctx context.Context, params *PostDeleteAllParams) error {
	var err error
	var bodyReader io.Reader
	buf, err := json.Marshal(params.Body)
	if err != nil {
		return err
	}
	bodyReader = bytes.NewReader(buf)

	serverURL, err := url.Parse(c.APIEndpoint)
	if err != nil {
		return err
	}

	operationPath := "./delete"

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return err
	}

	queryValues := queryURL.Query()

	if params.Org != nil {
		queryValues.Add("org", *params.Org)
	}

	if params.Bucket != nil {
		queryValues.Add("bucket", *params.Bucket)
	}

	if params.OrgID != nil {
		queryValues.Add("orgID", *params.OrgID)
	}

	if params.BucketID != nil {
		queryValues.Add("bucketID", *params.BucketID)
	}

	queryURL.RawQuery = queryValues.Encode()

	req, err := http.NewRequest("POST", queryURL.String(), bodyReader)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	if params.ZapTraceSpan != nil {
		req.Header.Set("Zap-Trace-Span", *params.ZapTraceSpan)
	}

	req = req.WithContext(ctx)
	rsp, err := c.Client.Do(req)
	if err != nil {
		return err
	}

	defer func() { _ = rsp.Body.Close() }()

	if rsp.StatusCode > 299 {
		bodyBytes, err := io.ReadAll(rsp.Body)
		if err != nil {
			return err
		}
		return decodeError(bodyBytes, rsp)
	}
	return nil

}

// GetReady calls the GET on /ready
// Get the readiness of an instance at startup
func (c *Client) GetReady(ctx context.Context, params *GetReadyParams) (*Ready, error) {
	var err error

	serverURL, err := url.Parse(c.Server)
	if err != nil {
		return nil, err
	}

	operationPath := "./ready"

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", queryURL.String(), nil)
	if err != nil {
		return nil, err
	}

	if params.ZapTraceSpan != nil {
		req.Header.Set("Zap-Trace-Span", *params.ZapTraceSpan)
	}

	req = req.WithContext(ctx)
	rsp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	bodyBytes, err := io.ReadAll(rsp.Body)

	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &Ready{}

	switch rsp.StatusCode {
	case 200:
		if err := unmarshalJSONResponse(bodyBytes, &response); err != nil {
			return nil, err
		}
	default:
		return nil, decodeError(bodyBytes, rsp)
	}
	return response, nil

}


// GetHealth calls the GET on /health
// Retrieve the health of the instance
func (c *Client) GetHealth(ctx context.Context, params *GetHealthParams) (*HealthCheck, error) {
	var err error

	serverURL, err := url.Parse(c.Server)
	if err != nil {
		return nil, err
	}

	operationPath := "./health"

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", queryURL.String(), nil)
	if err != nil {
		return nil, err
	}

	if params.ZapTraceSpan != nil {
		req.Header.Set("Zap-Trace-Span", *params.ZapTraceSpan)
	}

	req = req.WithContext(ctx)
	rsp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	bodyBytes, err := io.ReadAll(rsp.Body)

	defer func() { _ = rsp.Body.Close() }()
	if err != nil {
		return nil, err
	}

	response := &HealthCheck{}

	switch rsp.StatusCode {
	case 200:
		if err := unmarshalJSONResponse(bodyBytes, &response); err != nil {
			return nil, err
		}
	default:
		return nil, decodeError(bodyBytes, rsp)
	}
	return response, nil

}



// GetPing calls the GET on /ping
// Get the status and version of the instance
func (c *Client) GetPing(ctx context.Context) error {
	var err error

	serverURL, err := url.Parse(c.Server)
	if err != nil {
		return err
	}

	operationPath := "./ping"

	queryURL, err := serverURL.Parse(operationPath)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("GET", queryURL.String(), nil)
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	rsp, err := c.Client.Do(req)
	if err != nil {
		return err
	}

	defer func() { _ = rsp.Body.Close() }()

	if rsp.StatusCode > 299 {
		bodyBytes, err := io.ReadAll(rsp.Body)
		if err != nil {
			return err
		}
		return decodeError(bodyBytes, rsp)
	}
	return nil
}