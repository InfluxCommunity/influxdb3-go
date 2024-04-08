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
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func (c *Client) initializeQueryClient() error {
	url, safe := ReplaceURLProtocolWithPort(c.config.Host)

	var transport grpc.DialOption

	if safe == nil || *safe {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return fmt.Errorf("x509: %s", err)
		}
		transport = grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, ""))
	} else {
		transport = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	opts := []grpc.DialOption{
		transport,
	}

	client, err := flight.NewClientWithMiddleware(url, nil, nil, opts...)
	if err != nil {
		return fmt.Errorf("flight: %s", err)
	}
	c.queryClient = &client

	return nil
}

// QueryParameters is a type for query parameters.
type QueryParameters = map[string]any

// Query queries data from InfluxDB IOx.
// Parameters:
//   - ctx: The context.Context to use for the request.
//   - query: The query string to execute.
//   - options: The optional query options. See QueryOption for available options.
//
// Returns:
//   - A custom iterator (*QueryIterator).
//   - An error, if any.
func (c *Client) Query(ctx context.Context, query string, options ...QueryOption) (*QueryIterator, error) {
	return c.query(ctx, query, nil, newQueryOptions(&DefaultQueryOptions, options))
}

// QueryWithParameters queries data from InfluxDB IOx with parameterized query.
// Parameters:
//   - ctx: The context.Context to use for the request.
//   - query: The query string to execute.
//   - parameters: The query parameters.
//   - options: The optional query options. See QueryOption for available options.
//
// Returns:
//   - A custom iterator (*QueryIterator).
//   - An error, if any.
func (c *Client) QueryWithParameters(ctx context.Context, query string, parameters QueryParameters,
	options ...QueryOption) (*QueryIterator, error) {

	return c.query(ctx, query, parameters, newQueryOptions(&DefaultQueryOptions, options))
}

// QueryWithOptions Query data from InfluxDB IOx with query options.
// Parameters:
//   - ctx: The context.Context to use for the request.
//   - options: Query options (query type, optional database).
//   - query: The query string to execute.
//
// Returns:
//   - A custom iterator (*QueryIterator) that can also be used to get raw flightsql reader.
//   - An error, if any.
//
// Deprecated: use Query with variadic QueryOption options.
func (c *Client) QueryWithOptions(ctx context.Context, options *QueryOptions, query string) (*QueryIterator, error) {
	if options == nil {
		return nil, errors.New("options not set")
	}

	return c.query(ctx, query, nil, options)
}

func (c *Client) query(ctx context.Context, query string, parameters QueryParameters, options *QueryOptions) (*QueryIterator, error) {
	var database string
	if options.Database != "" {
		database = options.Database
	} else {
		database = c.config.Database
	}
	if database == "" {
		return nil, errors.New("database not specified")
	}

	var queryType QueryType
	queryType = options.QueryType

	// add config headers
	for k, v := range c.config.Headers {
		if !c.isSystemHeader(k) {
			for _, value := range v {
				ctx = metadata.AppendToOutgoingContext(ctx, k, value)
			}
		}
	}

	// add call options headers
	for k, v := range options.Headers {
		if !c.isSystemHeader(k) {
			for _, value := range v {
				ctx = metadata.AppendToOutgoingContext(ctx, k, value)
			}
		}
	}

	// add system headers
	ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", "Bearer "+c.config.Token)

	ticketData := map[string]interface{}{
		"database":   database,
		"sql_query":  query,
		"query_type": strings.ToLower(queryType.String()),
	}

	if len(parameters) > 0 {
		ticketData["params"] = parameters
	}

	ticketJSON, err := json.Marshal(ticketData)
	if err != nil {
		return nil, fmt.Errorf("serialize: %s", err)
	}

	ticket := &flight.Ticket{Ticket: ticketJSON}
	stream, err := (*c.queryClient).DoGet(ctx, ticket)
	if err != nil {
		return nil, fmt.Errorf("flight do get: %s", err)
	}

	reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		return nil, fmt.Errorf("flight reader: %s", err)
	}

	iterator := newQueryIterator(reader)
	return iterator, nil
}
