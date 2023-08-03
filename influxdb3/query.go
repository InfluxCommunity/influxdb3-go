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
	"fmt"

	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
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

// Query data from InfluxDB IOx using InfluxQL.
// Parameters:
//   - ctx: The context.Context to use for the request.
//   - database: The database to be used for InfluxDB operations.
//   - query: The InfluxQL query string to execute.
//   - queryParams: Additional query parameters.
//
// Returns:
//   - A custom iterator (*QueryIterator).
//   - An error, if any.
func (c *Client) QueryInfluxQL(ctx context.Context, database string, query string, queryParams ...string) (*QueryIterator, error) {
	return c.queryWithType(ctx, database, query, "influxql", queryParams...)
}

// Query data from InfluxDB IOx using FlightSQL.
// Parameters:
//   - ctx: The context.Context to use for the request.
//   - database: The database to be used for InfluxDB operations.
//   - query: The SQL query string to execute.
//   - queryParams: Additional query parameters.
//
// Returns:
//   - A custom iterator (*QueryIterator) that can also be used to get raw flightsql reader.
//   - An error, if any.
func (c *Client) Query(ctx context.Context, database string, query string, queryParams ...string) (*QueryIterator, error) {
	return c.queryWithType(ctx, database, query, "sql", queryParams...)
}

func (c *Client) queryWithType(ctx context.Context, database string, query string, queryType string, queryParams ...string) (*QueryIterator, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.config.Token)
	ctx = metadata.AppendToOutgoingContext(ctx, queryParams...)

	ticketData := map[string]interface{}{
		"database":   database,
		"sql_query":  query,
		"query_type": queryType,
	}

	ticketJson, err := json.Marshal(ticketData)
	if err != nil {
		return nil, fmt.Errorf("serialize: %s", err)
	}

	ticket := &flight.Ticket{Ticket: ticketJson}
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
