package influx

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
	url, safe := ReplaceURLProtocolWithPort(c.configs.HostURL)

	var transport grpc.DialOption

	if (safe == nil || *safe) {
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
//   - query: The InfluxQL query string to execute.
//   - database: The first optional parameter of queryParams to be used for InfluxDB operations,
//               if not present or empty, the database from Configs is used.
//   - queryParams: Additional query parameters.
// Returns:
//   - A custom iterator (*QueryIterator).
//   - An error, if any.
func (c *Client) QueryInfluxQL(ctx context.Context, query string, queryParams ...string) (*QueryIterator, error) {
	return c.queryWithType(ctx, query, "influxql", queryParams...)
}

// Query data from InfluxDB IOx using FlightSQL.
// Parameters:
//   - ctx: The context.Context to use for the request.
//   - query: The SQL query string to execute.
//   - database: The first optional parameter of queryParams to be used for InfluxDB operations,
//               if not present or empty, the database from Configs is used.
//   - queryParams: Additional query parameters.
// Returns:
//   - A custom iterator (*QueryIterator) that can also be used to get raw flightsql reader.
//   - An error, if any.
func (c *Client) Query(ctx context.Context, query string, queryParams ...string) (*QueryIterator, error) {
	return c.queryWithType(ctx, query, "sql", queryParams...)
}

func (c *Client) queryWithType(ctx context.Context, query string, queryType string, queryParams ...string) (*QueryIterator, error) {
	var database string

	hasParams := len(queryParams) > 0;
	if (hasParams && queryParams[0] != ""){
			database = queryParams[0]
	} else {
			database = c.configs.Database
	}
	if database == "" {
		return nil, fmt.Errorf("config: No database specified in arguments or in the configuration")
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.configs.AuthToken)
	if hasParams {
		ctx = metadata.AppendToOutgoingContext(ctx, queryParams[1:]...)
	}

	ticketData := map[string]interface{}{
		"database": database,
		"sql_query": query,
		"query_type": queryType,
	}

	ticketJson, err := json.Marshal(ticketData);
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
