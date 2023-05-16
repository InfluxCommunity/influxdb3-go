package influx

import (
	"context"
	"crypto/x509"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

func (c *Client) Query(ctx context.Context, bucket string, query string, queryParams interface{}) (*flight.Reader, error) {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("x509: %s", err)
	}
	transport := grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, ""))
	opts := []grpc.DialOption{
		transport,
	}

	url := ReplaceURLProtocolWithPort(c.configs.HostURL)

	client, err := flightsql.NewClient(url, nil, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("flightsql: %s", err)
	}
	defer client.Close()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.configs.AuthToken)
	ctx = metadata.AppendToOutgoingContext(ctx, "bucket-name", bucket)

	info, err := client.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("flightsql flight info: %s", err)
	}
	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, fmt.Errorf("flightsql do get: %s", err)
	}
	return reader, nil
}
