package influx

import (
	"context"
	"crypto/x509"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
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

	client, err := flightsql.NewClient(url, nil, nil, opts...)
	if err != nil {
		return fmt.Errorf("flightsql: %s", err)
	}
	c.queryClient = client
	return nil
}

func (c *Client) Query(ctx context.Context, database string, query string, queryParams interface{}) (*QueryIterator, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.configs.AuthToken)
	ctx = metadata.AppendToOutgoingContext(ctx, "database", database)
	ctx = metadata.AppendToOutgoingContext(ctx, "bucket-name", database)

	info, err := c.queryClient.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("flightsql flight info: %s", err)
	}
	reader, err := c.queryClient.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, fmt.Errorf("flightsql do get: %s", err)
	}
	iterator := newQueryIterator(reader)
	return iterator, nil
}
