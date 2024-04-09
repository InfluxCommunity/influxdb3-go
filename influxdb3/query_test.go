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
	"net/http"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestQueryDatabaseNotSet(t *testing.T) {
	c, err := New(ClientConfig{
		Host:  "http://localhost:8086",
		Token: "my-token",
	})
	require.NoError(t, err)
	iterator, err := c.Query(context.Background(), "SHOW NAMESPACES")
	assert.Nil(t, iterator)
	assert.Error(t, err)
	assert.EqualError(t, err, "database not specified")
}

func TestQueryWithOptionsNotSet(t *testing.T) {
	c, err := New(ClientConfig{
		Host:     "http://localhost:8086",
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	iterator, err := c.QueryWithOptions(context.Background(), nil, "SHOW NAMESPACES")
	assert.Nil(t, iterator)
	assert.Error(t, err)
	assert.EqualError(t, err, "options not set")
}

func TestQueryWithCustomHeader(t *testing.T) {
	s := flight.NewServerWithMiddleware(nil)
	err := s.Init("localhost:18080")
	require.NoError(t, err)
	f := &flightServer{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	middleware := &callHeadersMiddleware{}
	fc, err := flight.NewClientWithMiddleware(s.Addr().String(), nil, []flight.ClientMiddleware{
		flight.CreateClientMiddleware(middleware),
	}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer fc.Close()

	c, err := New(ClientConfig{
		Host: "http://localhost:80",
		Token: "my-token",
		Database: "my-database",
		Headers: http.Header{
			"my-config-header": {"hdr-config-1"},
		},
	})
	require.NoError(t, err)
	defer c.Close()

	c.setQueryClient(fc)

	_, err = c.Query(context.Background(), "SELECT * FROM nothing", WithHeader("my-call-header", "hdr-call-1"))
	_ = err // ignore it is EOF
	assert.True(t, middleware.outgoingMDOk, "context contains outgoing MD")
	assert.NotNil(t, middleware.outgoingMD, "outgoing MD is not nil")
	assert.Contains(t, middleware.outgoingMD, "authorization", "auth header present")
	assert.Contains(t, middleware.outgoingMD, "my-config-header", "custom config header present")
	assert.Equal(t, []string{"hdr-config-1"}, middleware.outgoingMD["my-config-header"], "custom config header value")
	assert.Contains(t, middleware.outgoingMD, "my-call-header", "custom call header present")
	assert.Equal(t, []string{"hdr-call-1"}, middleware.outgoingMD["my-call-header"],"custom call header value")
}

// fake Flight server implementation

type flightServer struct {
	flight.BaseFlightServer
}

func (f *flightServer) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	records := [0]arrow.Record{} // empty array will cause EOF but we do not need the result anyway, for now

	w := flight.NewRecordWriter(fs)
	for _, r := range records {
		w.Write(r)
	}

	return nil
}

type callHeadersMiddleware struct {
	outgoingMDOk bool
	outgoingMD   metadata.MD
}

func (c *callHeadersMiddleware) StartCall(ctx context.Context) context.Context {
	return ctx
}

func (c *callHeadersMiddleware) CallCompleted(ctx context.Context, err error) {
}

func (c *callHeadersMiddleware) HeadersReceived(ctx context.Context, md metadata.MD) {
	c.outgoingMD, c.outgoingMDOk = metadata.FromOutgoingContext(ctx)
}
