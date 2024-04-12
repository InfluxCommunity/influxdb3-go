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
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
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
	require.NoError(t, err, "DoGet success")
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
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "intField", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "stringField", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "floatField", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c", "d", "e"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{1, 0, 3, 0, 5}, []bool{true, false, true, false, true})
	rec0 := builder.NewRecord()
	defer rec0.Release()
	recs := []arrow.Record{rec0}

	w := flight.NewRecordWriter(fs, ipc.WithSchema(recs[0].Schema()))
	for _, r := range recs {
		w.Write(r)
	}

	return nil
}

type callHeadersMiddleware struct {
	outgoingMDOk bool
	outgoingMD   metadata.MD
}

func (c *callHeadersMiddleware) StartCall(ctx context.Context) context.Context {
	c.outgoingMD, c.outgoingMDOk = metadata.FromOutgoingContext(ctx)
	return ctx
}

func (c *callHeadersMiddleware) CallCompleted(ctx context.Context, err error) {
}

func (c *callHeadersMiddleware) HeadersReceived(ctx context.Context, md metadata.MD) {
}
