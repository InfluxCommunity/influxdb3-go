package influxdb3

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestCancelingRecordReader(t *testing.T) {
	s := flight.NewServerWithMiddleware(nil)
	err := s.Init("localhost:0")
	require.NoError(t, err)
	f := &flightServer{}
	s.RegisterFlightService(f)
	go func() {
		err := s.Serve()
		if err != nil {
			require.NoError(t, err)
		}
	}()
	defer s.Shutdown()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	client, err := flight.NewClientWithMiddleware(s.Addr().String(), nil, nil, opts...)
	require.NoError(t, err)
	defer client.Close()
	stream, err := client.DoGet(context.Background(), nil)
	require.NoError(t, err)
	reader, err := flight.NewRecordReader(stream)
	require.NoError(t, err)

	canceled := false
	cancel := func() {
		canceled = true
	}
	i := NewQueryIteratorFromReader(&cancelingRecordReader{reader: reader, cancel: cancel})
	for i.Next() {
		_ = i.Index()
	}
	require.NoError(t, i.Err())
	assert.True(t, canceled)
}
